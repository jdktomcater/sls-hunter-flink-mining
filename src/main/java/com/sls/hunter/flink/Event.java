package com.sls.hunter.flink;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** 一条输入。业务日固定按 +08:00 切分，kind 默认为 EVENT。 */
public class Event implements Serializable {
  public static final ZoneId ZONE = ZoneOffset.ofHours(8);
  public static final Set<String> FAMILIES = J8.set("tracing_sas_linux", "tracing_sas_win", "tracing_sas_jumper", "tracing_nginx_hertzbeat");

  public String family;
  public String id;
  public String day;
  public String kind = "EVENT";
  public long time;
  public Map<String, Object> fields = new LinkedHashMap<>();

  public Event() {}

  /** 信封输入。回放文件和已带 index_family 的消息走这里。 */
  public static Event parse(String raw) {
    return parse(raw, "", -1, -1L, Collections.<String, String>emptyMap());
  }

  /**
   * 原始 Kafka 消息没有 index_family、event_id、payload。族由配置或 topic、_index 识别，身份用文档 id，
   * 再没有时用 topic、分区和偏移。已写明的 index_family 仍然优先，非法值直接拒绝。
   */
  public static Event parse(
      String raw, String topic, int partition, long offset, Map<String, String> topicFamilies) {
    Map<String, Object> root = Json.map(raw);
    Event event = new Event();
    event.family = resolveFamily(root, topic, topicFamilies);
    event.kind = root.getOrDefault("kind", "EVENT").toString();
    if (event.kind.equals("SEAL") || event.kind.equals("FLEET")) {
      return parseControl(event, root);
    }
    if (!event.kind.equals("EVENT")) {
      throw new IllegalArgumentException("Only append-only EVENT, FLEET or coordinated SEAL supported");
    }
    event.fields = fields(root);
    fillCmdline(event.fields);
    event.id = identity(root, topic, partition, offset);
    event.time = epochMillis(timestamp(root, event.fields));
    event.day = Instant.ofEpochMilli(event.time).atZone(ZONE).toLocalDate().toString();
    return event;
  }

  /** 配置里的 topic 精确对应优先，其次按 topic 或 _index 的名字识别四族。 */
  private static String resolveFamily(
      Map<String, Object> root, String topic, Map<String, String> topicFamilies) {
    if (root.containsKey("index_family")) {
      String family = Json.str(root, "index_family");
      if (!FAMILIES.contains(family)) {
        throw new IllegalArgumentException("Unknown index_family:" + family);
      }
      return family;
    }
    String configured = topicFamilies == null ? null : topicFamilies.get(topic);
    if (configured != null) {
      if (!FAMILIES.contains(configured)) {
        throw new IllegalArgumentException("Unknown index_family:" + configured);
      }
      return configured;
    }
    String inferred = familyFromName(topic);
    if (!FAMILIES.contains(inferred)) {
      inferred = familyFromName(Json.str(root, "_index"));
    }
    if (!FAMILIES.contains(inferred)) {
      throw new IllegalArgumentException("Unknown index_family for topic " + topic);
    }
    return inferred;
  }

  private static String familyFromName(String name) {
    String text = name == null ? "" : name.toLowerCase(Locale.ROOT);
    if (named(text, "sas-linux") || named(text, "sas_linux")) {
      return "tracing_sas_linux";
    }
    if (named(text, "sas-win") || named(text, "sas_win")) {
      return "tracing_sas_win";
    }
    if (named(text, "jumper")) {
      return "tracing_sas_jumper";
    }
    if (named(text, "nginx") || named(text, "hertzbeat")) {
      return "tracing_nginx_hertzbeat";
    }
    return "";
  }

  /** token 两侧必须是名字边界，避免 sas-win 命中 sas-windows。 */
  private static boolean named(String text, String token) {
    int from = 0;
    while (from <= text.length() - token.length()) {
      int at = text.indexOf(token, from);
      if (at < 0) {
        return false;
      }
      boolean left = at == 0 || !Character.isLetterOrDigit(text.charAt(at - 1));
      int end = at + token.length();
      boolean right = end == text.length() || !Character.isLetterOrDigit(text.charAt(end));
      if (left && right) {
        return true;
      }
      from = at + 1;
    }
    return false;
  }

  /** 有 payload 用信封；有 _source 用 ES 文档；否则整条原始 JSON 就是字段。 */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> fields(Map<String, Object> root) {
    Object payload = root.get("payload");
    if (payload instanceof Map) {
      return new LinkedHashMap<String, Object>((Map<String, Object>) payload);
    }
    Object source = root.get("_source");
    if (source instanceof Map) {
      return new LinkedHashMap<String, Object>((Map<String, Object>) source);
    }
    return new LinkedHashMap<String, Object>(root);
  }

  /** 原始连接日志把当前进程命令放在 cmd_chain 里，没有单独的 cmdline。 */
  @SuppressWarnings("unchecked")
  private static void fillCmdline(Map<String, Object> fields) {
    if (!Json.str(fields, "cmdline").trim().isEmpty()) {
      return;
    }
    Object chain = fields.get("cmd_chain");
    if (!(chain instanceof String) || ((String) chain).trim().isEmpty()) {
      return;
    }
    List<?> items;
    try {
      items = Json.MAPPER.readValue((String) chain, List.class);
    } catch (Exception ignored) {
      return;
    }
    String pid = Json.str(fields, "pid");
    String first = "";
    for (Object item : items) {
      if (!(item instanceof Map)) {
        continue;
      }
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
        String command = entry.getValue() == null ? "" : entry.getValue().toString();
        if (first.isEmpty()) {
          first = command;
        }
        if (!pid.isEmpty() && pid.equals(String.valueOf(entry.getKey()))) {
          fields.put("cmdline", command);
          return;
        }
      }
    }
    if (!first.isEmpty()) {
      fields.put("cmdline", first);
    }
  }

  /** SEAL 声明这一天已经核验完整。FLEET 的 values 是进程到主机数，必须是非负整数。 */
  @SuppressWarnings("unchecked")
  private static Event parseControl(Event event, Map<String, Object> root) {
    event.day = LocalDate.parse(Json.str(root, "day")).toString();
    event.id = event.kind + ":" + event.day;
    if (!event.kind.equals("FLEET")) {
      return event;
    }
    if (!(root.get("values") instanceof Map<?, ?>)) {
      throw new IllegalArgumentException("FLEET.values must be an object");
    }
    event.fields = new LinkedHashMap<>((Map<String, Object>) root.get("values"));
    for (Object value : event.fields.values()) {
      if (!(value instanceof Number) || !wholeFleetCount((Number) value)) {
        throw new IllegalArgumentException("Invalid fleet count");
      }
    }
    return event;
  }

  private static boolean wholeFleetCount(Number number) {
    double value = number.doubleValue();
    return value >= 0 && value <= Integer.MAX_VALUE && value == Math.rint(value);
  }

  /**
   * event_id 优先，其次源文档身份、_id 或 id。原始连接日志的 uuid 会在多条记录间重复，不能当身份。都没有时用
   * 这条 Kafka 记录的位置。
   */
  private static String identity(
      Map<String, Object> root, String topic, int partition, long offset) {
    String eventId = Json.str(root, "event_id");
    if (!eventId.trim().isEmpty()) {
      return eventId;
    }
    String index = Json.str(root, "source_index");
    String sourceId = Json.str(root, "source_id");
    if (!index.trim().isEmpty() && !sourceId.trim().isEmpty()) {
      return Json.write(J8.list(index, sourceId));
    }
    String docId = Json.str(root, "_id");
    if (docId.trim().isEmpty()) {
      docId = Json.str(root, "id");
    }
    if (!docId.trim().isEmpty()) {
      return docId;
    }
    if (topic != null && !topic.isEmpty() && offset >= 0) {
      return topic + ":" + partition + ":" + offset;
    }
    throw new IllegalArgumentException("event_id or source_index/source_id required");
  }

  private static Object timestamp(Map<String, Object> root, Map<String, Object> fields) {
    Object value = fields.get("@timestamp");
    if (value == null) {
      value = root.get("event_time");
    }
    if (value != null) {
      return value;
    }
    Object start = fields.containsKey("start_time") ? fields.get("start_time") : root.get("start_time");
    if (start != null) {
      return start;
    }
    Object procStart =
        fields.containsKey("proc_start_time") ? fields.get("proc_start_time") : root.get("proc_start_time");
    if (procStart != null) {
      return procStart;
    }
    throw new IllegalArgumentException("Missing @timestamp/event_time/start_time");
  }

  /** 10 位及以下的数字按秒，13 位按毫秒。其余依次接受 UTC Instant、带偏移的时间和无时区本地时间。 */
  private static long epochMillis(Object timestamp) {
    if (timestamp instanceof Number) {
      return epochNumber(((Number) timestamp).longValue());
    }
    String digits = timestamp.toString().trim();
    if (digits.matches("\\d{1,13}")) {
      return epochNumber(Long.parseLong(digits));
    }
    String text = timestamp.toString();
    try {
      return Instant.parse(text).toEpochMilli();
    } catch (DateTimeParseException ignored) {
      try {
        return OffsetDateTime.parse(text).toInstant().toEpochMilli();
      } catch (DateTimeParseException ignoredAgain) {
        return LocalDateTime.parse(text.replace(' ', 'T')).atZone(ZONE).toInstant().toEpochMilli();
      }
    }
  }

  /** 小于 1e11 的值还在秒的范围内，当前毫秒时间戳已经超过这个界限。 */
  private static long epochNumber(long value) {
    if (Math.abs(value) < 100_000_000_000L) {
      return value * 1000L;
    }
    return value;
  }

  public String s(String key) {
    return Json.str(fields, key);
  }

  /** 主机、用户、进程三元组。字段顺序参与候选键，不能调整。 */
  public String triple() {
    return Json.write(J8.list(s("host_ip"), s("username"), s("proc_name")));
  }

  /** 事件时间优先，相同时间用 id 固定次序。 */
  public static final Comparator<Event> ORDER =
      Comparator.comparingLong((Event event) -> event.time).thenComparing(event -> event.id);
}
