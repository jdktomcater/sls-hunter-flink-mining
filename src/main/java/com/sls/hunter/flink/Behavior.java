package com.sls.hunter.flink;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Linux 行为规则。查询是规则 JSON 里的 Elasticsearch 子集：bool、term、terms、wildcard。 */
public final class Behavior {
  private static final String COMMAND_BOUNDARY = "(?:^|[\\s;|&])(?:/[^\\s;|&]*/)?";
  private static final String CRON_PATH = "(?:/etc/crontab|/etc/cron(?:\\.[^/\\s;|&>]+|/[^\\s;|&>]*)|/var/spool/cron(?:/[^\\s;|&>]*)?)";
  private static final Pattern CRONTAB_EDIT = Pattern.compile(COMMAND_BOUNDARY
      + "crontab\\s+(?:(?:-u\\s+\\S+|-[A-Za-z]*[A-DF-Za-df-z])\\s+)*-[A-Za-z]*e[A-Za-z]*(?:\\s|$)");
  private static final Pattern CRONTAB_INSTALL = Pattern.compile(COMMAND_BOUNDARY
      + "crontab\\s+(?:-u\\s+\\S+\\s+)?(?:/|\\./|\\.\\./|[A-Za-z0-9_])[^\\s;|&]*");
  private static final Pattern CRON_WRITE = Pattern.compile("(?s)(?:>{1,2}\\s*" + CRON_PATH
      + "|" + COMMAND_BOUNDARY + "(?:cp|mv|install|tee)\\s+[^;|&]*" + CRON_PATH + ")");
  private static final Pattern CREDENTIAL_PATH = Pattern.compile(
      "(?:/\\.ssh/(?:id_rsa|id_ed25519)|/etc/shadow)(?![A-Za-z0-9_./-])");
  private static final Pattern CREDENTIAL_READER = Pattern.compile(COMMAND_BOUNDARY
      + "(?:cat|head|tail|less|more|grep|sed|awk|cp|tar|base64|dd)\\s+");
  private static final Pattern NETCAT_LISTEN = Pattern.compile(COMMAND_BOUNDARY
      + "(?:nc|ncat)\\s+(?:[^;|&]*?\\s)?-[A-Za-z]*l[A-Za-z]*(?:\\s|$)");
  private static final Pattern OTHER_LISTEN = Pattern.compile(COMMAND_BOUNDARY
      + "(?:socat\\s+[^;|&]*TCP-LISTEN|python(?:[0-9.]*)\\s+[^;|&]*(?:http\\.server|SimpleHTTPServer|-m\\s+http))");
  private Behavior() {}

  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> patterns() {
    return (List<Map<String, Object>>) RuleData.data("tracing_sas_linux").get("BEHAVIOR_PATTERNS");
  }

  static boolean included(Map<String, Object> pattern, Event event) {
    String command = event.s("cmdline");
    String label = String.valueOf(pattern.get("label"));
    if (label.equals("cron_persistence")) {
      return CRONTAB_EDIT.matcher(command).find() || CRONTAB_INSTALL.matcher(command).find()
          || CRON_WRITE.matcher(command).find();
    }
    if (label.equals("credential_harvest")) {
      return CREDENTIAL_PATH.matcher(command).find() && CREDENTIAL_READER.matcher(command).find();
    }
    if (label.equals("port_listen_bind")) {
      return NETCAT_LISTEN.matcher(command).find() || OTHER_LISTEN.matcher(command).find();
    }
    return true;
  }

  /** 一条规则是否命中事件。不支持的查询类型直接失败，避免静默当成未命中。 */
  @SuppressWarnings("unchecked")
  public static boolean matches(Object query, Event event) {
    Map<String, Object> clause = (Map<String, Object>) query;
    if (clause.containsKey("bool")) {
      return matchesBool((Map<String, Object>) clause.get("bool"), event);
    }
    for (String type : J8.list("term", "terms", "wildcard")) {
      if (clause.containsKey(type)) {
        return matchesField(type, (Map<String, Object>) clause.get(type), event);
      }
    }
    throw new IllegalArgumentException("Unsupported query " + clause.keySet());
  }

  /** must 与 filter 全部成立，must_not 全部不成立，should 达到 minimum_should_match。 */
  @SuppressWarnings("unchecked")
  private static boolean matchesBool(Map<String, Object> boolQuery, Event event) {
    for (String key : J8.list("must", "filter")) {
      for (Object sub : (List<Object>) boolQuery.getOrDefault(key, Collections.emptyList())) {
        if (!matches(sub, event)) {
          return false;
        }
      }
    }
    for (Object sub : (List<Object>) boolQuery.getOrDefault("must_not", Collections.emptyList())) {
      if (matches(sub, event)) {
        return false;
      }
    }
    List<Object> should = (List<Object>) boolQuery.getOrDefault("should", Collections.emptyList());
    long matched = should.stream().filter(sub -> matches(sub, event)).count();
    return matched >= minimumShouldMatch(boolQuery, should);
  }

  /**
   * 未写 minimum_should_match 时：已经有 must/filter 则 should 不是必须；否则至少命中 1 条，没有 should 时为 0。
   */
  private static int minimumShouldMatch(Map<String, Object> boolQuery, List<Object> should) {
    if (boolQuery.containsKey("minimum_should_match")) {
      return ((Number) boolQuery.get("minimum_should_match")).intValue();
    }
    if (boolQuery.containsKey("must") || boolQuery.containsKey("filter")) {
      return 0;
    }
    return should.isEmpty() ? 0 : 1;
  }

  @SuppressWarnings("unchecked")
  private static boolean matchesField(String type, Map<String, Object> clause, Event event) {
    Map.Entry<String, Object> field = clause.entrySet().iterator().next();
    String actual = event.s(field.getKey().replace(".keyword", ""));
    if ("term".equals(type)) {
      return actual.equals(field.getValue().toString());
    }
    if ("terms".equals(type)) {
      return ((List<?>) field.getValue()).contains(actual);
    }
    if ("wildcard".equals(type)) {
      return actual.matches(wildcardRegex(field.getValue().toString()));
    }
    throw new IllegalArgumentException("Unsupported query " + type);
  }

  /** `*` 与 `?` 按通配符翻译，其余字符按字面量。`(?s)` 让 `.` 跨行。 */
  private static String wildcardRegex(String pattern) {
    StringBuilder regex = new StringBuilder("(?s)");
    for (char character : pattern.toCharArray()) {
      if (character == '*') {
        regex.append(".*");
      } else if (character == '?') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(character)));
      }
    }
    return regex.toString();
  }
}
