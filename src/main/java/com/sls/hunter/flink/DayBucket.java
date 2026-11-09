package com.sls.hunter.flink;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 一天的聚合。身份集合完整保留，用来去重；命令样本、首条日志和父路径证据按实体截断。
 *
 * <p>三元组要求 username、proc_name、host_ip 三个字段都存在，空字符串也算存在，与旧 composite 聚合一致。
 */
public class DayBucket {
  /** 23:59:59。这一秒里带亚秒的事件沿用旧查询上界，不进入桶。 */
  private static final int LAST_SECOND_OF_DAY = 86_399;
  private static final int HOST_FIRST_LOGS = 3;
  private static final int URI_SAMPLES = 5;
  private static final int TRIPLE_SAMPLES = 12;
  private static final int FETCH_LATEST = 120;
  private static final int PATTERN_SAMPLES = 30;
  private static final int LATERAL_LATEST = 500;
  private static final List<String> WINDOWS_FETCH =
      J8.list("curl", "curl.exe", "wget", "powershell", "powershell.exe");
  private static final List<String> UNIX_FETCH = J8.list("curl", "wget");
  private static final List<String> LATERAL_PROCS = J8.list("ssh", "scp", "rsync");

  public Set<String> identities = new HashSet<>();
  public Map<String, Set<String>> procHosts = new TreeMap<>();
  public Map<String, Long> hosts = new TreeMap<>();
  public Map<String, Aggregate> triples = new TreeMap<>();
  public Map<String, Aggregate> uris = new TreeMap<>();
  public Map<String, Aggregate> upstreams = new TreeMap<>();
  public Map<String, Aggregate> patterns = new TreeMap<>();
  public Map<String, List<Event>> firstHost = new TreeMap<>();
  public Map<String, List<Event>> fetchLatest = new TreeMap<>();
  public List<Event> lateralLatest = new ArrayList<>();
  public long events;
  public boolean sealed;

  public DayBucket() {}

  /** 计数是全量的，样本是有界的。upstream 传入 0，只保留次数。 */
  public static class Aggregate {
    public long count;
    public List<Event> samples = new ArrayList<>();

    public Aggregate() {}

    public void add(Event event, int limit) {
      count++;
      keep(samples, event, limit, false);
    }
  }

  /** earliest 留下时间最早的，latest 留下时间最晚的。同时间戳用 event id 决定次序。 */
  public static void keep(List<Event> list, Event event, int limit, boolean latest) {
    list.add(event);
    list.sort(latest ? Event.ORDER.reversed() : Event.ORDER);
    if (list.size() > limit) {
      list.remove(list.size() - 1);
    }
  }

  /** @return 事件进入了这一天。重复身份和日末亚秒事件返回 false。 */
  public boolean add(Event event) {
    LocalTime local = Instant.ofEpochMilli(event.time).atZone(Event.ZONE).toLocalTime();
    if (local.toSecondOfDay() == LAST_SECOND_OF_DAY && local.getNano() > 0) {
      return false;
    }
    if (!identities.add(event.id)) {
      return false;
    }
    events++;
    if (event.fields.get("host_ip") != null) {
      hosts.merge(event.s("host_ip"), 1L, Long::sum);
      keep(
          firstHost.computeIfAbsent(event.s("host_ip"), key -> new ArrayList<>()),
          event,
          HOST_FIRST_LOGS,
          false);
    }
    if (event.fields.get("proc_name") != null && event.fields.get("host_ip") != null) {
      procHosts
          .computeIfAbsent(event.s("proc_name"), key -> new TreeSet<>())
          .add(event.s("host_ip"));
    }
    if (event.family.equals("tracing_nginx_hertzbeat")) {
      addNginx(event);
    } else {
      addSas(event);
    }
    return true;
  }

  private void addNginx(Event event) {
    uris.computeIfAbsent(Templates.uri(event.s("uri")), key -> new Aggregate())
        .add(event, URI_SAMPLES);
    upstreams.computeIfAbsent(event.s("upstream_addr"), key -> new Aggregate()).add(event, 0);
  }

  private void addSas(Event event) {
    if (event.fields.get("username") != null
        && event.fields.get("proc_name") != null
        && event.fields.get("host_ip") != null) {
      triples.computeIfAbsent(event.triple(), key -> new Aggregate()).add(event, TRIPLE_SAMPLES);
    }
    List<String> fetchProcs = event.family.equals("tracing_sas_win") ? WINDOWS_FETCH : UNIX_FETCH;
    if (fetchProcs.contains(event.s("proc_name"))) {
      keep(
          fetchLatest.computeIfAbsent(event.s("proc_name"), key -> new ArrayList<>()),
          event,
          FETCH_LATEST,
          true);
    }
    if (!event.family.equals("tracing_sas_linux")) {
      return;
    }
    for (Map<String, Object> pattern : Behavior.patterns()) {
      if (Behavior.matches(pattern.get("query"), event)) {
        patterns
            .computeIfAbsent(pattern.get("label").toString(), key -> new Aggregate())
            .add(event, PATTERN_SAMPLES);
      }
    }
    if (LATERAL_PROCS.contains(event.s("proc_name"))) {
      keep(lateralLatest, event, LATERAL_LATEST, true);
    }
  }
}
