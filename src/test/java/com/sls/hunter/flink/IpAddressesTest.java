package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class IpAddressesTest {
  @Test
  void classifiesBoundariesAndRejectsAmbiguousAddresses() {
    for (String ip : J8.list("10.0.0.1", "172.16.0.0", "172.31.255.255", "192.168.1.2",
        "100.64.0.0", "100.127.255.255", "fd00::1", "::ffff:10.0.0.1"))
      assertEquals(IpAddresses.Scope.INTERNAL, IpAddresses.scope(ip), ip);
    for (String ip : J8.list("172.15.255.255", "172.32.0.0", "100.63.255.255", "100.128.0.0", "8.8.8.8", "2606:4700:4700::1111"))
      assertEquals(IpAddresses.Scope.PUBLIC, IpAddresses.scope(ip), ip);
    for (String ip : J8.list("", "localhost", "172.16.0.999", "10.0.0.01", "127.0.0.1", "169.254.169.254",
        "0.0.0.0", "224.0.0.1", "192.0.2.1", "198.51.100.2", "203.0.113.1", "::1", "fe80::1", "ff02::1", "2001:db8::1", "fe80::1%en0"))
      assertEquals(IpAddresses.Scope.UNKNOWN, IpAddresses.scope(ip), ip);
    assertEquals(IpAddresses.canonical("fd00::1"), IpAddresses.canonical("FD00:0:0:0:0:0:0:1"));
    assertEquals("10.0.0.1", IpAddresses.canonical("::ffff:10.0.0.1"));
  }

  @Test
  void scoringOnlyTreatsPublicAddressCandidatesAsExternal() {
    assertTrue(externalReason("172.32.1.1"));
    assertTrue(externalReason("100.128.0.1"));
    assertTrue(externalReason("2606:4700:4700::1111"));
    assertFalse(externalReason("172.16.1.1"));
    assertFalse(externalReason("100.64.0.1"));
    assertFalse(externalReason("169.254.169.254"));
    assertFalse(externalReason("example.org"));
  }

  private boolean externalReason(String ip) {
    Event e = RulesTest.event("e", "tracing_sas_win", "2026-06-10", "a", "test.exe", "test.exe");
    e.fields.put("type", "Connect"); e.fields.put("dst_ip", ip);
    List<Event> events = Collections.singletonList(e);
    Scoring.Adjustment result = Scoring.adjust(e.family, 0, e.s("proc_name"), events,
        Templates.commands(events), Templates.chains(events), Collections.<String>emptySet(), 100, false);
    return ((List<?>) result.breakdown().get("structural_reasons")).contains("novel_external_connect");
  }
}
