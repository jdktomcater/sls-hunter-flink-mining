package com.sls.hunter.flink;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Conservative literal-address classification; never resolves a hostname. */
final class IpAddresses {
  enum Scope { INTERNAL, PUBLIC, UNKNOWN }

  private IpAddresses() {}

  static String canonical(String value) {
    if (value == null || value.isEmpty() || !value.equals(value.trim())) return "";
    try {
      byte[] bytes;
      if (value.indexOf(':') >= 0) {
        if (!value.matches("[0-9a-fA-F:.]+")) return "";
        if (value.indexOf('.') >= 0 && parseV4(value.substring(value.lastIndexOf(':') + 1)) == null) return "";
        bytes = InetAddress.getByName(value).getAddress();
      } else {
        bytes = parseV4(value);
      }
      return bytes == null ? "" : InetAddress.getByAddress(bytes).getHostAddress();
    } catch (UnknownHostException ex) {
      return "";
    }
  }

  private static byte[] parseV4(String value) {
    String[] parts = value.split("\\.", -1);
    if (parts.length != 4) return null;
    byte[] result = new byte[4];
    for (int i = 0; i < 4; i++) {
      if (!parts[i].matches("0|[1-9][0-9]{0,2}")) return null;
      int n = Integer.parseInt(parts[i]);
      if (n > 255) return null;
      result[i] = (byte) n;
    }
    return result;
  }

  static Scope scope(String value) {
    String canonical = canonical(value);
    if (canonical.isEmpty()) return Scope.UNKNOWN;
    try {
      byte[] b = InetAddress.getByName(canonical).getAddress();
      int a = b[0] & 255, c = b[1] & 255;
      if (b.length == 4) {
        int d = b[2] & 255;
        if (a == 10 || (a == 172 && c >= 16 && c <= 31) || (a == 192 && c == 168)
            || (a == 100 && c >= 64 && c <= 127)) return Scope.INTERNAL;
        if (a == 0 || a == 127 || a >= 224 || (a == 169 && c == 254)
            || (a == 192 && c == 0 && (d == 0 || d == 2))
            || (a == 192 && c == 88 && d == 99)
            || (a == 198 && (c == 18 || c == 19 || (c == 51 && d == 100)))
            || (a == 203 && c == 0 && d == 113)) return Scope.UNKNOWN;
        return Scope.PUBLIC;
      }
      if ((a & 0xfe) == 0xfc) return Scope.INTERNAL;
      // Only ordinary global-unicast candidates. Special-use blocks remain unknown.
      if ((a & 0xe0) == 0x20 && !(a == 0x20 && c == 0x01 && ((b[2] & 0xfe) == 0))
          && !(a == 0x20 && c == 0x01 && (b[2] & 255) == 0x0d && (b[3] & 255) == 0xb8)
          && !(a == 0x20 && c == 0x02)
          && !(a == 0x3f && c == 0xff && ((b[2] & 0xf0) == 0))) return Scope.PUBLIC;
      return Scope.UNKNOWN;
    } catch (UnknownHostException ex) {
      return Scope.UNKNOWN;
    }
  }
}
