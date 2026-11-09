package com.sls.hunter.flink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Java 8 替代 List.of / Set.of。返回的集合不可修改。 */
final class J8 {
  private J8() {}

  @SafeVarargs
  static <T> List<T> list(T... items) {
    return Collections.unmodifiableList(Arrays.asList(items));
  }

  @SafeVarargs
  static <T> Set<T> set(T... items) {
    return Collections.unmodifiableSet(new LinkedHashSet<T>(Arrays.asList(items)));
  }

  static byte[] readAll(InputStream input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int n;
    while ((n = input.read(buffer)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  static String hex(byte[] bytes) {
    char[] digits = "0123456789abcdef".toCharArray();
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int value = bytes[i] & 0xff;
      out[i * 2] = digits[value >>> 4];
      out[i * 2 + 1] = digits[value & 0x0f];
    }
    return new String(out);
  }
}
