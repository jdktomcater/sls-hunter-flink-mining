package com.sls.hunter.flink;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 三类 SAS 规则表。启动时从 classpath 读入，运行期只读。 */
public final class RuleData {
  private static final Map<String, Map<String, Object>> ALL = new HashMap<>();

  static {
    for (String family : J8.list("tracing_sas_linux", "tracing_sas_win", "tracing_sas_jumper")) {
      try (InputStream input = RuleData.class.getResourceAsStream("/rules/" + family + ".json")) {
        if (input == null) {
          throw new IllegalStateException(family);
        }
        ALL.put(family, Json.map(new String(J8.readAll(input), StandardCharsets.UTF_8)));
      } catch (Exception ex) {
        throw new ExceptionInInitializerError(ex);
      }
    }
  }

  private RuleData() {}

  public static Map<String, Object> data(String family) {
    return ALL.get(family);
  }

  @SuppressWarnings("unchecked")
  public static List<String> list(String family, String key) {
    return (List<String>) data(family).getOrDefault(key, Collections.<String>emptyList());
  }

  /** 名单按整个字符串精确匹配，不忽略大小写，也不去掉扩展名。 */
  public static boolean has(String family, String key, String value) {
    return list(family, key).contains(value);
  }

  public static boolean containsAny(String text, Collection<String> values) {
    return values.stream().anyMatch(text::contains);
  }
}
