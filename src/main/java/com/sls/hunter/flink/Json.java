package com.sls.hunter.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;

/** JSON 辅助。对象使用 LinkedHashMap，字段顺序参与快照签名。 */
public final class Json {
  public static final ObjectMapper MAPPER = new ObjectMapper();

  private Json() {}

  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (Exception ex) {
      throw new IllegalArgumentException("JSON encoding failed", ex);
    }
  }

  public static Map<String, Object> map(String value) {
    try {
      Map<String, Object> parsed =
          MAPPER.readValue(value, new TypeReference<LinkedHashMap<String, Object>>() {});
      if (parsed == null) {
        throw new IllegalArgumentException("JSON object cannot be null");
      }
      return parsed;
    } catch (Exception ex) {
      throw new IllegalArgumentException("Invalid JSON object", ex);
    }
  }

  public static String str(Map<String, ?> object, String key) {
    Object value = object.get(key);
    return value == null ? "" : value.toString();
  }

  /** 交替写入键和值。键按参数顺序保留。 */
  public static Map<String, Object> obj(Object... pairs) {
    Map<String, Object> object = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      object.put((String) pairs[i], pairs[i + 1]);
    }
    return object;
  }
}
