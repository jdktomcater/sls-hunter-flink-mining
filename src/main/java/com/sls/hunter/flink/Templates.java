package com.sls.hunter.flink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** 命令、URI 和父链的归一化。掩码顺序和截断长度都是规则的一部分。 */
public final class Templates {
  private Templates() {}

  /** 按 Unicode 码点截断，避免把代理对切成一半。 */
  public static String cut(String text, int codePoints) {
    return text.codePointCount(0, text.length()) <= codePoints
        ? text
        : text.substring(0, text.offsetByCodePoints(0, codePoints));
  }

  /**
   * 先掩掉 URL、IP 和十六进制，再按空白切词。纯数字和 16 位以上的十六进制词变成 {@code <*>}，路径中的数字也替换，超长词变成
   * {@code <LONG>}。{@code (?U)} 用来覆盖全角数字。
   */
  public static String command(String text) {
    String masked =
        text.replaceAll("(?U)^\\s+|\\s+$", "")
            .replaceAll(
                "(?iU)https?://[^\\s]+|brd-customer[^\\s]*@brd\\.superproxy\\.io[^\\s]*", "<URL>")
            .replaceAll("(?U)\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b", "<IP>")
            .replaceAll("(?i)\\b0x[0-9a-f]+\\b", "<HEX>");
    if (masked.isEmpty()) {
      return "";
    }
    List<String> tokens = new ArrayList<>();
    for (String token : masked.split("(?U)\\s+")) {
      if (token.matches("(?U)\\d+|(?i)[0-9a-f]{16,}")) {
        token = "<*>";
      } else if (token.startsWith("/") && Pattern.compile("(?U)/\\d+").matcher(token).find()) {
        token = token.replaceAll("(?U)\\d+", "<*>");
      } else if (token.codePointCount(0, token.length()) > 64) {
        token = "<LONG>";
      }
      tokens.add(token);
    }
    return String.join(" ", tokens);
  }

  /** 先去掉 query，再把长十六进制段换成 {@code <ID>}，最后把剩余数字换成 {@code <N>}。 */
  public static String uri(String uri) {
    return uri.split("\\?", 2)[0]
        .replaceAll("/[0-9a-f]{8,}", "/<ID>")
        .replaceAll("(?U)\\d+", "<N>");
  }

  /**
   * 解析父链 JSON。列表和对象都只取值；解析失败时退回截断后的原文。null 写成 {@code None}，与 Python 的字符串形式一致。
   */
  public static List<String> parents(String chain) {
    if (chain.length() < 3) {
      return Collections.<String>emptyList();
    }
    List<String> values = new ArrayList<>();
    try {
      Object parsed = Json.MAPPER.readValue(chain, Object.class);
      if (parsed instanceof List) {
        for (Object item : (List<?>) parsed) {
          if (item instanceof Map) {
            for (Object value : ((Map<?, ?>) item).values()) {
              values.add(pythonText(value));
            }
          } else {
            values.add(pythonText(item));
          }
        }
      } else if (parsed instanceof Map) {
        for (Object value : ((Map<?, ?>) parsed).values()) {
          values.add(pythonText(value));
        }
      }
    } catch (Exception ex) {
      values.add(cut(chain, 200));
    }
    return values;
  }

  private static String pythonText(Object value) {
    return value == null ? "None" : value.toString();
  }

  /** 父链前 4 段，每段先做命令归一化再截到 80 个码点，用箭头连接。 */
  public static String chain(String raw) {
    List<String> parts = new ArrayList<String>();
    for (String segment : parents(raw)) {
      if (parts.size() == 4) {
        break;
      }
      if (!segment.isEmpty()) {
        parts.add(cut(command(segment), 80));
      }
    }
    return String.join(" → ", parts);
  }

  /** 命令行去掉空白后至少 4 个字符才计入模板。 */
  public static Map<String, Integer> commands(List<Event> events) {
    Map<String, Integer> counts = new LinkedHashMap<>();
    for (Event event : events) {
      if (event.s("cmdline").trim().length() >= 4) {
        counts.merge(command(event.s("cmdline")), 1, Integer::sum);
      }
    }
    return counts;
  }

  public static Map<String, Integer> chains(List<Event> events) {
    Map<String, Integer> counts = new LinkedHashMap<>();
    for (Event event : events) {
      String signature = chain(event.s("cmd_chain"));
      if (!signature.isEmpty()) {
        counts.merge(signature, 1, Integer::sum);
      }
    }
    return counts;
  }
}
