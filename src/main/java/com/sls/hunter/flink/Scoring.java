package com.sls.hunter.flink;

import static com.sls.hunter.flink.RuleData.containsAny;
import static com.sls.hunter.flink.RuleData.has;
import static com.sls.hunter.flink.RuleData.list;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * SAS 历史差集评分。权重和命中顺序对齐旧 Python 规则，中间结果用银行家舍入保留两位。
 *
 * <p>预评分只决定哪些候选进入 deep；正式分先算 base，再按族做结构加分（封顶 45）、部署调整和白名单
 * 封顶。罕见网关的 +85 在 {@link MiningEngine} 里、白名单之后加上，不能前移。
 */
public final class Scoring {
  /** 结构加分上限。封顶发生在部署调整和白名单之前。 */
  private static final double STRUCTURAL_CAP = 45;

  private static final Set<String> WINDOWS_SCRIPT_HOSTS =
      J8.set("cmd", "powershell", "pwsh", "wscript", "cscript", "mshta");
  private static final Set<String> JUMP_HALVE_REASONS =
      J8.set(
          "fetch_child_shell_parent_chain",
          "proxy_fetch_flag",
          "few_novel_chains_fetch_proc",
          "novel_external_connect");

  private Scoring() {}

  /** 对齐 Python round(..., 2) 的银行家舍入。 */
  public static double round(double value) {
    return new BigDecimal(value).setScale(2, RoundingMode.HALF_EVEN).doubleValue();
  }

  /** 进程名去掉首个 '.' 之后的部分，Windows 用它忽略 .exe。 */
  public static String base(String name) {
    return name.toLowerCase(Locale.ROOT).split("\\.", 2)[0];
  }

  public static boolean re(String regex, String text) {
    return Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS)
        .matcher(text)
        .find();
  }

  /** URL、裸 IP 或主机路径里出现 "/." 隐藏段。 */
  public static final String DOT =
      "(?:https?://[^/]*/\\.[A-Za-z0-9][\\w.-]*|\\d{1,3}(?:\\.\\d{1,3}){3}/\\.[A-Za-z0-9]|[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/\\.[A-Za-z0-9])";

  /** Windows 临时目录：temp、tmp、low，以及 AppData\\Local\\Temp。 */
  public static final String TEMP = "[/\\\\](temp|tmp|low)[/\\\\]|appdata[/\\\\]local[/\\\\]temp";

  /** 企业终端代理落在 syscon 下的标准文件名。 */
  public static final String AGENT =
      "[/\\\\]+windows[/\\\\]+system[/\\\\]+sys[/\\\\]+syscon[/\\\\]+(mssys|mskes|mdua)\\.exe";

  /** 与标准代理配套的采集脚本。两者同时出现才视为正常采集。 */
  public static final String BAT =
      "appdata[/\\\\]local[/\\\\]temp[/\\\\]low[/\\\\](tasklist|netstat|sysinfo)\\.bat";

  /** PowerShell -enc / -e 后面的长 Base64。 */
  public static final String ENC = "-enc\\s+[A-Za-z0-9+/=]{20,}|-e\\s+[A-Za-z0-9+/=]{20,}";

  /** 标准采集：代理路径和采集 bat 同时出现。 */
  public static boolean collection(String text) {
    return re(BAT, text) && re(AGENT, text);
  }

  /** 进程本身是 mssys/mskes/mdua，并且路径落在标准 syscon 目录。 */
  public static boolean agent(String text, String proc) {
    return J8.set("mssys", "mskes", "mdua").contains(base(proc)) && re(AGENT, text);
  }

  /** Windows 进程名单按 base 名比较，因此 curl 与 curl.exe 视为同一项。 */
  public static boolean memberBase(String family, String listKey, String proc) {
    return list(family, listKey).stream().map(Scoring::base).anyMatch(base(proc)::equals);
  }

  /**
   * 预评分，只用于 deep 名额排序，不写入最终 score。
   *
   * <p>{@code novel} 表示三元组在基线中没有出现。进程名单用全名精确匹配；Windows 的去扩展名匹配只用于结构加分。
   */
  public static double pre(
      boolean novel,
      long targetCount,
      long baselineCount,
      double ratio,
      int fleetHosts,
      String proc,
      String family) {
    String procLower = proc.toLowerCase(Locale.ROOT);
    double procPoints =
        has(family, "RARE_GATEWAY_PROCS", procLower)
            ? 40
            : has(family, "NETWORK_FETCH_PROCS", procLower) ? 15 : 0;
    return round(
        (novel ? 30 : 0)
            + fleetRarity(fleetHosts, 25, 18, 12, 6)
            + procPoints
            + spikeWeight(ratio, 20, 1.5)
            + (baselineCount == 0 && targetCount >= 2 ? 10 : 0)
            + Math.min(targetCount / 5.0, 10));
  }

  /**
   * 正式分的 base。{@code novelTriple} 与预评分的 novel 同义，表示三元组在基线中没有出现；{@code novelHost}
   * 是另一项。突增分档 3 和 10 只用于打分，候选过滤仍用未舍入比值是否达到 2。
   */
  public static double score(
      boolean novelHost,
      boolean novelTriple,
      long targetCount,
      long baselineCount,
      double ratio,
      int fleetHosts,
      int novelTemplates,
      int novelChains) {
    return round(
        (novelHost ? 25 : 0)
            + (novelTriple ? 22 : 0)
            + Math.min(novelTemplates * 3, 24)
            + Math.min(novelChains * 4, 20)
            + fleetRarity(fleetHosts, 18, 10, 4, 0)
            + spikeWeight(ratio, 25, 2)
            + Math.min(targetCount / 10.0, 12)
            + (baselineCount == 0 && targetCount >= 2 ? 8 : 0));
  }

  /** 基线里出现过该进程的主机越少，稀有加分越高。档位边界是 1、3、8、25。 */
  private static double fleetRarity(
      int fleetHosts, double atMost1, double atMost3, double atMost8, double atMost25) {
    if (fleetHosts <= 1) {
      return atMost1;
    }
    if (fleetHosts <= 3) {
      return atMost3;
    }
    if (fleetHosts <= 8) {
      return atMost8;
    }
    return fleetHosts <= 25 ? atMost25 : 0;
  }

  /** ratio ≥ 10 时封顶，≥ 3 时按倍率，低于 3 不加分。 */
  private static double spikeWeight(double ratio, double highCap, double midFactor) {
    if (ratio >= 10) {
      return Math.min(ratio, highCap);
    }
    if (ratio >= 3) {
      return ratio * midFactor;
    }
    return 0;
  }

  /** 样本命令行和父链拼成一段，供正则规则扫描。 */
  public static String chainBlob(List<Event> samples) {
    List<String> parts = new ArrayList<>();
    for (Event event : samples) {
      parts.add(event.s("cmdline"));
      parts.add(event.s("cmd_chain"));
    }
    return String.join(" ", parts);
  }

  /** curl/wget 一类带代理参数的拉取。Windows 认 "-x " 和 " -proxy "，其他族只认 "-x"。 */
  public static boolean proxy(String text, boolean windows) {
    String lower = text.toLowerCase(Locale.ROOT);
    return (windows ? (lower.contains("-x ") || lower.contains(" -proxy ")) : lower.contains("-x"))
        && lower.contains("://");
  }

  /** 下载模板上的可疑形状：隐藏路径、占位地址、管道进解释器，或编码命令。 */
  public static boolean suspicious(String text, boolean windows) {
    String lower = text.toLowerCase(Locale.ROOT);
    return re(DOT, text)
        || (lower.contains("connect-timeout") && (lower.contains("<ip>") || lower.contains("<*>")))
        || (text.contains("|")
            && (lower.contains("curl") || lower.contains("wget") || (windows && lower.contains("powershell"))))
        || (windows ? re(ENC, text) : proxy(text, false));
  }

  /**
   * 从父链首段取出 shell 直接调用的绝对路径。只接受 /bin、/usr/bin、/sbin、/usr/sbin，其他返回空串。
   */
  public static String invoked(String parent) {
    String[] parts = parent.trim().split("\\s+");
    if (parts.length < 2) {
      return "";
    }
    String shell = parts[0].substring(parts[0].lastIndexOf('/') + 1).toLowerCase(Locale.ROOT);
    if (!shell.contains("sh") && !shell.contains("bash")) {
      return "";
    }
    return parts[1].matches("^/(usr/bin|bin|usr/sbin|sbin)/.*") ? parts[1] : "";
  }

  /**
   * 统一铺开：模板至少 5 个且每个最多出现 2 次，父链形状不超过 8 种，并且所有父链的末段相同。
   * 这种形状记为部署，而不是单点入侵。
   */
  public static boolean rollout(Map<String, Integer> templates, Map<String, Integer> chains) {
    if (templates.size() < 5 || Collections.max(templates.values()) > 2 || chains.size() > 8) {
      return false;
    }
    Set<String> tails = new HashSet<>();
    for (String signature : chains.keySet()) {
      String[] parts = signature.split("→");
      if (parts.length >= 2) {
        tails.add(Templates.cut(parts[parts.length - 1].trim(), 60));
      }
    }
    return tails.size() == 1;
  }

  /** 调整后的分数，以及要写进 score_breakdown 的明细。 */
  public static final class Adjustment {
    private final double score;
    private final Map<String, Object> breakdown;

    public Adjustment(double score, Map<String, Object> breakdown) {
      this.score = score;
      this.breakdown = breakdown;
    }

    public double score() {
      return score;
    }

    public Map<String, Object> breakdown() {
      return breakdown;
    }
  }

  /**
   * 在 base 上叠加结构加分、部署调整和白名单。
   *
   * <p>{@code novelTemplates} 与 {@code novelChains} 由调用方去掉基线里已经见过的形状，这里不再过滤。
   * 原因列表的顺序就是命中顺序。
   */
  public static Adjustment adjust(
      String family,
      double baseScore,
      String proc,
      List<Event> samples,
      Map<String, Integer> novelTemplates,
      Map<String, Integer> novelChains,
      Set<String> baselineParentPaths,
      int fleetHosts) {
    return adjust(
        family,
        baseScore,
        proc,
        samples,
        novelTemplates,
        novelChains,
        baselineParentPaths,
        fleetHosts,
        true);
  }

  /**
   * {@code historyCompare} 为 false 时，模板和父链按当天样本原样传入，不再做基线差集。新父路径、少见新父链、
   * 以及依赖“基线里没见过”的部署降分都不计。
   */
  public static Adjustment adjust(
      String family,
      double baseScore,
      String proc,
      List<Event> samples,
      Map<String, Integer> novelTemplates,
      Map<String, Integer> novelChains,
      Set<String> baselineParentPaths,
      int fleetHosts,
      boolean historyCompare) {
    boolean windows = family.equals("tracing_sas_win");
    boolean jumper = family.equals("tracing_sas_jumper");
    String procLower = proc.toLowerCase(Locale.ROOT);
    String procBase = base(procLower);
    String blob = chainBlob(samples);
    List<String> reasons = new ArrayList<>();
    double bonus = 0;
    // Windows 名单含 .exe，按 base 比较；Linux/Jumper 仍是全名精确匹配。
    boolean gateway =
        windows
            ? memberBase(family, "RARE_GATEWAY_PROCS", procLower)
            : has(family, "RARE_GATEWAY_PROCS", procLower);
    boolean fetch =
        windows
            ? memberBase(family, "NETWORK_FETCH_PROCS", procLower)
            : has(family, "NETWORK_FETCH_PROCS", procLower);

    if (gateway && fleetHosts <= 25) {
      // 主机越少越高，14 起、24 封顶。
      bonus += Math.min(24, 14 + Math.max(0, 25 - fleetHosts));
      reasons.add("rare_gateway_proc");
    }
    if (windows && memberBase(family, "ENTERPRISE_AGENT_PROCS", procLower) && !agent(blob, procLower)) {
      // 进程在企业代理集合里，但不是 syscon 下的标准 mssys/mskes/mdua。
      if (re(AGENT, blob) && fleetHosts < 50) {
        bonus += 20;
        reasons.add("nonstandard_enterprise_agent_path");
      }
      if (re(TEMP, blob) && !collection(blob)) {
        bonus += 14;
        reasons.add("agent_temp_path_spawn");
      }
    }
    if (windows
        && WINDOWS_SCRIPT_HOSTS.contains(procBase)
        && !collection(blob)
        && samples.stream().anyMatch(event -> re(TEMP, event.s("cmdline")) || re(TEMP, event.s("proc_path")))) {
      bonus += 16;
      reasons.add("temp_path_script_execution");
    }
    if (pipedToInterpreter(samples, windows)) {
      bonus += 10;
      reasons.add("pipe_to_interpreter");
    }
    if (samples.stream().anyMatch(event -> re(DOT, event.s("cmdline")))) {
      bonus += 12;
      reasons.add("dot_segment_url_path");
    }
    if (samples.stream().anyMatch(event -> proxy(event.s("cmdline"), windows))) {
      bonus += 10;
      reasons.add("proxy_fetch_flag");
    }
    if (windows && re(ENC, blob)) {
      bonus += 18;
      reasons.add("encoded_powershell");
    }
    if (windows
        && memberBase(family, "LOL_BIN_PROCS", procLower)
        && samples.stream()
            .anyMatch(
                event ->
                    re(TEMP, event.s("cmdline"))
                        || event.s("cmdline").toLowerCase(Locale.ROOT).contains(".dll"))) {
      bonus += 12;
      reasons.add("lolbin_suspicious_target");
    }
    if (fetch) {
      if (fetchLaunchedByShell(novelChains, windows)) {
        bonus += 14;
        reasons.add("fetch_child_shell_parent_chain");
      }
      if (novelTemplates.keySet().stream().anyMatch(template -> suspicious(template, windows))) {
        bonus += 6;
        reasons.add("suspicious_fetch_template");
      }
      if (historyCompare) {
        String parentReason = novelParentReason(samples, windows, baselineParentPaths);
        if (parentReason != null) {
          bonus += parentBonus(parentReason, windows);
          reasons.add(parentReason);
        }
        if (!novelChains.isEmpty() && novelChains.size() <= 4) {
          bonus += 8;
          reasons.add("few_novel_chains_fetch_proc");
        }
      }
    }
    if ((windows || jumper) && externalConnect(samples)) {
      bonus += 6;
      reasons.add("novel_external_connect");
    }
    // 跳板机上的发布动作仍保留攻击形状，但结构加分减半。减半在 45 封顶之前。
    if (jumper
        && Whitelist.deployment(samples, novelTemplates)
        && J8.set("curl", "wget", "fetch", "ssh").contains(procBase)
        && reasons.stream().anyMatch(JUMP_HALVE_REASONS::contains)) {
      bonus = round(bonus * 0.5);
      reasons.add("deployment_context_struct_halved");
    }
    bonus = round(Math.min(bonus, STRUCTURAL_CAP));

    double deployment = 0;
    String deploymentReason = null;
    if (historyCompare && !windows && !fetch && !gateway && rollout(novelTemplates, novelChains)) {
      deployment = -18;
      deploymentReason = "uniform_parent_many_oneoff_templates";
    }
    Whitelist.Result whitelist =
        windows
            ? Whitelist.windows(proc, samples, novelTemplates, novelChains, fleetHosts)
            : jumper
                ? Whitelist.jumper(proc, samples, novelTemplates, bonus, reasons, fleetHosts)
                : new Whitelist.Result(0, null, null, null);
    // Windows 的部署调整就是白名单降分，覆盖上面的 rollout。Jumper 的白名单另加，并可封顶。
    if (windows) {
      deployment = whitelist.adjustment();
      deploymentReason = whitelist.rule();
    }
    double result = round(baseScore + bonus + deployment + (jumper ? whitelist.adjustment() : 0));
    if (jumper && whitelist.cap() != null) {
      result = Math.min(result, whitelist.cap());
    }

    Map<String, Object> detail =
        Json.obj(
            "base_score",
            baseScore,
            "structural_bonus",
            bonus,
            "structural_reasons",
            reasons,
            "deployment_adjustment",
            deployment,
            "deployment_reason",
            deploymentReason);
    if (windows || jumper) {
      detail.put("whitelist_rule", whitelist.rule());
      if (whitelist.block() != null) {
        detail.put("whitelist_blocked", whitelist.block());
      }
      if (jumper) {
        detail.put("whitelist_adjustment", whitelist.adjustment());
      }
      if (whitelist.cap() != null) {
        detail.put("score_cap", whitelist.cap());
      }
    }
    return new Adjustment(result, detail);
  }

  /** 管道右侧落到 shell 或解释器。一条样本命中即成立。 */
  private static boolean pipedToInterpreter(List<Event> samples, boolean windows) {
    List<String> shells =
        windows
            ? J8.list("cmd", "powershell", "pwsh")
            : J8.list("bash", "sh", "dash", "zsh", "/bin/sh", "python");
    for (Event event : samples) {
      String command = event.s("cmdline");
      if (!command.contains("|")) {
        continue;
      }
      String tail = command.substring(command.lastIndexOf('|') + 1).trim().toLowerCase(Locale.ROOT);
      for (String shell : shells) {
        if (tail.startsWith(shell)
            || tail.contains("/" + shell)
            || (windows && tail.contains("\\" + shell))) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * 父链签名按解析顺序用 "→" 拼接。第一段像下载工具、第二段像 shell 时记为拉取拉起解释器。 "cmd " 带空格，用来区别 cmd.exe。
   */
  private static boolean fetchLaunchedByShell(Map<String, Integer> chains, boolean windows) {
    List<String> tools =
        windows ? J8.list("curl", "wget", "bitsadmin", "powershell") : J8.list("curl", "wget", "fetch");
    List<String> shells =
        windows
            ? J8.list("cmd.exe", "cmd ", "powershell", "pwsh")
            : J8.list("bash", "/bin/sh", " /sh ", "shell");
    for (String signature : chains.keySet()) {
      String[] parts = signature.split("→");
      if (parts.length < 2) {
        continue;
      }
      String first = parts[0].toLowerCase(Locale.ROOT);
      String second = parts[1].toLowerCase(Locale.ROOT);
      if (containsAny(first, tools) && containsAny(second, shells)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 下载进程的父可执行文件。基线路径集非空且对不上时记为新路径；Linux 在基线路径为空、 且父路径落在 /usr/bin/ 下时另记一条。返回的字符串是 breakdown
   * 原因名。
   */
  private static String novelParentReason(
      List<Event> samples, boolean windows, Set<String> baselineParentPaths) {
    for (Event event : samples) {
      String executable = firstParentExecutable(event, windows);
      if (executable.isEmpty()) {
        continue;
      }
      if (!baselineParentPaths.isEmpty() && !baselineParentPaths.contains(executable)) {
        return windows ? "novel_parent_executable_path" : "novel_shell_parent_path";
      }
      String fileName = executable.substring(executable.lastIndexOf('/') + 1);
      if (!windows
          && baselineParentPaths.isEmpty()
          && executable.startsWith("/usr/bin/")
          && fileName.length() >= 4) {
        return "shell_parent_usr_bin_invoke";
      }
    }
    return null;
  }

  /** 新父路径：Windows +14，其他族 +16；仅有 /usr/bin 调用时 +10。 */
  private static double parentBonus(String reason, boolean windows) {
    if (reason.equals("shell_parent_usr_bin_invoke")) {
      return 10;
    }
    return windows ? 14 : 16;
  }

  /** 只看父链第一段。Windows 取第一个盘符路径；其他族取 {@link #invoked} 的结果。 */
  private static String firstParentExecutable(Event event, boolean windows) {
    List<String> parents = Templates.parents(event.s("cmd_chain"));
    if (parents.isEmpty()) {
      return "";
    }
    if (!windows) {
      return invoked(parents.get(0));
    }
    for (String token : parents.get(0).split("\\s+")) {
      String cleaned = token.replaceAll("^[\"']+|[\"']+$", "");
      if (cleaned.contains(":") && cleaned.contains("\\")) {
        return cleaned;
      }
    }
    return "";
  }

  /** Connect 且目的地址不是 10.*、172.*、192.168.*。172 前缀比 RFC1918 更宽，保持旧规则。 */
  private static boolean externalConnect(List<Event> samples) {
    for (Event event : samples) {
      String destination = event.s("dst_ip");
      if (event.s("type").equals("Connect")
          && !destination.isEmpty()
          && !destination.startsWith("10.")
          && !destination.startsWith("172.")
          && !destination.startsWith("192.168.")) {
        return true;
      }
    }
    return false;
  }
}
