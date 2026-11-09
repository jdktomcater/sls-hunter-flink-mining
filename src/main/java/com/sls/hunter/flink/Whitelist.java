package com.sls.hunter.flink;

import static com.sls.hunter.flink.RuleData.containsAny;
import static com.sls.hunter.flink.RuleData.data;
import static com.sls.hunter.flink.RuleData.has;
import static com.sls.hunter.flink.RuleData.list;
import static com.sls.hunter.flink.Scoring.AGENT;
import static com.sls.hunter.flink.Scoring.DOT;
import static com.sls.hunter.flink.Scoring.ENC;
import static com.sls.hunter.flink.Scoring.TEMP;
import static com.sls.hunter.flink.Scoring.agent;
import static com.sls.hunter.flink.Scoring.base;
import static com.sls.hunter.flink.Scoring.chainBlob;
import static com.sls.hunter.flink.Scoring.collection;
import static com.sls.hunter.flink.Scoring.memberBase;
import static com.sls.hunter.flink.Scoring.proxy;
import static com.sls.hunter.flink.Scoring.re;
import static com.sls.hunter.flink.Scoring.rollout;
import static com.sls.hunter.flink.Scoring.suspicious;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 白名单不丢事件，只下调分数或设置封顶。
 *
 * <p>Jumper 按固定优先级返回第一条命中规则。Windows 先排除攻击形状（命中则不降分），再在资产规则里取降幅最大的一条；
 * 降幅相同时保留先加入的规则。
 */
public final class Whitelist {
  private static final String WINDOWS = "tracing_sas_win";
  private static final String JUMPER = "tracing_sas_jumper";
  private static final Set<String> STRUCTURAL_ATTACK_REASONS =
      J8.set(
          "fetch_child_shell_parent_chain",
          "novel_shell_parent_path",
          "pipe_to_interpreter",
          "dot_segment_url_path",
          "proxy_fetch_flag",
          "rare_gateway_proc");

  private Whitelist() {}

  /**
   * @param adjustment 加到分数上的值，降分是负数
   * @param rule 命中的白名单规则名；未命中为 null
   * @param cap 分数上限；没有上限为 null
   * @param block 拒绝降分的原因；未拦截为 null
   */
  public static final class Result {
    private final double adjustment;
    private final String rule;
    private final Double cap;
    private final String block;

    public Result(double adjustment, String rule, Double cap, String block) {
      this.adjustment = adjustment;
      this.rule = rule;
      this.cap = cap;
      this.block = block;
    }

    public double adjustment() {
      return adjustment;
    }

    public String rule() {
      return rule;
    }

    public Double cap() {
      return cap;
    }

    public String block() {
      return block;
    }
  }

  private static Result result(double adjustment, String rule) {
    return new Result(adjustment, rule, null, null);
  }

  private static Result cap(double adjustment, String rule, double scoreCap) {
    return new Result(adjustment, rule, scoreCap, null);
  }

  /** 命令、路径、用户、目的地址和模板拼在一起，供发布特征扫描。 */
  private static String jumpBlob(List<Event> samples, Map<String, Integer> templates) {
    List<String> parts = new ArrayList<>();
    for (Event event : samples) {
      for (String key : J8.list("cmdline", "proc_path", "cmd_chain", "username", "dst_ip", "domain"))
        parts.add(event.s(key));
    }
    parts.addAll(templates.keySet());
    return String.join("\n", parts).toLowerCase(Locale.ROOT);
  }

  /** 样本或模板里出现跳板机发布标记。 */
  public static boolean deployment(List<Event> samples, Map<String, Integer> templates) {
    return containsAny(jumpBlob(samples, templates), list(JUMPER, "DEPLOYMENT_MARKERS"));
  }

  public static Result jumper(
      String proc,
      List<Event> samples,
      Map<String, Integer> templates,
      double structuralBonus,
      List<String> reasons,
      int fleetHosts) {
    String procBase = base(proc);
    String blob = jumpBlob(samples, templates);
    List<String> chainParts = new ArrayList<String>();
    for (Event event : samples) chainParts.add(event.s("cmd_chain"));
    String chain = String.join("\n", chainParts).toLowerCase(Locale.ROOT);
    boolean deploy = deployment(samples, templates);
    boolean fetch = has(JUMPER, "FETCH_PROCS", procBase);
    boolean container = containsAny(chain, J8.list("runc init", "containerd-shim"));
    boolean attack =
        structuralBonus >= 10 || reasons.stream().anyMatch(STRUCTURAL_ATTACK_REASONS::contains);

    // 只有连接日志、没有进程名时，分数压到 15。
    if ((proc.isEmpty() || proc.equals("N/A"))
        && !samples.isEmpty()
        && samples.stream().allMatch(event -> event.s("type").equals("Connect")))
      return cap(-40, "connect_only_event", 15);

    if (fetch) {
      if (samples.stream()
          .anyMatch(
              event ->
                  re("metadata-flavor:\\s*google|169\\.254\\.169\\.254", event.s("cmdline"))
                      || event.s("dst_ip").equals("169.254.169.254")))
        return cap(-120, "gcp_metadata_curl", 0);
      if (container && re("https?://(?:127\\.0\\.0\\.1|localhost)(?::\\d+)?", blob))
        return cap(-80, "docker_healthcheck_curl", 20);
      if (deploy && attack) {
        return result(-50, "deployment_context_fetch");
      }
    }
    if (procBase.equals("wget") && blob.contains("github.com/docker/compose") && deploy)
      return cap(-70, "deployment_context_fetch", 25);
    if (fetch && containsAny(blob, J8.list("jumpserver/installer/releases", "jumpserver-installer-")))
      return cap(-75, "jumpserver_installer_fetch", 25);
    if (fetch
        && containsAny(blob, list(JUMPER, "OPS_EGRESS_DOMAINS"))
        && containsAny(blob, list(JUMPER, "OPS_USER_MARKERS")))
      return cap(-45, "ops_egress_self_check", 40);

    // 解释器、下载工具和永不白名单进程不再降分。有攻击形状时单独记录拦截原因。
    boolean never =
        has(JUMPER, "NEVER_WHITELIST_PROCS", procBase)
            || has(JUMPER, "INTERPRETER_PROCS", procBase)
            || fetch;
    if (never)
      return new Result(
          0,
          null,
          null,
          attack ? "structural_attack_shape" : "never_whitelist_proc:" + procBase);

    if (has(JUMPER, "COMPILE_TOOL_PROCS", procBase)
        && blob.contains("make")
        && (blob.contains("redis") || blob.contains("jemalloc") || blob.replace(" ", "").contains("usesystemd")))
      return cap(-90, "bastion_compile_deploy", 45);
    if (has(JUMPER, "DEPLOY_HELPER_PROCS", procBase) && deploy)
      return cap(-55, "deployment_helper_proc", 35);
    if (procBase.equals("ssh")) {
      if (chain.contains("test_ssh_login") && blob.contains(" exit"))
        return cap(-60, "ssh_batch_login_test", 40);
      if (containsAny(blob, list(JUMPER, "OPS_SSH_MARKERS")) && blob.contains("opsvictor@"))
        return cap(-55, "ops_batch_ssh_patrol", 45);
      if (containsAny(blob, list(JUMPER, "NOTIFY_SCRIPT_MARKERS")))
        return cap(-50, "ssh_login_notify_script", 35);
      if (containsAny(blob, J8.list("/usr/local/bin/ansible", "/data/ansible/")))
        return cap(-45, "jumpserver_ansible_deploy", 50);
      if (fleetHosts >= 8) {
        return cap(-30, "sshd_bastion_baseline", 55);
      }
    }
    if ((has(JUMPER, "JUMPSERVER_STACK", procBase)
            || containsAny(blob, list(JUMPER, "JUMPSERVER_PATH_MARKERS")))
        && (fleetHosts >= 3 || blob.contains("jumpserver"))) return result(-38, "jumpserver_stack");
    if (has(JUMPER, "CONTAINER_RUNTIME", procBase) || (container && has(JUMPER, "MONITORING", procBase)))
      return cap(-45, "container_runtime", 35);
    if (has(JUMPER, "CRON_MAINT", procBase) || containsAny(blob, list(JUMPER, "CRON_PATH_MARKERS")))
      return cap(-45, "cron_maintenance", 30);
    if (has(JUMPER, "MONITORING", procBase) && (container || fleetHosts >= 3))
      return cap(-55, "monitoring_stack", 30);
    if (containsAny(blob, list(JUMPER, "HEALTH_MARKERS"))) {
      return cap(-40, "health_check", 25);
    }
    if (container && deploy) {
      return result(-25, "container_deploy_context");
    }
    return result(0, null);
  }

  /** 下载、网关、LOLBin，以及名单里的永不白名单进程。 */
  private static boolean neverWindows(String proc) {
    return has(WINDOWS, "NEVER_WHITELIST_BASE", base(proc))
        || memberBase(WINDOWS, "NETWORK_FETCH_PROCS", proc)
        || memberBase(WINDOWS, "RARE_GATEWAY_PROCS", proc)
        || memberBase(WINDOWS, "LOL_BIN_PROCS", proc);
  }

  /** Git 自带 curl 访问内网主机，不按外部下载拦截。 */
  private static boolean internalFetch(String blob) {
    return re("\\b(curl|wget|bitsadmin)(?:\\.exe)?\\b", blob)
        && containsAny(blob, J8.list("\\git\\", "mingw64\\bin\\curl", "programs\\git\\"))
        && containsAny(blob, list(WINDOWS, "INTERNAL_FETCH_HOST_MARKERS"));
  }

  /**
   * 攻击形状。返回非 null 时白名单不降分。Git 内网 curl 可以穿过“永不白名单”，其余拦截仍生效。
   */
  private static String blockReason(
      String proc,
      String procBase,
      String blob,
      String lower,
      Map<String, Integer> templates,
      int fleetHosts) {
    if (neverWindows(proc) && !(procBase.equals("curl") && internalFetch(lower)))
      return "never_whitelist_proc:" + procBase;
    if (containsAny(lower, list(WINDOWS, "ATTACK_TEST_CMD_MARKERS")))
      return "attack_test_or_lolbin_pattern";
    if (re("(?<![/\\\\-])\\bping(?:\\.exe)?\\b", blob) || procBase.equals("ping"))
      return "ping_command_or_proc";
    if (re("\\b(curl|wget|bitsadmin)(?:\\.exe)?\\b", blob) && !internalFetch(lower))
      return "fetch_command_in_chain";
    if (re(DOT, blob)) {
      return "dot_segment_url";
    }
    if (re("https?://[^\\s\"'<>]+", blob)
        && J8.set("cmd", "powershell", "pwsh", "mshta", "rundll32").contains(procBase))
      return "url_in_interpreter_cmdline";
    if (proxy(blob, true)) {
      return "proxy_fetch_flag";
    }
    if (re(ENC, blob) && !containsAny(lower, list(WINDOWS, "DEV_ENCODED_PS_MARKERS"))) {
      return "encoded_powershell";
    }
    if (templates.keySet().stream().anyMatch(template -> suspicious(template, true)))
      return "suspicious_fetch_template";
    if (J8.set("cmd", "powershell", "pwsh").contains(procBase)
        && re(TEMP, blob)
        && !collection(blob)
        && !re("keepassxc-proxy\\.exe", blob)) return "temp_path_interpreter";
    if (memberBase(WINDOWS, "ENTERPRISE_AGENT_PROCS", proc) && !agent(blob, proc)) {
      // 标准代理路径已经命中、只是主机数不少于 50 时，不再改判成临时目录拉起。
      if (re(AGENT, blob) && fleetHosts < 50) {
        return "nonstandard_agent_path_low_fleet";
      } else if (re(TEMP, blob) && !collection(blob)) {
        return "agent_temp_spawn";
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public static Result windows(
      String proc,
      List<Event> samples,
      Map<String, Integer> templates,
      Map<String, Integer> chains,
      int fleetHosts) {
    String procBase = base(proc);
    // 拦截阶段把命令和模板合在一起；标准代理判定只看原始命令，不看模板。
    String commandBlob = chainBlob(samples);
    String blob = commandBlob + " " + String.join(" ", templates.keySet());
    String lower = blob.toLowerCase(Locale.ROOT);
    String blocked = blockReason(proc, procBase, blob, lower, templates, fleetHosts);
    if (blocked != null) {
      return new Result(0, null, null, blocked);
    }

    List<Result> checks = new ArrayList<>();
    if (agent(commandBlob, proc)) checks.add(result(-38, "imonitor_agent_syscon"));
    if (procBase.equals("cmd") && collection(commandBlob))
      checks.add(result(-42, "imonitor_collection_script"));
    if (memberBase(WINDOWS, "ENTERPRISE_AGENT_PROCS", proc) && fleetHosts >= 50)
      checks.add(result(-24, "enterprise_security_agent_fleet_wide"));
    if (re("intellij-git-askpass|git-askpass|credential\\.helper", blob))
      checks.add(result(-18, "developer_git_askpass"));
    else if (procBase.equals("git")
        && templates.keySet().stream()
            .anyMatch(
                template ->
                    template.toLowerCase(Locale.ROOT).contains("git.exe")
                        && containsAny(
                            template.toLowerCase(Locale.ROOT),
                            J8.list("fetch", "ls-remote", "rev-list", "remote-http"))))
      checks.add(result(-18, "developer_git_operation"));

    List<String> paths = windowsPaths(samples);
    if (has(WINDOWS, "WINDOWS_BUILTIN_BASE", procBase)
        && !paths.isEmpty()
        && paths.stream().allMatch(path -> re("[/\\\\]windows[/\\\\](system32|syswow64)[/\\\\]", path)))
      checks.add(result(-28, "windows_builtin_system32:" + procBase));
    addFasooHit(checks, procBase, lower);
    addFirstVendorHit(checks, proc, procBase, paths, commandBlob);
    if (!neverWindows(proc) && rollout(templates, chains))
      checks.add(result(-18, "uniform_parent_many_oneoff_templates"));
    return checks.stream()
        .min(
            new Comparator<Result>() {
              @Override
              public int compare(Result left, Result right) {
                return Double.compare(left.adjustment(), right.adjustment());
              }
            })
        .orElse(result(0, null));
  }

  private static List<String> windowsPaths(List<Event> samples) {
    List<String> paths = new ArrayList<>();
    for (Event event : samples) {
      if (!event.s("proc_path").isEmpty())
        paths.add(event.s("proc_path").toLowerCase(Locale.ROOT).replace('/', '\\'));
      for (String token : event.s("cmdline").split("\\s+")) {
        String cleaned = token.replaceAll("^[\"']+|[\"']+$", "");
        if (cleaned.contains(":") && cleaned.contains("\\"))
          paths.add(cleaned.toLowerCase(Locale.ROOT).replace('/', '\\'));
      }
    }
    return paths;
  }

  /** 第一条标记就是 Run 键路径，调用前已经单独判断；这里只匹配其余厂商特征。 */
  private static void addFasooHit(List<Result> checks, String procBase, String lower) {
    List<String> markers = list(WINDOWS, "FASOO_REG_CMD_MARKERS");
    if (procBase.equals("reg")
        && lower.contains("currentversion\\run")
        && containsAny(lower, markers.subList(1, markers.size())))
      checks.add(result(-24, "assets_fasoo_reg_run_key"));
    else if (lower.contains("aliaqsinstall") || lower.contains("aegis.aliyun"))
      checks.add(result(-24, "assets_fasoo_aegis_installer"));
  }

  /** VENDOR_RULES 之后再追加 ASSETS_VENDOR_SPECS。进程名和标记都命中时只取第一条。 */
  @SuppressWarnings("unchecked")
  private static void addFirstVendorHit(
      List<Result> checks, String proc, String procBase, List<String> paths, String commandBlob) {
    List<Map<String, Object>> vendors =
        new ArrayList<Map<String, Object>>((List<Map<String, Object>>) data(WINDOWS).get("VENDOR_RULES"));
    for (List<Object> spec : (List<List<Object>>) data(WINDOWS).get("ASSETS_VENDOR_SPECS"))
      vendors.add(Json.obj("bases", spec.get(0), "markers", spec.get(1), "id", spec.get(2)));
    String procLower = proc.toLowerCase(Locale.ROOT).replace(".exe", "");
    String haystack = String.join(" ", paths) + " " + commandBlob.toLowerCase(Locale.ROOT);
    for (Map<String, Object> vendor : vendors) {
      boolean nameHit =
          ((List<String>) vendor.get("bases"))
              .stream()
              .anyMatch(
                  baseName ->
                      procBase.equals(baseName) || procLower.equals(baseName) || procLower.startsWith(baseName));
      if (nameHit && containsAny(haystack, (List<String>) vendor.get("markers"))) {
        checks.add(result(-22, vendor.get("id").toString()));
        return;
      }
    }
  }
}
