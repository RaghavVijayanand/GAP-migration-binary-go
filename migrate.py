#!/usr/bin/env python3
"""
migrate.py — Full GAP → HyperDX / ClickHouse migration orchestrator.

Uses the `gap` binary (built from this directory) to run the migration
pipeline step by step.

Configuration priority (highest → lowest):
  1. CLI flags
  2. .env file  (looked up next to this script, or --env-file <path>)
  3. JSON config file (--config <path>)

Usage:
    python migrate.py                          # uses .env in same folder
    python migrate.py --env-file prod.env
    python migrate.py --config config.json
    python migrate.py --grafana-url http://grafana:3000 --grafana-api-key glsa_...
    python migrate.py --help
"""

import argparse
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any


# ─────────────────────────────────────────────
# Colours / formatting helpers
# ─────────────────────────────────────────────

USE_COLOR = sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text

def green(t):  return _c("32;1", t)
def yellow(t): return _c("33;1", t)
def red(t):    return _c("31;1", t)
def cyan(t):   return _c("36;1", t)
def bold(t):   return _c("1",    t)

def step(n: int, total: int, label: str):
    print(f"\n{cyan(f'[{n}/{total}]')} {bold(label)}", flush=True)

def ok(msg: str):   print(f"  {green('✓')} {msg}", flush=True)
def warn(msg: str): print(f"  {yellow('⚠')} {msg}", flush=True)
def fail(msg: str): print(f"  {red('✗')} {msg}", flush=True)
def info(msg: str): print(f"  {msg}", flush=True)


# ─────────────────────────────────────────────
# .env file loader (no external deps)
# ─────────────────────────────────────────────

def load_dotenv(path: Path) -> dict[str, str]:
    """
    Parse a .env file and return a dict of key → value.

    Supported syntax:
      KEY=value
      KEY="quoted value"
      KEY='single quoted'
      # comment lines
      export KEY=value
    """
    env: dict[str, str] = {}
    if not path.exists():
        return env

    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # strip optional surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            env[key] = value

    return env


# Mapping from .env variable names → internal config keys
ENV_VAR_MAP = {
    "GAP_GRAFANA_URL":          "grafana_url",
    "GAP_GRAFANA_API_KEY":      "grafana_api_key",
    "GAP_PROMETHEUS_URL":       "prometheus_url",
    "GAP_ALERTMANAGER_URL":     "alertmanager_url",
    "GAP_HYPERDX_URL":          "hyperdx_url",
    "GAP_HYPERDX_API_KEY":      "hyperdx_api_key",
    "GAP_METRIC_SOURCE_ID":     "metric_source_id",
    "GAP_WEBHOOK_ID":           "webhook_id",
    "GAP_CLICKHOUSE_HOST":      "clickhouse_host",
    "GAP_CLICKHOUSE_PORT":      "clickhouse_port",
    "GAP_CLICKHOUSE_DATABASE":  "clickhouse_database",
    "GAP_CLICKHOUSE_USERNAME":  "clickhouse_username",
    "GAP_CLICKHOUSE_PASSWORD":  "clickhouse_password",
    "GAP_LOOKBACK_DAYS":        "lookback_days",
    "GAP_STEP_SECONDS":         "step_seconds",
    "GAP_BATCH_SIZE":           "batch_size",
    "GAP_METRIC_FILTER":        "metric_filter",
    "GAP_SERVICE_NAME":         "service_name",
}

# Fields that should be coerced to int
INT_FIELDS = {"clickhouse_port", "lookback_days", "step_seconds", "batch_size"}


# ─────────────────────────────────────────────
# Gap binary runner
# ─────────────────────────────────────────────

def find_gap_binary() -> Path:
    here = Path(__file__).parent
    for name in ("gap.exe", "gap"):
        candidate = here / name
        if candidate.exists():
            return candidate
    sys.exit(
        red("ERROR: gap binary not found next to migrate.py.\n")
        + "       Build it first:  go build -o gap ./cmd/gap"
    )


def run_gap(binary: Path, subcommand: str, payload: dict[str, Any]) -> dict[str, Any]:
    input_bytes = json.dumps(payload).encode()
    env = {**os.environ, "GAPTOOL_INSECURE_SKIP_VERIFY": os.environ.get("GAPTOOL_INSECURE_SKIP_VERIFY", "")}

    result = subprocess.run(
        [str(binary), subcommand],
        input=input_bytes,
        stdout=subprocess.PIPE,
        env=env,
    )

    raw_stdout = result.stdout.strip()
    try:
        response = json.loads(raw_stdout)
    except json.JSONDecodeError:
        fail(f"gap {subcommand} returned non-JSON output:\n{raw_stdout.decode(errors='replace')}")
        sys.exit(1)

    if not response.get("status"):
        fail(f"gap {subcommand} failed: {response.get('message', '(no message)')}")
        sys.exit(1)

    return response


# ─────────────────────────────────────────────
# Pipeline steps
# ─────────────────────────────────────────────

def do_fetch(binary: Path, cfg: dict) -> dict:
    resp = run_gap(binary, "fetch", {
        "grafana_url":      cfg["grafana_url"],
        "grafana_api_key":  cfg["grafana_api_key"],
        "prometheus_url":   cfg["prometheus_url"],
        "alertmanager_url": cfg.get("alertmanager_url", ""),
    })
    gap_data = resp["data"]
    grafana_count = len(gap_data.get("grafana", {}).get("dashboards", []))
    rule_groups   = gap_data.get("prometheus", {}).get("rules", {}).get("data", {}).get("groups", [])
    alert_count   = sum(1 for g in rule_groups for r in g.get("rules", []) if str(r.get("type", "")).lower() == "alerting")
    ok(f"Fetched {grafana_count} Grafana dashboard(s), {alert_count} alerting rule(s)")
    return gap_data


def do_convert_grafana(binary: Path, gap_data: dict, cfg: dict) -> list[dict]:
    resp = run_gap(binary, "convert-grafana", {
        "gap_data":                gap_data,
        "hyperdx_metric_source_id": cfg["metric_source_id"],
    })
    ok(f"Converted {resp['dashboard_count']} dashboard(s) to HyperDX format")
    return resp["dashboards"]


def do_apply_grafana(binary: Path, dashboards: list[dict], cfg: dict, dry_run: bool) -> int:
    resp = run_gap(binary, "apply-grafana", {
        "hyperdx_url":     cfg["hyperdx_url"],
        "hyperdx_api_key": cfg["hyperdx_api_key"],
        "dashboards":      dashboards,
        "dry_run":         dry_run,
    })
    label = "Would push" if dry_run else "Pushed"
    ok(f"{label} {resp['dashboard_count']} dashboard(s) to HyperDX")
    return resp["dashboard_count"]


def do_convert_alerts(binary: Path, gap_data: dict, cfg: dict) -> list[dict]:
    resp = run_gap(binary, "convert-alerts", {
        "gap_data":                gap_data,
        "hyperdx_metric_source_id": cfg["metric_source_id"],
        "webhook_id":              cfg.get("webhook_id", ""),
    })
    ok(f"Converted {resp['alert_count']} alert rule(s) to HyperDX format")
    return resp["alert_pairs"]


def do_apply_alerts(binary: Path, alert_pairs: list[dict], cfg: dict, dry_run: bool) -> int:
    resp = run_gap(binary, "apply-alerts", {
        "hyperdx_url":     cfg["hyperdx_url"],
        "hyperdx_api_key": cfg["hyperdx_api_key"],
        "alert_pairs":     alert_pairs,
        "dry_run":         dry_run,
    })
    label = "Would push" if dry_run else "Pushed"
    ok(f"{label} {resp['alert_count']} alert rule(s) to HyperDX")
    return resp["alert_count"]


def do_backfill(binary: Path, cfg: dict, dry_run: bool):
    resp = run_gap(binary, "backfill", {
        "prometheus_url":      cfg["prometheus_url"],
        "clickhouse_host":     cfg.get("clickhouse_host", "localhost"),
        "clickhouse_port":     int(cfg.get("clickhouse_port", 8123)),
        "clickhouse_database": cfg.get("clickhouse_database", "default"),
        "clickhouse_username": cfg.get("clickhouse_username", "default"),
        "clickhouse_password": cfg.get("clickhouse_password", ""),
        "lookback_days":       int(cfg.get("lookback_days", 3)),
        "step_seconds":        int(cfg.get("step_seconds", 60)),
        "batch_size":          int(cfg.get("batch_size", 10000)),
        "metric_filter":       cfg.get("metric_filter", ""),
        "service_name":        cfg.get("service_name", "prometheus-migrated"),
        "dry_run":             dry_run,
    })
    stats = resp.get("stats", {})
    label = "dry run — would insert" if dry_run else "migrated"
    ok(f"Backfill {label} {stats.get('metrics_migrated', 0)} metric(s) "
       f"({stats.get('metrics_failed', 0)} failed, {stats.get('metrics_skipped', 0)} skipped)")
    for table, count in sorted(stats.get("rows_inserted", {}).items()):
        info(f"  {table}: {count:,} row(s)")
    for err in stats.get("errors", [])[:10]:
        warn(err)


def do_validate(binary: Path, cfg: dict):
    resp = run_gap(binary, "validate", {
        "grafana_url":      cfg["grafana_url"],
        "grafana_api_key":  cfg["grafana_api_key"],
        "prometheus_url":   cfg["prometheus_url"],
        "alertmanager_url": cfg.get("alertmanager_url", ""),
        "hyperdx_url":      cfg["hyperdx_url"],
        "hyperdx_api_key":  cfg["hyperdx_api_key"],
    })
    source_dash, target_dash  = resp.get("source_dashboards", {}), resp.get("target_dashboards", {})
    source_alerts, target_alerts = resp.get("source_alerts", {}), resp.get("target_alerts", {})

    migrated_dash  = [k for k in source_dash if k in target_dash]
    missing_dash   = [k for k in source_dash if k not in target_dash]
    migrated_alert = [k for k in source_alerts if k in target_alerts]
    missing_alert  = [k for k in source_alerts if k not in target_alerts]

    info(f"Dashboards : {len(migrated_dash)}/{len(source_dash)} migrated")
    if missing_dash:
        warn("Missing: " + ", ".join(missing_dash[:5]) + (f" +{len(missing_dash)-5} more" if len(missing_dash) > 5 else ""))

    info(f"Alerts     : {len(migrated_alert)}/{len(source_alerts)} migrated")
    if missing_alert:
        warn("Missing: " + ", ".join(missing_alert[:5]) + (f" +{len(missing_alert)-5} more" if len(missing_alert) > 5 else ""))

    if not missing_dash and not missing_alert:
        ok("All source resources found in HyperDX")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=textwrap.dedent("""\
            Migrate Grafana / Prometheus / Alertmanager (GAP) → HyperDX / ClickHouse.

            Configuration priority (highest to lowest):
              1. CLI flags
              2. .env file  (default: .env next to this script, or --env-file)
              3. JSON config file (--config)
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("--env-file", metavar="FILE",
                   help="Path to .env file (default: .env next to this script)")
    p.add_argument("--config",   metavar="FILE",
                   help="JSON config file (lowest priority)")

    g = p.add_argument_group("Source (GAP stack)")
    g.add_argument("--grafana-url")
    g.add_argument("--grafana-api-key")
    g.add_argument("--prometheus-url")
    g.add_argument("--alertmanager-url", default="")

    h = p.add_argument_group("Target (HyperDX)")
    h.add_argument("--hyperdx-url")
    h.add_argument("--hyperdx-api-key")
    h.add_argument("--metric-source-id",
                   help="HyperDX metric source ID (for dashboard/alert series)")
    h.add_argument("--webhook-id", default="",
                   help="HyperDX webhook ID for alert notifications")

    c = p.add_argument_group("ClickHouse (backfill, optional)")
    c.add_argument("--clickhouse-host",     default=None)
    c.add_argument("--clickhouse-port",     type=int, default=None)
    c.add_argument("--clickhouse-database", default=None)
    c.add_argument("--clickhouse-username", default=None)
    c.add_argument("--clickhouse-password", default=None)
    c.add_argument("--lookback-days",       type=int, default=None)
    c.add_argument("--step-seconds",        type=int, default=None)
    c.add_argument("--batch-size",          type=int, default=None)
    c.add_argument("--metric-filter",       default=None)
    c.add_argument("--service-name",        default=None)

    ctl = p.add_argument_group("Pipeline control")
    ctl.add_argument("--dry-run",       action="store_true")
    ctl.add_argument("--skip-grafana",  action="store_true")
    ctl.add_argument("--skip-alerts",   action="store_true")
    ctl.add_argument("--backfill",      action="store_true",
                     help="Enable historical Prometheus → ClickHouse backfill")
    ctl.add_argument("--skip-validate", action="store_true")

    return p


def build_config(args: argparse.Namespace) -> dict:
    """Merge configs: JSON file (lowest) → .env → CLI flags (highest)."""

    # 1. JSON config file (base layer)
    cfg: dict = {}
    if args.config:
        path = Path(args.config)
        if not path.exists():
            sys.exit(red(f"ERROR: config file not found: {path}"))
        with open(path) as f:
            cfg = json.load(f)

    # 2. .env file
    env_file = Path(args.env_file) if args.env_file else Path(__file__).parent / ".env"
    dotenv = load_dotenv(env_file)
    if dotenv:
        info(f"Loaded {len(dotenv)} variable(s) from {env_file}")
    for env_key, cfg_key in ENV_VAR_MAP.items():
        if env_key in dotenv:
            value = dotenv[env_key]
            cfg[cfg_key] = int(value) if cfg_key in INT_FIELDS and value.isdigit() else value

    # 3. CLI flags (only override when explicitly set)
    flag_map = {
        "grafana_url":        args.grafana_url,
        "grafana_api_key":    args.grafana_api_key,
        "prometheus_url":     args.prometheus_url,
        "alertmanager_url":   args.alertmanager_url or None,
        "hyperdx_url":        args.hyperdx_url,
        "hyperdx_api_key":    args.hyperdx_api_key,
        "metric_source_id":   args.metric_source_id,
        "webhook_id":         args.webhook_id or None,
        "clickhouse_host":    args.clickhouse_host,
        "clickhouse_port":    args.clickhouse_port,
        "clickhouse_database":args.clickhouse_database,
        "clickhouse_username":args.clickhouse_username,
        "clickhouse_password":args.clickhouse_password,
        "lookback_days":      args.lookback_days,
        "step_seconds":       args.step_seconds,
        "batch_size":         args.batch_size,
        "metric_filter":      args.metric_filter,
        "service_name":       args.service_name,
    }
    for key, value in flag_map.items():
        if value is not None:
            cfg[key] = value

    return cfg


def require(cfg: dict, keys: list[str]):
    missing = [k for k in keys if not cfg.get(k)]
    if missing:
        sys.exit(
            red("ERROR: Missing required configuration:\n")
            + "\n".join(f"  {k.upper()} (.env)  or  --{k.replace('_','-')} (flag)" for k in missing)
        )


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = build_parser()
    args   = parser.parse_args()
    cfg    = build_config(args)
    binary = find_gap_binary()

    dry_run      = args.dry_run
    do_grafana   = not args.skip_grafana
    do_alerts    = not args.skip_alerts
    run_backfill = args.backfill
    run_validate = not args.skip_validate

    steps = ["fetch"]
    if do_grafana:    steps += ["convert-grafana", "apply-grafana"]
    if do_alerts:     steps += ["convert-alerts",  "apply-alerts"]
    if run_backfill:  steps += ["backfill"]
    if run_validate:  steps += ["validate"]
    total = len(steps)

    print()
    print(bold("═" * 56))
    print(bold("  GAP → HyperDX Migration"))
    print(bold("═" * 56))
    if dry_run:
        print(yellow("  DRY RUN — nothing will be written"))
    print(f"  Binary : {binary}")
    print(f"  Source : {cfg.get('grafana_url','?')} / {cfg.get('prometheus_url','?')}")
    print(f"  Target : {cfg.get('hyperdx_url','?')}")
    print()

    n = 0

    # ── Fetch ────────────────────────────────────────────────────────────
    n += 1; step(n, total, "Fetching GAP configurations")
    require(cfg, ["grafana_url", "grafana_api_key", "prometheus_url"])
    gap_data = do_fetch(binary, cfg)

    # ── Grafana dashboards ────────────────────────────────────────────────
    if do_grafana:
        n += 1; step(n, total, "Converting Grafana dashboards")
        require(cfg, ["metric_source_id"])
        dashboards = do_convert_grafana(binary, gap_data, cfg)

        n += 1; step(n, total, "Applying Grafana dashboards" + (" (dry run)" if dry_run else ""))
        require(cfg, ["hyperdx_url", "hyperdx_api_key"])
        do_apply_grafana(binary, dashboards, cfg, dry_run)

    # ── Alert rules ───────────────────────────────────────────────────────
    if do_alerts:
        n += 1; step(n, total, "Converting Prometheus alert rules")
        require(cfg, ["metric_source_id"])
        alert_pairs = do_convert_alerts(binary, gap_data, cfg)

        n += 1; step(n, total, "Applying alert rules" + (" (dry run)" if dry_run else ""))
        require(cfg, ["hyperdx_url", "hyperdx_api_key"])
        do_apply_alerts(binary, alert_pairs, cfg, dry_run)

    # ── Historical backfill ───────────────────────────────────────────────
    if run_backfill:
        n += 1; step(n, total, "Backfilling historical data into ClickHouse" + (" (dry run)" if dry_run else ""))
        require(cfg, ["prometheus_url"])
        do_backfill(binary, cfg, dry_run)

    # ── Validate ──────────────────────────────────────────────────────────
    if run_validate and not dry_run:
        n += 1; step(n, total, "Validating migration")
        require(cfg, ["grafana_url", "grafana_api_key", "prometheus_url",
                      "hyperdx_url", "hyperdx_api_key"])
        do_validate(binary, cfg)
    elif run_validate and dry_run:
        warn("Skipping validation in dry-run mode")

    # ── Done ──────────────────────────────────────────────────────────────
    print()
    print(bold("═" * 56))
    print(green(bold(" DRY RUN complete — rerun without --dry-run to apply" if dry_run else " Migration complete!")))
    print(bold("═" * 56))
    print()


if __name__ == "__main__":
    main()
