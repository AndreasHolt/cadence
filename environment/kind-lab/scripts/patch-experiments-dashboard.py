#!/usr/bin/env python3
"""Inject a short run-config banner into the Cadence Matching Lab Experiments dashboard."""
from __future__ import annotations

import json
import sys
from pathlib import Path

BANNER_PANEL_ID = 99


def load_dashboard_from_grafana_yaml(path: Path) -> dict:
    in_dashboard = False
    lines: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("  cadence-experiment-overview.json: |"):
            in_dashboard = True
            continue
        if in_dashboard:
            if line.startswith("  ") and not line.startswith("    ") and line.strip():
                break
            if line.startswith("    "):
                lines.append(line[4:])
    if not lines:
        raise ValueError(f"could not find cadence-experiment-overview.json in {path}")
    return json.loads("\n".join(lines))


def shift_panels_down(dashboard: dict, delta: int) -> None:
    for panel in dashboard.get("panels", []):
        grid = panel.get("gridPos") or {}
        if "y" in grid:
            grid["y"] = int(grid["y"]) + delta


def upsert_banner_panel(dashboard: dict, banner: str) -> None:
    panels = dashboard.setdefault("panels", [])
    for panel in panels:
        if panel.get("id") == BANNER_PANEL_ID:
            panel.setdefault("options", {})["mode"] = "markdown"
            panel["options"]["content"] = banner
            panel["title"] = "Active run"
            panel["type"] = "text"
            panel["gridPos"] = {"h": 3, "w": 24, "x": 0, "y": 0}
            return

    shift_panels_down(dashboard, 3)
    panels.insert(
        0,
        {
            "id": BANNER_PANEL_ID,
            "type": "text",
            "title": "Active run",
            "gridPos": {"h": 3, "w": 24, "x": 0, "y": 0},
            "options": {"mode": "markdown", "content": banner},
        },
    )


def main() -> None:
    if len(sys.argv) != 4:
        print(
            "usage: patch-experiments-dashboard.py <grafana.yaml> <banner.md> <output.json>",
            file=sys.stderr,
        )
        sys.exit(2)

    grafana_yaml = Path(sys.argv[1])
    banner = Path(sys.argv[2]).read_text(encoding="utf-8").strip()
    output = Path(sys.argv[3])

    dashboard = load_dashboard_from_grafana_yaml(grafana_yaml)
    upsert_banner_panel(dashboard, banner)
    output.write_text(json.dumps(dashboard, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
