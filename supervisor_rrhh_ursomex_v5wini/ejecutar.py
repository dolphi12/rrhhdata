from __future__ import annotations

import argparse

from collector.cli.dashboard import run_dashboard


def main():
    p = argparse.ArgumentParser(description="ISAPI Collector CLI (Checador Asistencias)")
    p.add_argument("-c", "--config", default="config.json", help="Ruta al config.json")
    args = p.parse_args()
    run_dashboard(args.config)


if __name__ == "__main__":
    main()
