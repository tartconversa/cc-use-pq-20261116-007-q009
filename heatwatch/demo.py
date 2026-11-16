from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from .service import HeatService


def run(fixture_path: str) -> dict:
    fixture = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory() as temporary:
        service = HeatService(str(Path(temporary) / "heatwatch.sqlite3"))
        try:
            network = service.configure_network(fixture["stations"], fixture["pipes"])
            rules = service.configure_rules(fixture["rules"])
            quality = service.configure_quality(fixture.get("quality", {}))
            batches = []
            for batch in fixture["batches"]:
                batches.append(service.ingestion.ingest(batch["batch_id"],
                                                        batch["observations"],
                                                        batch["received_at"]))
            report = service.reporting.window(fixture["from_at"], fixture["to_at"])
            reconciliation = service.reporting.reconcile()
            audit = service.operations.audit()
            capacity = service.plan_capacity(fixture["demands"])
            if not reconciliation["ok"] or not audit["ok"]:
                raise RuntimeError("demonstration ledger failed consistency checks")
            return {"network": network, "rules": rules, "quality": quality,
                    "batches": batches, "report": report,
                    "reconciliation": reconciliation, "audit": audit,
                    "capacity": capacity}
        finally:
            service.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local heat network demonstration")
    parser.add_argument("--fixture", required=True, help="JSON input fixture")
    args = parser.parse_args()
    print(json.dumps(run(args.fixture), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
