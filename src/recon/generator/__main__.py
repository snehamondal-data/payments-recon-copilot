"""python -m recon.generator [--seed 42] [--as-of 2026-10-15] [--no-publish]"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date

from recon.config import load_settings
from recon.fx import write_fx_csv
from recon.generator import DEFAULT_TRANSACTIONS, build_scenario
from recon.generator.ground_truth import write_ground_truth
from recon.generator.publish import publish


def main(argv: list[str] | None = None) -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Generate the synthetic Tallyline scenario.")
    parser.add_argument("--seed", type=int, default=settings.seed)
    parser.add_argument("--as-of", type=date.fromisoformat, default=settings.as_of)
    parser.add_argument("--transactions", type=int, default=DEFAULT_TRANSACTIONS)
    parser.add_argument("--no-publish", action="store_true", help="write reference and ground-truth files only")
    args = parser.parse_args(argv)

    scenario = build_scenario(args.seed, args.as_of, args.transactions)
    write_fx_csv(settings.fx_rates_path, scenario.fx_rates)
    write_ground_truth(settings.ground_truth_path, scenario)

    by_topic = Counter(m.topic for m in scenario.messages)
    print(f"seed={scenario.seed} as_of={scenario.as_of} sha256={scenario.content_hash()[:12]}")
    print(f"messages: {dict(by_topic)} (incl. {len(scenario.poison)} poison, {scenario.redelivered} redelivered)")
    print(f"planted cases: {len(scenario.cases)} ({sum(c.held_out for c in scenario.cases)} held out)")
    print(f"wrote {settings.fx_rates_path} and {settings.ground_truth_path}")
    if not args.no_publish:
        sent = publish(scenario, settings.kafka_bootstrap, settings.topic_prefix)
        print(f"published {sent} messages to {settings.kafka_bootstrap}")


if __name__ == "__main__":
    main()
