"""Command-line interface for generating lineups from projections."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from pydfs.config_loader import MappingProfile
from pydfs.ingest import load_records_from_csv, merge_player_and_projection_files
from pydfs.optimizer import build_lineups


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate DFS lineups from projections")
    parser.add_argument("projections", type=Path, help="Path to projections CSV")
    parser.add_argument("--site", default="FD", help="Site key (e.g., FD, DK)")
    parser.add_argument("--sport", default="NFL", help="Sport key (e.g., NFL, MLB)")
    parser.add_argument("--players", type=Path, help="Optional players list CSV", default=None)
    parser.add_argument(
        "--projection-column",
        action="append",
        default=[],
        help="Mapping for projection CSV columns (e.g., name=player)",
    )
    parser.add_argument(
        "--players-column",
        action="append",
        default=[],
        help="Mapping for players CSV columns (e.g., name=First Name|Last Name)",
    )
    parser.add_argument("--load-profile", type=Path, help="Load column mapping JSON", default=None)
    parser.add_argument("--save-profile", type=Path, help="Save column mapping JSON", default=None)
    parser.add_argument("--lineups", type=int, default=20, help="Number of lineups to build")
    parser.add_argument("--output", type=Path, default=Path("lineups.csv"), help="Output CSV path")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path to write merge summary JSON",
    )
    parser.add_argument(
        "--lock",
        nargs="*",
        default=None,
        help="Player IDs to force into every lineup",
    )
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=None,
        help="Player IDs to remove from consideration",
    )
    parser.add_argument(
        "--max-repeat",
        type=int,
        default=None,
        help="Maximum number of repeating players between lineups",
    )
    parser.add_argument(
        "--max-team",
        type=int,
        default=None,
        help="Maximum players from one team",
    )
    return parser.parse_args()


def _parse_mapping(entries: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid mapping entry '{entry}', expected key=value")
        key, value = entry.split("=", 1)
        mapping[key.strip()] = value.strip()
    return mapping


def main() -> None:
    args = _parse_args()

    projection_mapping = _parse_mapping(args.projection_column)
    players_mapping = _parse_mapping(args.players_column)

    if args.load_profile:
        profile = MappingProfile.load(args.load_profile)
        players_mapping = profile.players_mapping | players_mapping
        projection_mapping = profile.projection_mapping | projection_mapping

    if args.players:
        records, report = merge_player_and_projection_files(
            players_path=args.players,
            projections_path=args.projections,
            site=args.site,
            sport=args.sport,
            players_mapping=players_mapping or None,
            projection_mapping=projection_mapping or None,
        )
        if args.save_profile:
            MappingProfile(players_mapping, projection_mapping).save(args.save_profile)
            print(f"Saved mapping profile to {args.save_profile}")
        print(
            f"Merged {report.matched_players}/{report.total_players} players with projections"
        )
        if args.report:
            report_payload = {
                "total_players": report.total_players,
                "matched_players": report.matched_players,
                "players_missing_projection": report.players_missing_projection,
                "unmatched_projection_rows": report.unmatched_projection_rows,
            }
            args.report.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
            print(f"Wrote merge report to {args.report}")

        if report.players_missing_projection:
            preview = ", ".join(report.players_missing_projection[:5])
            more = len(report.players_missing_projection) - 5
            suffix = f", +{more} more" if more > 0 else ""
            print(f"Players missing projections: {preview}{suffix}")
        if report.unmatched_projection_rows:
            preview = ", ".join(report.unmatched_projection_rows[:5])
            more = len(report.unmatched_projection_rows) - 5
            suffix = f", +{more} more" if more > 0 else ""
            print(f"Projection rows without players: {preview}{suffix}")
    else:
        records = load_records_from_csv(
            args.projections,
            site=args.site,
            sport=args.sport,
            mapping=projection_mapping or None,
        )
        if args.save_profile:
            MappingProfile(players_mapping, projection_mapping).save(args.save_profile)
            print(f"Saved mapping profile to {args.save_profile}")
    lineups = build_lineups(
        records,
        site=args.site,
        sport=args.sport,
        n_lineups=args.lineups,
        lock_player_ids=args.lock,
        exclude_player_ids=args.exclude,
        max_repeating_players=args.max_repeat,
        max_from_one_team=args.max_team,
    )

    with args.output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = [
            "lineup_id",
            "salary",
            "projection",
            "player_ids",
            "player_names",
            "teams",
            "positions",
            "ownership",
        ]
        writer.writerow(header)
        for lineup in lineups:
            player_ids = " ".join(player.player_id for player in lineup.players)
            player_names = " ".join(player.name for player in lineup.players)
            teams = " ".join(player.team for player in lineup.players)
            positions = " ".join("/".join(player.positions) for player in lineup.players)
            ownerships = " ".join(
                "-" if player.ownership is None else f"{player.ownership:.1f}"
                for player in lineup.players
            )
            writer.writerow([
                lineup.lineup_id,
                lineup.salary,
                lineup.projection,
                player_ids,
                player_names,
                teams,
                positions,
                ownerships,
            ])


if __name__ == "__main__":
    main()
