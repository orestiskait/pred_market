"""CLI for ASOS vs CLI high plateau analysis.

Compares Synoptic ASOS 1-minute temperatures with NWS CLI daily high. Analyzes
how often the "stability plateau" max (peak of N consecutive same-integer-round
ASOS observations) matches the official NWS CLI high, compared to the raw ASOS max.

Usage:
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW --start 2026-02-08 --end 2026-02-20
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW --min-consecutive 3
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW --export asos_cli_report.csv
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from services.core.config import load_config, configure_logging
from services.backtest.asos_cli_plateau_analyzer import AsosCliPlateauAnalyzer
from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY


def main():
    parser = argparse.ArgumentParser(
        description="ASOS vs CLI high: compare Synoptic ASOS 1-min plateau temps with NWS CLI daily high.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument(
        "--station", default="KMDW",
        help="ICAO station ID (e.g. KMDW, KNYC). Default: KMDW",
    )
    parser.add_argument(
        "--tz", default=None,
        help="IANA timezone (auto-detected from station registry if omitted)",
    )
    parser.add_argument(
        "--lat", type=float, default=None,
        help="Station latitude (auto from registry). Use for LST hemisphere: lat<0 → Jul 15, else Jan 15.",
    )
    parser.add_argument(
        "--asos-source", choices=["synoptic", "iem", "both"], default="synoptic",
        help="ASOS 1-min source: synoptic, iem, or both (same-day comparison vs CLI)",
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--min-consecutive", type=int, default=2,
        help="Minimum consecutive same-round observations for a plateau (default: 2)",
    )
    parser.add_argument("--export", default=None, help="Export results to CSV")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

    # Auto-detect timezone and latitude from station registry
    tz_name = args.tz
    lat = args.lat
    if lat is None:
        for mc in KALSHI_MARKET_REGISTRY.values():
            if mc.icao == args.station:
                if tz_name is None:
                    tz_name = mc.tz
                lat = mc.lat
                break
    if tz_name is None:
        tz_name = "America/Chicago"
        logging.warning(
            "Station %s not found in registry, using default tz %s",
            args.station, tz_name,
        )

    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None

    if args.asos_source == "both":
        # Same-day comparison: run both IEM and Synoptic on overlap dates
        overlap, n_iem, n_synoptic = AsosCliPlateauAnalyzer.overlap_dates(
            data_dir, args.station
        )
        if start_date:
            overlap = [d for d in overlap if d >= start_date]
        if end_date:
            overlap = [d for d in overlap if d <= end_date]
        if not overlap:
            print("No overlap: no days with both IEM and Synoptic ASOS data (and CLI).")
            return

        analyzer_iem = AsosCliPlateauAnalyzer(
            data_dir=str(data_dir),
            station=args.station,
            tz_name=tz_name,
            min_consecutive=args.min_consecutive,
            lat=lat,
            asos_source="iem",
        )
        analyzer_synoptic = AsosCliPlateauAnalyzer(
            data_dir=str(data_dir),
            station=args.station,
            tz_name=tz_name,
            min_consecutive=args.min_consecutive,
            lat=lat,
            asos_source="synoptic",
        )
        report_iem = analyzer_iem.run_with_dates(overlap)
        report_synoptic = analyzer_synoptic.run_with_dates(overlap)

        n = len(overlap)
        print(f"\n{'=' * 120}")
        print(f"IEM vs CLI / Synoptic vs CLI — same {n} days (overlap)")
        print(f"{'=' * 120}")
        print(f"IEM days (vs CLI):     {n_iem}")
        print(f"Synoptic days (vs CLI): {n_synoptic}")
        print(f"Overlap (both + CLI):  {n}")
        print()
        print("IEM vs CLI:")
        print(f"  Raw max == CLI:     {report_iem.raw_match_rate * 100:.0f}% ({sum(1 for d in report_iem.days if d.raw_matches_cli)}/{n})")
        print(f"  2-min avg max == CLI: {report_iem.avg2_match_rate * 100:.0f}% ({sum(1 for d in report_iem.days if d.avg2_matches_cli)}/{n})")
        print(f"  5-min avg max == CLI: {report_iem.avg5_match_rate * 100:.0f}% ({sum(1 for d in report_iem.days if d.avg5_matches_cli)}/{n})")
        print(f"  Stable max == CLI:  {report_iem.stable_match_rate * 100:.0f}% ({sum(1 for d in report_iem.days if d.stable_matches_cli)}/{n})")
        print()
        print("Synoptic vs CLI:")
        print(f"  Raw max == CLI:     {report_synoptic.raw_match_rate * 100:.0f}% ({sum(1 for d in report_synoptic.days if d.raw_matches_cli)}/{n})")
        print(f"  2-min avg max == CLI: {report_synoptic.avg2_match_rate * 100:.0f}% ({sum(1 for d in report_synoptic.days if d.avg2_matches_cli)}/{n})")
        print(f"  5-min avg max == CLI: {report_synoptic.avg5_match_rate * 100:.0f}% ({sum(1 for d in report_synoptic.days if d.avg5_matches_cli)}/{n})")
        print(f"  Stable max == CLI:  {report_synoptic.stable_match_rate * 100:.0f}% ({sum(1 for d in report_synoptic.days if d.stable_matches_cli)}/{n})")
        print()
        n_metar = sum(1 for d in report_iem.days if d.metar_matches_cli is not None)
        print("METAR/SPECI vs CLI:")
        print(f"  METAR max == CLI:   {report_iem.metar_match_rate * 100:.0f}% ({sum(1 for d in report_iem.days if d.metar_matches_cli)}/{n_metar})")
        print(f"{'=' * 120}")

        # Detailed per-day table: IEM vs Synoptic vs CLI
        print(f"\n{'=' * 120}")
        print("DETAILED PER-DAY TABLE — IEM vs Synoptic vs CLI (match ✓ / mismatch ✗)")
        print(f"{'=' * 120}")
        hdr = (
            f"{'Date':<12} | {'CLI':>4} | "
            f"{'IEM raw':>8} {'IEM✓/✗':>6} | {'IEM avg2':>8} {'IEM✓/✗':>6} | {'IEM avg5':>8} {'IEM✓/✗':>6} | {'IEM stable':>10} {'IEM✓/✗':>6} | "
            f"{'Syn raw':>8} {'Syn✓/✗':>6} | {'Syn avg2':>8} {'Syn✓/✗':>6} | {'Syn avg5':>8} {'Syn✓/✗':>6} | {'Syn stable':>10} {'Syn✓/✗':>6} | "
            f"{'METAR':>6} {'METAR✓/✗':>8} | {'IEM=Syn raw':>12} {'IEM=Syn stable':>14}"
        )
        print(hdr)
        print("-" * 120)
        for i, d in enumerate(overlap):
            di = report_iem.days[i]
            ds = report_synoptic.days[i]
            cli = di.cli_high_f or ds.cli_high_f
            cli_s = f"{cli}" if cli is not None else "N/A"
            # IEM
            iem_raw = round(di.asos_raw_max) if di.n_obs > 0 else None
            iem_raw_s = f"{iem_raw}" if iem_raw is not None else "—"
            iem_raw_ok = "✓" if di.raw_matches_cli else ("✗" if di.raw_matches_cli is False else "?")
            iem_avg2 = round(di.avg2_max) if di.avg2_max is not None else None
            iem_avg2_s = f"{iem_avg2}" if iem_avg2 is not None else "—"
            iem_avg2_ok = "✓" if di.avg2_matches_cli else ("✗" if di.avg2_matches_cli is False else "?")
            iem_avg5 = round(di.avg5_max) if di.avg5_max is not None else None
            iem_avg5_s = f"{iem_avg5}" if iem_avg5 is not None else "—"
            iem_avg5_ok = "✓" if di.avg5_matches_cli else ("✗" if di.avg5_matches_cli is False else "?")
            iem_stable = di.stable_max_rounded
            iem_stable_s = f"{iem_stable}" if iem_stable is not None else "—"
            iem_stable_ok = "✓" if di.stable_matches_cli else ("✗" if di.stable_matches_cli is False else "?")
            # Synoptic
            syn_raw = round(ds.asos_raw_max) if ds.n_obs > 0 else None
            syn_raw_s = f"{syn_raw}" if syn_raw is not None else "—"
            syn_raw_ok = "✓" if ds.raw_matches_cli else ("✗" if ds.raw_matches_cli is False else "?")
            syn_avg2 = round(ds.avg2_max) if ds.avg2_max is not None else None
            syn_avg2_s = f"{syn_avg2}" if syn_avg2 is not None else "—"
            syn_avg2_ok = "✓" if ds.avg2_matches_cli else ("✗" if ds.avg2_matches_cli is False else "?")
            syn_avg5 = round(ds.avg5_max) if ds.avg5_max is not None else None
            syn_avg5_s = f"{syn_avg5}" if syn_avg5 is not None else "—"
            syn_avg5_ok = "✓" if ds.avg5_matches_cli else ("✗" if ds.avg5_matches_cli is False else "?")
            syn_stable = ds.stable_max_rounded
            syn_stable_s = f"{syn_stable}" if syn_stable is not None else "—"
            syn_stable_ok = "✓" if ds.stable_matches_cli else ("✗" if ds.stable_matches_cli is False else "?")
            # METAR (same for both IEM and Synoptic days)
            metar_val = round(di.metar_raw_max) if di.metar_raw_max is not None else None
            metar_s = f"{metar_val}" if metar_val is not None else "—"
            metar_ok = "✓" if di.metar_matches_cli else ("✗" if di.metar_matches_cli is False else "?")
            # IEM vs Synoptic agreement
            iem_eq_syn_raw = "✓ match" if (iem_raw is not None and syn_raw is not None and iem_raw == syn_raw) else ("✗ diff" if (iem_raw is not None and syn_raw is not None) else "—")
            iem_eq_syn_stable = "✓ match" if (iem_stable is not None and syn_stable is not None and iem_stable == syn_stable) else ("✗ diff" if (iem_stable is not None and syn_stable is not None) else "—")
            print(
                f"{d!s:<12} | {cli_s:>4} | "
                f"{iem_raw_s:>8} {iem_raw_ok:>6} | {iem_avg2_s:>8} {iem_avg2_ok:>6} | {iem_avg5_s:>8} {iem_avg5_ok:>6} | {iem_stable_s:>10} {iem_stable_ok:>6} | "
                f"{syn_raw_s:>8} {syn_raw_ok:>6} | {syn_avg2_s:>8} {syn_avg2_ok:>6} | {syn_avg5_s:>8} {syn_avg5_ok:>6} | {syn_stable_s:>10} {syn_stable_ok:>6} | "
                f"{metar_s:>6} {metar_ok:>8} | {iem_eq_syn_raw:>12} {iem_eq_syn_stable:>14}"
            )
        print(f"{'=' * 120}")

        if args.export:
            df_iem = report_iem.to_dataframe()
            df_iem["asos_source"] = "iem"
            df_syn = report_synoptic.to_dataframe()
            df_syn["asos_source"] = "synoptic"
            import pandas as pd
            pd.concat([df_iem, df_syn], ignore_index=True).to_csv(args.export, index=False)
            print(f"\nExported to {args.export}")
    else:
        analyzer = AsosCliPlateauAnalyzer(
            data_dir=str(data_dir),
            station=args.station,
            tz_name=tz_name,
            min_consecutive=args.min_consecutive,
            lat=lat,
            asos_source=args.asos_source,
        )
        report = analyzer.run(start_date=start_date, end_date=end_date)
        report.log_summary()
        report.print_table()

        if args.export:
            df = report.to_dataframe()
            df.to_csv(args.export, index=False)
            print(f"\nExported to {args.export}")


if __name__ == "__main__":
    main()
