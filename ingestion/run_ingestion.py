"""
Command-line entry point for the ingestion layer.
"""

import argparse
import sys
from datetime import date

from ingestion.aemet.ingest import AemetIngestion
from ingestion.common.exceptions import IngestionError
from ingestion.common.logger import get_logger
from ingestion.esios.ingest import EsiosIngestion
from ingestion.open_meteo.ingest import OpenMeteoIngestion


logger = get_logger(__name__)


def parse_date(value: str) -> date:
    """
    Convert an ISO date string (YYYY-MM-DD) into a date object.
    """

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD."
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    """
    Build the command-line argument parser.
    """

    parser = argparse.ArgumentParser(
        description="Energy Lakehouse Platform ingestion CLI."
    )

    subparsers = parser.add_subparsers(
        dest="source",
        required=True,
    )

    # ---------------------------------------------------------
    # Open-Meteo
    # ---------------------------------------------------------

    open_meteo_parser = subparsers.add_parser(
        "open_meteo",
        help="Run Open-Meteo ingestion.",
    )

    open_meteo_parser.add_argument(
        "--mode",
        choices=("historical", "incremental"),
        required=True,
    )

    open_meteo_parser.add_argument(
        "--latitude",
        type=float,
        required=True,
    )

    open_meteo_parser.add_argument(
        "--longitude",
        type=float,
        required=True,
    )

    open_meteo_parser.add_argument(
        "--start-date",
        type=parse_date,
    )

    open_meteo_parser.add_argument(
        "--end-date",
        type=parse_date,
    )

    # ---------------------------------------------------------
    # AEMET
    # ---------------------------------------------------------

    aemet_parser = subparsers.add_parser(
        "aemet",
        help="Run AEMET ingestion.",
    )

    aemet_parser.add_argument(
        "--mode",
        choices=("historical", "incremental"),
        required=True,
    )

    aemet_parser.add_argument(
        "--station-id",
        required=True,
    )

    aemet_parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
    )

    aemet_parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
    )

    # ---------------------------------------------------------
    # ESIOS
    # ---------------------------------------------------------

    esios_parser = subparsers.add_parser(
        "esios",
        help="Run REE / ESIOS ingestion.",
    )

    esios_parser.add_argument(
        "--mode",
        choices=("historical", "incremental"),
        required=True,
    )

    esios_parser.add_argument(
        "--indicator-id",
        type=int,
        required=True,
    )

    esios_parser.add_argument(
        "--dataset",
        required=True,
    )

    esios_parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
    )

    esios_parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
    )

    esios_parser.add_argument(
        "--time-trunc",
    )

    esios_parser.add_argument(
        "--time-agg",
    )

    esios_parser.add_argument(
        "--geo-trunc",
    )

    esios_parser.add_argument(
        "--geo-agg",
    )

    esios_parser.add_argument(
        "--geo-id",
        dest="geo_ids",
        type=int,
        action="append",
    )

    return parser


def validate_date_arguments(args: argparse.Namespace) -> None:
    """
    Validate source-specific date requirements.
    """

    if args.source == "open_meteo" and args.mode == "historical":
        if args.start_date is None or args.end_date is None:
            raise ValueError(
                "Open-Meteo historical ingestion requires "
                "--start-date and --end-date."
            )


def run_open_meteo(args: argparse.Namespace) -> None:
    """Execute Open-Meteo ingestion."""

    ingestion = OpenMeteoIngestion()

    if args.mode == "historical":
        output_path = ingestion.ingest_historical(
            latitude=args.latitude,
            longitude=args.longitude,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    else:
        output_path = ingestion.ingest_current(
            latitude=args.latitude,
            longitude=args.longitude,
        )

    logger.info("Generated Bronze file: %s", output_path)


def run_aemet(args: argparse.Namespace) -> None:
    """Execute AEMET ingestion."""

    ingestion = AemetIngestion()

    if args.mode == "historical":
        output_path = ingestion.ingest_historical(
            station_id=args.station_id,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    else:
        output_path = ingestion.ingest_incremental(
            station_id=args.station_id,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    logger.info("Generated Bronze file: %s", output_path)


def run_esios(args: argparse.Namespace) -> None:
    """Execute REE / ESIOS ingestion."""

    ingestion = EsiosIngestion()

    common_arguments = {
        "indicator_id": args.indicator_id,
        "dataset": args.dataset,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "time_trunc": args.time_trunc,
        "time_agg": args.time_agg,
        "geo_ids": args.geo_ids,
        "geo_trunc": args.geo_trunc,
        "geo_agg": args.geo_agg,
    }

    if args.mode == "historical":
        output_path = ingestion.ingest_historical(
            **common_arguments
        )
    else:
        output_path = ingestion.ingest_incremental(
            **common_arguments
        )

    logger.info("Generated Bronze file: %s", output_path)


def main() -> int:
    """
    Execute the ingestion command-line interface.
    """

    parser = build_parser()
    args = parser.parse_args()

    try:
        validate_date_arguments(args)

        if args.source == "open_meteo":
            run_open_meteo(args)

        elif args.source == "aemet":
            run_aemet(args)

        elif args.source == "esios":
            run_esios(args)

        else:
            parser.error(
                f"Unsupported ingestion source: {args.source}"
            )

    except (IngestionError, ValueError) as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())