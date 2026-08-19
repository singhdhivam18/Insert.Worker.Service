from __future__ import annotations

import argparse
import logging
import sys

from config import WorkerConfig
from worker import InvoiceInsertWorker


LOG = logging.getLogger("invoice_insert")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Invoice insert worker: "
            "RabbitMQ → Azure Blob JSON → Database"
        )
    )

    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to worker configuration JSON file",
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one extraction message and exit",
    )

    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(
            logging,
            level.upper(),
            logging.INFO,
        ),
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s "
            "%(message)s"
        ),
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        # -----------------------------------------------------
        # 1. Load configuration
        # -----------------------------------------------------

        config = WorkerConfig.from_file(
            args.config
        )

        # -----------------------------------------------------
        # 2. Validate configuration
        # -----------------------------------------------------

        config.validate()

        # -----------------------------------------------------
        # 3. Configure logging
        # -----------------------------------------------------

        configure_logging(
            config.log_level
        )

        LOG.info(
            "invoice_insert_worker_starting"
        )

        # -----------------------------------------------------
        # 4. Create worker
        # -----------------------------------------------------

        worker = InvoiceInsertWorker(
            config
        )

        # -----------------------------------------------------
        # 5. Start worker
        # -----------------------------------------------------

        if args.once:

            LOG.info(
                "invoice_insert_single_poll"
            )

            worker.poll_once()

        else:

            LOG.info(
                "invoice_insert_continuous_mode"
            )

            worker.run_forever()

        return 0

    except KeyboardInterrupt:

        LOG.info(
            "invoice_insert_worker_stopped"
        )

        return 0

    except Exception:

        logging.getLogger(
            "invoice_insert"
        ).exception(
            "invoice_insert_worker_failed"
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())