from __future__ import annotations

import json
import os

from dataclasses import dataclass
from pathlib import Path


# ============================================================
# RabbitMQ configuration
# ============================================================

@dataclass(frozen=True)
class RabbitMqConfig:
    host: str
    username: str
    password: str

    port: int = 5672
    virtual_host: str = "/"

    # Worker 3 consumes the extraction-completed messages.
    queue: str = "invoice.extraction.completed"

    # Destination queue for the insertion-completed notification.
    # Published on the default exchange, routed by queue name.
    notification_queue: str = "invoice.insert.completed"

    use_ssl: bool = False


# ============================================================
# Azure Blob configuration
# ============================================================

@dataclass(frozen=True)
class AzureBlobConfig:
    account_name: str
    account_key: str

    container: str

    endpoint_suffix: str = (
        "core.windows.net"
    )


# ============================================================
# Database configuration
# ============================================================

@dataclass(frozen=True)
class DatabaseConfig:
    """
    SQL Server database configuration.
    """

    server: str
    username: str
    password: str
    database: str
    encrypt: bool = True

    @property
    def connection_string(self) -> str:
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt={'yes' if self.encrypt else 'no'};"
            "TrustServerCertificate=yes;"
        )

# ============================================================
# Worker configuration
# ============================================================

@dataclass(frozen=True)
class WorkerConfig:
    rabbitmq: RabbitMqConfig
    azure_blob: AzureBlobConfig
    database: DatabaseConfig

    poll_interval_seconds: int = 5

    state_file: Path = Path(
        "data/invoice-insert-state.json"
    )

    log_level: str = "INFO"

    # ========================================================
    # Configuration loading
    # ========================================================

    @classmethod
    def from_file(
        cls,
        path: str | Path,
    ) -> "WorkerConfig":

        config_path = Path(path)

        if not config_path.exists():

            raise FileNotFoundError(
                "Configuration file not found: "
                f"{config_path}"
            )

        try:

            data = json.loads(
                config_path.read_text(
                    encoding="utf-8"
                )
            )

        except json.JSONDecodeError as exc:

            raise ValueError(
                "Invalid JSON configuration: "
                f"{config_path}"
            ) from exc

        if not isinstance(data, dict):

            raise ValueError(
                "Configuration root must be an object."
            )

        # -----------------------------------------------------
        # Generic value reader
        # -----------------------------------------------------

        def value(
            section: str,
            name: str,
            default=None,
        ):
            """
            Read configuration value.

            Environment variable takes precedence.

            Example:

                INVOICE_INSERT_RABBITMQ_HOST
            """

            env_name = (
                "INVOICE_INSERT_"
                f"{section.upper()}_"
                f"{name.upper()}"
            )

            if env_name in os.environ:

                return os.environ[
                    env_name
                ]

            section_data = data.get(
                section,
                {},
            )

            if not isinstance(
                section_data,
                dict,
            ):

                raise ValueError(
                    f"Configuration section "
                    f"'{section}' must be an object."
                )

            return section_data.get(
                name,
                default,
            )

        # -----------------------------------------------------
        # Boolean reader
        # -----------------------------------------------------

        def boolean(
            section: str,
            name: str,
            default: bool,
        ) -> bool:

            raw = value(
                section,
                name,
                default,
            )

            text = str(
                raw
            ).strip().lower()

            if text not in {
                "true",
                "false",
                "1",
                "0",
                "yes",
                "no",
            }:

                raise ValueError(
                    f"{section}.{name} must be a boolean."
                )

            return text in {
                "true",
                "1",
                "yes",
            }

        # -----------------------------------------------------
        # Integer reader
        # -----------------------------------------------------

        def integer(
            section: str,
            name: str,
            default: int,
        ) -> int:

            raw = value(
                section,
                name,
                default,
            )

            try:

                return int(raw)

            except (
                TypeError,
                ValueError,
            ) as exc:

                raise ValueError(
                    f"{section}.{name} "
                    f"must be an integer."
                ) from exc

        # =====================================================
        # Build configuration
        # =====================================================

        config = cls(

            # -------------------------------------------------
            # RabbitMQ
            # -------------------------------------------------

            rabbitmq=RabbitMqConfig(

                host=str(
                    value(
                        "rabbitmq",
                        "host",
                        "",
                    )
                ).strip(),

                username=str(
                    value(
                        "rabbitmq",
                        "username",
                        "",
                    )
                ).strip(),

                password=str(
                    value(
                        "rabbitmq",
                        "password",
                        "",
                    )
                ),

                port=integer(
                    "rabbitmq",
                    "port",
                    5672,
                ),

                virtual_host=str(
                    value(
                        "rabbitmq",
                        "virtual_host",
                        "/",
                    )
                ),

                queue=str(
                    value(
                        "rabbitmq",
                        "queue",
                        "invoice.extraction.completed",
                    )
                ).strip(),

                notification_queue=str(
                    value(
                        "rabbitmq",
                        "notification_queue",
                        "invoice.insert.completed",
                    )
                ).strip(),

                use_ssl=boolean(
                    "rabbitmq",
                    "use_ssl",
                    False,
                ),
            ),

            # -------------------------------------------------
            # Azure Blob
            # -------------------------------------------------

            azure_blob=AzureBlobConfig(

                account_name=str(
                    value(
                        "azure_blob",
                        "account_name",
                        "",
                    )
                ).strip(),

                account_key=str(
                    value(
                        "azure_blob",
                        "account_key",
                        "",
                    )
                ),

                container=str(
                    value(
                        "azure_blob",
                        "container",
                        "",
                    )
                ).strip(),

                endpoint_suffix=str(
                    value(
                        "azure_blob",
                        "endpoint_suffix",
                        "core.windows.net",
                    )
                ).strip(),
            ),

            # -------------------------------------------------
            # Database
            # -------------------------------------------------

                database=DatabaseConfig(
                    server=str(
                        value(
                            "database",
                            "server",
                            "",
                        )
                    ).strip(),

                    username=str(
                        value(
                            "database",
                            "username",
                            "",
                        )
                    ).strip(),

                    password=str(
                        value(
                            "database",
                            "password",
                            "",
                        )
                    ),

                    database=str(
                        value(
                            "database",
                            "database",
                            "",
                        )
                    ).strip(),

                    encrypt=boolean(
                        "database",
                        "encrypt",
                        True,
                    ),
                ),

            # -------------------------------------------------
            # Worker
            # -------------------------------------------------

            poll_interval_seconds=integer(
                "worker",
                "poll_interval_seconds",
                5,
            ),

            state_file=Path(
                value(
                    "worker",
                    "state_file",
                    "data/invoice-insert-state.json",
                )
            ),

            # -------------------------------------------------
            # Logging
            # -------------------------------------------------

            log_level=str(
                value(
                    "logging",
                    "level",
                    "INFO",
                )
            ).strip().upper(),
        )

        return config

    # =========================================================
    # Validation
    # =========================================================

    def validate(self) -> None:

        missing: list[str] = []

        required_values = [

            (
                "rabbitmq.host",
                self.rabbitmq.host,
            ),

            (
                "rabbitmq.username",
                self.rabbitmq.username,
            ),

            (
                "rabbitmq.password",
                self.rabbitmq.password,
            ),

            (
                "rabbitmq.queue",
                self.rabbitmq.queue,
            ),

            (
                "azure_blob.account_name",
                self.azure_blob.account_name,
            ),

            (
                "azure_blob.account_key",
                self.azure_blob.account_key,
            ),

            (
                "azure_blob.container",
                self.azure_blob.container,
            ),

            (
                "rabbitmq.notification_queue",
                self.rabbitmq.notification_queue,
            ),

            (
                "database.server",
                self.database.server,
            ),

            (
                "database.username",
                self.database.username,
            ),

            (
                "database.password",
                self.database.password,
            ),

            (
                "database.database",
                self.database.database,
            ),
        ]

        for name, current_value in (
            required_values
        ):

            if not current_value:

                missing.append(name)

        if missing:

            raise ValueError(
                "Missing required configuration: "
                + ", ".join(missing)
            )

        # -----------------------------------------------------
        # RabbitMQ port
        # -----------------------------------------------------

        self._validate_port(
            "rabbitmq.port",
            self.rabbitmq.port,
        )

        # -----------------------------------------------------
        # Worker interval
        # -----------------------------------------------------

        if (
            self.poll_interval_seconds
            <= 0
        ):

            raise ValueError(
                "worker.poll_interval_seconds "
                "must be greater than zero."
            )

        # -----------------------------------------------------
        # Azure endpoint
        # -----------------------------------------------------

        if not self.azure_blob.endpoint_suffix:

            raise ValueError(
                "azure_blob.endpoint_suffix "
                "cannot be empty."
            )

        # -----------------------------------------------------
        # Database connection string
        # -----------------------------------------------------

        if not (
            self.database.connection_string
            .strip()
        ):

            raise ValueError(
                "database.connection_string "
                "cannot be empty."
            )

        # -----------------------------------------------------
        # Log level
        # -----------------------------------------------------

        valid_log_levels = {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }

        if (
            self.log_level
            not in valid_log_levels
        ):

            raise ValueError(
                "Invalid log level: "
                f"{self.log_level}"
            )

    # =========================================================
    # Port validation
    # =========================================================

    @staticmethod
    def _validate_port(
        name: str,
        port: int,
    ) -> None:

        if not 1 <= port <= 65535:

            raise ValueError(
                f"{name} must be between "
                "1 and 65535."
            )