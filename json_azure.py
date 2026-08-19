from __future__ import annotations

import json
import logging
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from config import AzureBlobConfig


LOG = logging.getLogger(
    "invoice_insert.json_azure"
)


AZURE_API_VERSION = "2023-11-03"
HTTP_TIMEOUT_SECONDS = 60


class AzureJsonReader:
    """
    Reads extracted invoice JSON from Azure Blob Storage.

    Input:
        result_blob_path

    Output:
        Parsed Python dictionary.

    Responsibilities:
        - Build Azure Blob URL.
        - Authenticate using Shared Key.
        - Download the JSON blob.
        - Decode UTF-8.
        - Parse JSON.

    Does NOT:
        - Read RabbitMQ.
        - Insert into database.
        - Acknowledge RabbitMQ.
        - Modify extracted data.
    """

    def __init__(
        self,
        config: AzureBlobConfig,
    ):
        self.config = config

        self._validate_configuration()

        LOG.info(
            "Azure JSON reader initialized "
            "account=%s container=%s",
            self.config.account_name,
            self.config.container,
        )

    # =========================================================
    # Validation
    # =========================================================

    def _validate_configuration(self) -> None:

        if not self.config.account_name:
            raise ValueError(
                "Azure storage account name is required."
            )

        if not self.config.account_key:
            raise ValueError(
                "Azure storage account key is required."
            )

        if not self.config.container:
            raise ValueError(
                "Azure blob container is required."
            )

        if not self.config.endpoint_suffix:
            raise ValueError(
                "Azure storage endpoint suffix is required."
            )

    # =========================================================
    # Read JSON
    # =========================================================

    def read(
        self,
        result_blob_path: str,
    ) -> dict:
        """
        Download and parse an extracted JSON blob.

        Args:
            result_blob_path:
                Blob path returned by the extraction worker.

        Returns:
            Parsed JSON object as a Python dictionary.
        """

        if not result_blob_path:
            raise ValueError(
                "result_blob_path cannot be empty."
            )

        encoded_blob_path = (
            self._encode_blob_path(
                result_blob_path
            )
        )

        url = (
            f"https://"
            f"{self.config.account_name}"
            f".blob."
            f"{self.config.endpoint_suffix}"
            f"/"
            f"{self.config.container}"
            f"/"
            f"{encoded_blob_path}"
        )

        LOG.info(
            "azure_json_download_start "
            "blob_path=%s",
            result_blob_path,
        )

        headers = {
            "x-ms-date": self._utc_date(),
            "x-ms-version": AZURE_API_VERSION,
        }

        resource = (
            f"/"
            f"{self.config.account_name}"
            f"/"
            f"{self.config.container}"
            f"/"
            f"{result_blob_path}"
        )

        headers["Authorization"] = (
            self._authorization(
                method="GET",
                resource=resource,
                headers=headers,
            )
        )

        request = Request(
            url,
            headers=headers,
            method="GET",
        )

        try:

            with urlopen(
                request,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:

                if response.status != 200:
                    raise RuntimeError(
                        "Azure JSON download failed "
                        f"with HTTP {response.status}"
                    )

                raw_data = response.read()

        except HTTPError as exc:

            try:
                body = exc.read().decode(
                    "utf-8",
                    errors="replace",
                )
            except Exception:
                body = ""

            LOG.error(
                "azure_json_download_failed "
                "status=%s "
                "blob_path=%s "
                "response=%s",
                exc.code,
                result_blob_path,
                body,
            )

            raise RuntimeError(
                "Azure JSON download failed "
                f"with HTTP {exc.code}"
            ) from exc

        except URLError as exc:

            raise RuntimeError(
                "Unable to connect to Azure while "
                "downloading JSON: "
                f"{exc.reason}"
            ) from exc

        # -----------------------------------------------------
        # Decode JSON
        # -----------------------------------------------------

        try:

            text = raw_data.decode(
                "utf-8"
            )

        except UnicodeDecodeError as exc:

            raise ValueError(
                "Azure JSON blob is not valid UTF-8."
            ) from exc

        try:

            payload = json.loads(
                text
            )

        except json.JSONDecodeError as exc:

            raise ValueError(
                "Azure blob does not contain "
                "valid JSON."
            ) from exc

        if not isinstance(
            payload,
            dict,
        ):

            raise ValueError(
                "Azure extraction JSON must "
                "contain a JSON object."
            )

        LOG.info(
            "azure_json_download_success "
            "blob_path=%s "
            "size=%d",
            result_blob_path,
            len(raw_data),
        )

        return payload

    # =========================================================
    # URL helpers
    # =========================================================

    @staticmethod
    def _encode_blob_path(
        blob_path: str,
    ) -> str:
        """
        Encode each path component independently.

        Keeps '/' as the blob path separator.
        """

        return "/".join(
            quote(
                part,
                safe="-_.~",
            )
            for part in blob_path.split("/")
        )

    # =========================================================
    # Azure authentication
    # =========================================================

    def _authorization(
        self,
        method: str,
        resource: str,
        headers: dict[str, str],
    ) -> str:

        canonical_headers = "".join(
            f"{key.lower()}:{value}\n"
            for key, value in sorted(
                headers.items(),
                key=lambda item: item[0].lower(),
            )
            if key.lower().startswith(
                "x-ms-"
            )
        )

        standard_headers = [
            "",
            "",
            "",
            "",
            "",
            headers.get(
                "Date",
                "",
            ),
            "",
            "",
            "",
            "",
            "",
        ]

        string_to_sign = (
            method
            + "\n"
            + "\n".join(
                standard_headers
            )
            + "\n"
            + canonical_headers
            + resource
        )

        import base64
        import hashlib
        import hmac

        try:

            decoded_key = (
                base64.b64decode(
                    self.config.account_key
                )
            )

            signature = base64.b64encode(
                hmac.new(
                    decoded_key,
                    string_to_sign.encode(
                        "utf-8"
                    ),
                    hashlib.sha256,
                ).digest()
            ).decode(
                "ascii"
            )

        except Exception as exc:

            raise RuntimeError(
                "Unable to generate Azure "
                "Shared Key authorization."
            ) from exc

        return (
            "SharedKey "
            f"{self.config.account_name}:"
            f"{signature}"
        )

    # =========================================================
    # Date
    # =========================================================

    @staticmethod
    def _utc_date() -> str:
        from datetime import datetime, timezone
        from email.utils import format_datetime

        return format_datetime(
            datetime.now(
                timezone.utc
            ),
            usegmt=True,
        )