from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any


LOG = logging.getLogger(
    "invoice_insert.state"
)


class ProcessingState:
    RECEIVED = "RECEIVED"
    JSON_READ = "JSON_READ"
    INSERTED = "INSERTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    DATABASE_INSERTED_STATUSES = {
        INSERTED,
        COMPLETED,
    }
    def __init__(
        self,
        path: Path,
    ) -> None:

        if path is None:
            raise ValueError(
                "State file path cannot be None."
            )

        self.path = Path(path)

        self._lock = threading.Lock()

        self._items = self._load()

        LOG.info(
            "processing_state_loaded path=%s items=%d",
            self.path,
            len(self._items),
        )

    # ============================================================
    # Load
    # ============================================================

    def _load(
        self,
    ) -> dict[str, dict[str, Any]]:
        """
        Load persisted state from disk.

        Missing state file is treated as a fresh worker state.

        Corrupt or invalid state is considered a startup error rather
        than silently deleting the state, because silently deleting it
        could cause duplicate invoice inserts.
        """

        if not self.path.exists():

            LOG.info(
                "processing_state_file_not_found "
                "path=%s starting_with_empty_state",
                self.path,
            )

            return {}

        try:

            raw = self.path.read_text(
                encoding="utf-8"
            )

            data = json.loads(
                raw
            )

        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:

            raise RuntimeError(
                "Unable to load invoice insert "
                f"processing state: {self.path}"
            ) from exc

        if not isinstance(
            data,
            dict,
        ):

            raise RuntimeError(
                "Invoice insert processing state "
                "root must be a JSON object."
            )

        # --------------------------------------------------------
        # Validate individual state entries.
        #
        # We do not want malformed state to silently participate
        # in idempotency decisions.
        # --------------------------------------------------------

        for key, value in data.items():

            if not isinstance(
                key,
                str,
            ):

                raise RuntimeError(
                    "Invoice insert processing state "
                    "contains a non-string key."
                )

            if not isinstance(
                value,
                dict,
            ):

                raise RuntimeError(
                    "Invoice insert processing state "
                    f"entry '{key}' must be an object."
                )

        return data

    # ============================================================
    # Get
    # ============================================================

    def get(
        self,
        key: str,
    ) -> dict[str, Any] | None:
        """
        Get the state for one processing key.

        A copy is returned so callers cannot accidentally modify
        internal state without going through set().
        """

        self._validate_key(
            key
        )

        with self._lock:

            item = self._items.get(
                key
            )

            if item is None:
                return None

            return dict(
                item
            )

    # ============================================================
    # Set
    # ============================================================

    def set(
        self,
        key: str,
        **values: Any,
    ) -> None:
        """
        Update and persist state for one processing key.

        Existing values are preserved and supplied values overwrite
        them.

        Example:

            state.set(
                key,
                status="INSERTED",
                extracted_vendor_invoice_id=123
            )
        """

        self._validate_key(
            key
        )

        if not values:
            raise ValueError(
                "At least one state value must be supplied."
            )

        with self._lock:

            current = self._items.get(
                key,
                {},
            )

            if not isinstance(
                current,
                dict,
            ):

                raise RuntimeError(
                    f"Invalid existing state for key: {key}"
                )

            updated = {
                **current,
                **values,
            }

            self._items[key] = updated

            self._persist_locked()

        LOG.debug(
            "processing_state_updated key=%s status=%s",
            key,
            updated.get("status"),
        )

    # ============================================================
    # Exists
    # ============================================================

    def exists(
        self,
        key: str,
    ) -> bool:
        """
        Return True if a processing entry exists.
        """

        self._validate_key(
            key
        )

        with self._lock:

            return key in self._items

    # ============================================================
    # Status
    # ============================================================

    def status(
        self,
        key: str,
    ) -> str | None:
        """
        Return the current status for a processing key.
        """

        item = self.get(
            key
        )

        if item is None:
            return None

        value = item.get(
            "status"
        )

        if value is None:
            return None

        return str(
            value
        )

    # ============================================================
    # Idempotency
    # ============================================================

    def is_completed(
        self,
        key: str,
    ) -> bool:
        """
        Return True when this invoice has already completed
        successfully.

        This is the main idempotency check used by the worker.

        COMPLETED means:

            database insert succeeded
            +
            completion event was published
            +
            original RabbitMQ message was acknowledged

        Therefore the worker should not perform the database insert
        again for this key.
        """

        item = self.get(
            key
        )

        if item is None:
            return False

        return item.get(
            "status"
        ) == "COMPLETED"

    # ============================================================
    # Database insertion checkpoint
    # ============================================================

    def is_inserted(
        self,
        key: str,
    ) -> bool:
        """
        Return True if the database insert has already succeeded.

        This is useful for recovering from a crash that happens
        after SQL insertion but before RabbitMQ ACK.
        """

        item = self.get(
            key
        )

        if item is None:
            return False

        status = item.get(
            "status"
        )

        return status in {
            "INSERTED",
            "COMPLETED",
        }

    # ============================================================
    # Retrieve inserted database ID
    # ============================================================

    def get_inserted_id(
        self,
        key: str,
    ) -> int | None:
        """
        Return the ExtractedVendorInvoiceId stored after a successful
        database insert.
        """

        item = self.get(
            key
        )

        if item is None:
            return None

        value = item.get(
            "extracted_vendor_invoice_id"
        )

        if value is None:
            return None

        try:

            return int(
                value
            )

        except (
            TypeError,
            ValueError,
        ) as exc:

            raise RuntimeError(
                "Invalid extracted_vendor_invoice_id "
                f"stored for processing key: {key}"
            ) from exc

    # ============================================================
    # Mark received
    # ============================================================

    def mark_received(
        self,
        key: str,
        *,
        correlation_id: str | None = None,
        blob_url: str | None = None,
    ) -> None:
        """
        Mark a message as received.
        """

        values: dict[str, Any] = {
            "status": "RECEIVED",
        }

        if correlation_id is not None:
            values[
                "correlation_id"
            ] = correlation_id

        if blob_url is not None:
            values[
                "blob_url"
            ] = blob_url

        self.set(
            key,
            **values,
        )

    # ============================================================
    # Mark inserting
    # ============================================================

    def mark_inserting(
        self,
        key: str,
    ) -> None:
        """
        Mark that SQL insertion has started.
        """

        self.set(
            key,
            status="INSERTING",
        )

    # ============================================================
    # Mark inserted
    # ============================================================

    def mark_inserted(
        self,
        key: str,
        extracted_vendor_invoice_id: int,
    ) -> None:
        """
        Mark successful database insertion.

        IMPORTANT:

        This checkpoint is written immediately after the stored
        procedure successfully returns the database ID.

        Example:

            state.mark_inserted(
                key,
                extracted_vendor_invoice_id=12345,
            )
        """

        if extracted_vendor_invoice_id <= 0:
            raise ValueError(
                "extracted_vendor_invoice_id must be greater than zero."
            )

        self.set(
            key,
            status="INSERTED",
            extracted_vendor_invoice_id=(
                extracted_vendor_invoice_id
            ),
        )

    # ============================================================
    # Mark completed
    # ============================================================

    def mark_completed(
        self,
        key: str,
    ) -> None:
        """
        Mark processing as completely finished.

        This should happen only after:

            1. Database insertion succeeded.
            2. Completion message was published.
            3. Original RabbitMQ message was acknowledged.
        """

        self.set(
            key,
            status="COMPLETED",
        )

    # ============================================================
    # Mark failed
    # ============================================================

    def mark_failed(
        self,
        key: str,
        error: str,
    ) -> None:
        """
        Store failure information.

        The worker can later retry the RabbitMQ message.
        """

        error_text = str(
            error
        ).strip()

        if not error_text:
            error_text = (
                "Unknown processing error."
            )

        self.set(
            key,
            status="FAILED",
            error=error_text,
        )

    # ============================================================
    # Remove
    # ============================================================

    def remove(
        self,
        key: str,
    ) -> None:
        """
        Remove one processing entry.

        This should normally NOT be used during normal processing.

        It is provided mainly for administrative/testing purposes.
        """

        self._validate_key(
            key
        )

        with self._lock:

            if key not in self._items:
                return

            del self._items[key]

            self._persist_locked()

        LOG.info(
            "processing_state_removed key=%s",
            key,
        )

    # ============================================================
    # Clear
    # ============================================================

    def clear(self) -> None:
        """
        Remove all persisted processing state.

        WARNING:
            Clearing this file removes the worker's local
            idempotency history and can result in duplicate
            database inserts if old RabbitMQ messages are replayed.
        """

        with self._lock:

            self._items.clear()

            self._persist_locked()

        LOG.warning(
            "processing_state_cleared path=%s",
            self.path,
        )

    # ============================================================
    # Persistence
    # ============================================================

    def _persist_locked(
        self,
    ) -> None:
        """
        Atomically persist state to disk.

        Caller MUST hold self._lock.

        Strategy:

            state.json.tmp
                    ↓
              flush + fsync
                    ↓
              os.replace()
                    ↓
              state.json

        os.replace() gives us an atomic replacement on the same
        filesystem, preventing a partially written JSON file from
        becoming the active state file.
        """

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary = self.path.with_name(
            f"{self.path.name}.tmp"
        )

        try:

            payload = json.dumps(
                self._items,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )

            # ----------------------------------------------------
            # Write temporary file.
            # ----------------------------------------------------

            with temporary.open(
                "w",
                encoding="utf-8",
            ) as file:

                file.write(
                    payload
                )

                file.flush()

                os.fsync(
                    file.fileno()
                )

            # ----------------------------------------------------
            # Atomically replace active state.
            # ----------------------------------------------------

            os.replace(
                temporary,
                self.path,
            )

        except OSError as exc:

            # ----------------------------------------------------
            # Best-effort cleanup of temporary file.
            # ----------------------------------------------------

            try:

                if temporary.exists():
                    temporary.unlink()

            except OSError:

                LOG.warning(
                    "processing_state_temp_cleanup_failed "
                    "path=%s",
                    temporary,
                    exc_info=True,
                )

            raise RuntimeError(
                "Unable to persist invoice insert "
                f"processing state: {self.path}"
            ) from exc

    # ============================================================
    # Key validation
    # ============================================================

    @staticmethod
    def _validate_key(
        key: str,
    ) -> None:
        """
        Validate a processing key.
        """

        if not isinstance(
            key,
            str,
        ):

            raise TypeError(
                "Processing state key must be a string."
            )

        if not key.strip():

            raise ValueError(
                "Processing state key cannot be empty."
            )
    def mark_received(
        self,
        key: str,
        *,
        correlation_id: str,
        provider_type: str | None,
        result_blob_path: str,
    ) -> None:

        self.set(
            key,
            status="RECEIVED",
            correlation_id=correlation_id,
            provider_type=provider_type,
            result_blob_path=result_blob_path,
        )


    def mark_json_read(
        self,
        key: str,
    ) -> None:

        self.set(
            key,
            status="JSON_READ",
        )


    def mark_inserted(
        self,
        key: str,
        extracted_vendor_invoice_id: int,
    ) -> None:

        if not isinstance(
            extracted_vendor_invoice_id,
            int,
        ):
            raise TypeError(
                "extracted_vendor_invoice_id must be an int."
            )

        if extracted_vendor_invoice_id <= 0:
            raise ValueError(
                "extracted_vendor_invoice_id must be greater than zero."
            )

        self.set(
            key,
            status="INSERTED",
            extracted_vendor_invoice_id=(
                extracted_vendor_invoice_id
            ),
        )


    def mark_completed(
        self,
        key: str,
        extracted_vendor_invoice_id: int | None = None,
    ) -> None:

        existing = self.get(key)

        if existing is None:
            raise RuntimeError(
                "Cannot mark unknown processing key as COMPLETED."
            )

        existing_id = existing.get(
            "extracted_vendor_invoice_id"
        )

        if (
            existing_id is not None
            and extracted_vendor_invoice_id is not None
            and int(existing_id)
            != int(extracted_vendor_invoice_id)
        ):
            raise RuntimeError(
                "ExtractedVendorInvoiceId mismatch "
                "while marking processing as COMPLETED."
            )

        invoice_id = (
            extracted_vendor_invoice_id
            if extracted_vendor_invoice_id is not None
            else existing_id
        )

        if invoice_id is None:
            raise RuntimeError(
                "Cannot mark processing as COMPLETED "
                "without ExtractedVendorInvoiceId."
            )

        self.set(
            key,
            status="COMPLETED",
            extracted_vendor_invoice_id=int(invoice_id),
        )


    def mark_failed(
        self,
        key: str,
        error: str,
    ) -> None:

        error_text = str(error).strip()

        if not error_text:
            error_text = "Unknown processing error."

        current = self.get(key)

        # NEVER overwrite a successful DB insertion with FAILED.
        if current is not None and current.get("status") in {
            "INSERTED",
            "COMPLETED",
        }:
            return

        self.set(
            key,
            status="FAILED",
            error=error_text,
        )


    def is_database_inserted(
        self,
        key: str,
    ) -> bool:

        item = self.get(key)

        if item is None:
            return False

        return item.get("status") in {
            "INSERTED",
            "COMPLETED",
        }


    def get_inserted_id(
        self,
        key: str,
    ) -> int | None:

        item = self.get(key)

        if item is None:
            return None

        value = item.get(
            "extracted_vendor_invoice_id"
        )

        if value is None:
            return None

        try:
            return int(value)

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise RuntimeError(
                "Stored ExtractedVendorInvoiceId is invalid."
            ) from exc