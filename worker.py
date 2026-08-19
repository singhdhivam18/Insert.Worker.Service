from __future__ import annotations

import hashlib
import logging
import time

from config import WorkerConfig
from insert_json import JsonDataInserter
from json_azure import AzureJsonReader
from read_extraction_rabbitmq import (
    ExtractionCompletedMessage,
    RabbitMqExtractionReader,
)
from state import ProcessingState


LOG = logging.getLogger(
    "invoice_insert.worker"
)


class InvoiceInsertWorker:
    """
    Invoice insertion worker.

    Processing flow:

        RabbitMQ
            ↓
        idempotency check
            ↓
        read JSON from Azure
            ↓
        insert into SQL Server
            ↓
        persist INSERTED checkpoint
            ↓
        publish insert-completed notification
            ↓
        ACK original RabbitMQ message
            ↓
        persist COMPLETED

    Important idempotency rule:

        Once SQL insertion succeeds and the database ID has
        been persisted as INSERTED, the worker must NEVER
        insert the same invoice again because of a later
        notification or ACK failure.

    RabbitMQ input:
        invoice.extraction.completed

    RabbitMQ output:
        invoice.insert.completed
    """

    def __init__(
        self,
        config: WorkerConfig,
        *,
        state: ProcessingState | None = None,
        rabbit_reader: RabbitMqExtractionReader | None = None,
        azure_reader: AzureJsonReader | None = None,
        database_inserter: JsonDataInserter | None = None,
    ) -> None:

        self.config = config

        # =====================================================
        # State / Idempotency
        # =====================================================

        self.state = (
            state
            or ProcessingState(
                config.state_file
            )
        )

        # =====================================================
        # RabbitMQ
        # =====================================================

        self.rabbit_reader = (
            rabbit_reader
            or RabbitMqExtractionReader(
                config.rabbitmq
            )
        )

        # =====================================================
        # Azure
        # =====================================================

        self.azure_reader = (
            azure_reader
            or AzureJsonReader(
                config.azure_blob
            )
        )

        # =====================================================
        # SQL Server
        # =====================================================

        self.database_inserter = (
            database_inserter
            or JsonDataInserter(
                config.database.connection_string
            )
        )

    # =========================================================
    # Close
    # =========================================================

    def close(self) -> None:
        """
        Close worker resources.
        """

        try:

            self.rabbit_reader.close()

        finally:

            LOG.info(
                "invoice_insert_worker_connections_closed"
            )

    # =========================================================
    # Run forever
    # =========================================================

    def run_forever(self) -> None:
        """
        Continuously process extraction-completed messages.
        """

        LOG.info(
            "invoice_insert_worker_started"
        )

        try:

            while True:

                self.poll_once()

                time.sleep(
                    self.config.poll_interval_seconds
                )

        finally:

            self.close()

    # =========================================================
    # Poll once
    # =========================================================

    def poll_once(self) -> bool:
        """
        Receive and process at most one RabbitMQ message.

        Returns:

            True:
                Message completed successfully.

            False:
                No message or processing failed.
        """

        LOG.info(
            "invoice_insert_poll_started"
        )

        # -----------------------------------------------------
        # Ensure RabbitMQ connection
        # -----------------------------------------------------

        self.rabbit_reader.connect()

        LOG.info(
            "invoice_insert_waiting_for_message "
            "queue=%s",
            self.config.rabbitmq.queue,
        )

        # -----------------------------------------------------
        # Receive message
        # -----------------------------------------------------

        message = (
            self.rabbit_reader.receive()
        )

        if message is None:

            LOG.info(
                "invoice_insert_no_message "
                "queue=%s",
                self.config.rabbitmq.queue,
            )

            return False

        LOG.info(
            "invoice_insert_message_received "
            "correlation_id=%s "
            "provider_type=%s "
            "status=%s "
            "result_blob_path=%s "
            "delivery_tag=%s",
            message.correlation_id,
            message.provider_type,
            message.status,
            message.result_blob_path,
            message.delivery_tag,
        )

        try:

            self._process_message(
                message
            )

            LOG.info(
                "invoice_insert_poll_completed "
                "correlation_id=%s",
                message.correlation_id,
            )

            return True

        except Exception as exc:

            key = self._processing_key(
                message
            )

            # -------------------------------------------------
            # Record failure.
            #
            # state.py protects INSERTED/COMPLETED from being
            # downgraded to FAILED.
            # -------------------------------------------------

            try:

                self.state.mark_failed(
                    key,
                    str(exc),
                )

            except Exception:

                LOG.exception(
                    "invoice_insert_state_failure_record_failed "
                    "correlation_id=%s",
                    message.correlation_id,
                )

            LOG.exception(
                "invoice_insert_failed "
                "correlation_id=%s",
                message.correlation_id,
            )

            # -------------------------------------------------
            # Requeue original extraction message.
            # -------------------------------------------------

            try:

                self.rabbit_reader.reject(
                    message.delivery_tag,
                    requeue=True,
                )

                LOG.info(
                    "invoice_insert_message_requeued "
                    "correlation_id=%s "
                    "delivery_tag=%s",
                    message.correlation_id,
                    message.delivery_tag,
                )

            except Exception:

                LOG.exception(
                    "invoice_insert_requeue_failed "
                    "correlation_id=%s",
                    message.correlation_id,
                )

            return False

    # =========================================================
    # Process message
    # =========================================================

    def _process_message(
        self,
        message: ExtractionCompletedMessage,
    ) -> None:
        """
        Process one extraction-completed message.
        """

        key = self._processing_key(
            message
        )

        LOG.info(
            "invoice_insert_processing_started "
            "correlation_id=%s "
            "processing_key=%s",
            message.correlation_id,
            key,
        )

        # =====================================================
        # 0. IDEMPOTENCY CHECK
        # =====================================================

        existing = self.state.get(
            key
        )

        if existing is not None:

            existing_status = existing.get(
                "status"
            )

            LOG.info(
                "invoice_insert_existing_state_found "
                "correlation_id=%s "
                "status=%s",
                message.correlation_id,
                existing_status,
            )

            # -------------------------------------------------
            # COMPLETED
            # -------------------------------------------------

            if existing_status == (
                ProcessingState.COMPLETED
            ):

                LOG.info(
                    "invoice_insert_already_completed "
                    "correlation_id=%s",
                    message.correlation_id,
                )

                self.rabbit_reader.acknowledge(
                    message.delivery_tag
                )

                return

            # -------------------------------------------------
            # INSERTED
            #
            # Database already contains invoice.
            #
            # DO NOT call database_inserter.insert().
            # -------------------------------------------------

            if existing_status == (
                ProcessingState.INSERTED
            ):

                extracted_vendor_invoice_id = (
                    self.state.get_inserted_id(
                        key
                    )
                )

                if extracted_vendor_invoice_id is None:

                    raise RuntimeError(
                        "Processing state is INSERTED "
                        "but ExtractedVendorInvoiceId is missing."
                    )

                LOG.info(
                    "invoice_insert_database_already_completed "
                    "correlation_id=%s "
                    "extracted_vendor_invoice_id=%s",
                    message.correlation_id,
                    extracted_vendor_invoice_id,
                )

                # -------------------------------------------------
                # Retry only the notification.
                # -------------------------------------------------

                self._publish_completion(
                    message,
                    extracted_vendor_invoice_id,
                )

                # -------------------------------------------------
                # ACK original message.
                # -------------------------------------------------

                self.rabbit_reader.acknowledge(
                    message.delivery_tag
                )

                # -------------------------------------------------
                # Mark fully completed.
                # -------------------------------------------------

                self.state.mark_completed(
                    key,
                    extracted_vendor_invoice_id,
                )

                LOG.info(
                    "invoice_insert_existing_database_result_completed "
                    "correlation_id=%s "
                    "extracted_vendor_invoice_id=%s",
                    message.correlation_id,
                    extracted_vendor_invoice_id,
                )

                return

        # =====================================================
        # 1. RECEIVED
        # =====================================================

        self.state.mark_received(
            key,
            correlation_id=(
                message.correlation_id
            ),
            provider_type=(
                message.provider_type
            ),
            result_blob_path=(
                message.result_blob_path
            ),
        )

        LOG.info(
            "invoice_insert_processing_received "
            "correlation_id=%s",
            message.correlation_id,
        )

        # =====================================================
        # 2. VALIDATE EXTRACTION MESSAGE
        # =====================================================

        if message.status != "success":

            raise RuntimeError(
                "Extraction result is not successful: "
                f"{message.status}"
            )

        if not message.result_blob_path:

            raise ValueError(
                "result_blob_path cannot be empty."
            )

        # =====================================================
        # 3. READ JSON FROM AZURE
        # =====================================================

        LOG.info(
            "invoice_insert_json_read_started "
            "correlation_id=%s "
            "result_blob_path=%s",
            message.correlation_id,
            message.result_blob_path,
        )

        extracted_json = (
            self.azure_reader.read(
                message.result_blob_path
            )
        )

        if not isinstance(
            extracted_json,
            dict,
        ):

            raise ValueError(
                "Azure extraction result must be a JSON object."
            )

        LOG.info(
            "invoice_insert_json_read_completed "
            "correlation_id=%s",
            message.correlation_id,
        )

        self.state.mark_json_read(
            key
        )

        # =====================================================
        # 4. INSERT INTO SQL SERVER
        # =====================================================

        LOG.info(
            "invoice_insert_database_started "
            "correlation_id=%s",
            message.correlation_id,
        )

        extracted_vendor_invoice_id = (
            self.database_inserter.insert(
                extracted_json=extracted_json,
                requested_by="invoice_insert_worker",
            )
        )

        LOG.info(
            "invoice_insert_database_completed "
            "correlation_id=%s "
            "extracted_vendor_invoice_id=%s",
            message.correlation_id,
            extracted_vendor_invoice_id,
        )

        # =====================================================
        # 5. PERSIST DATABASE INSERT CHECKPOINT
        # =====================================================

        # IMPORTANT:
        #
        # This MUST happen before publishing the notification.
        #
        # If notification publishing fails after this point,
        # the message can be retried without inserting another
        # invoice into SQL Server.

        self.state.mark_inserted(
            key,
            extracted_vendor_invoice_id,
        )

        LOG.info(
            "invoice_insert_idempotency_checkpoint_saved "
            "correlation_id=%s "
            "extracted_vendor_invoice_id=%s",
            message.correlation_id,
            extracted_vendor_invoice_id,
        )

        # =====================================================
        # 6. PUBLISH COMPLETION NOTIFICATION
        # =====================================================

        self._publish_completion(
            message,
            extracted_vendor_invoice_id,
        )

        # =====================================================
        # 7. ACK ORIGINAL EXTRACTION MESSAGE
        # =====================================================

        LOG.info(
            "invoice_insert_acknowledging_message "
            "correlation_id=%s "
            "delivery_tag=%s",
            message.correlation_id,
            message.delivery_tag,
        )

        self.rabbit_reader.acknowledge(
            message.delivery_tag
        )

        LOG.info(
            "invoice_insert_message_acknowledged "
            "correlation_id=%s "
            "delivery_tag=%s",
            message.correlation_id,
            message.delivery_tag,
        )

        # =====================================================
        # 8. COMPLETED
        # =====================================================

        self.state.mark_completed(
            key,
            extracted_vendor_invoice_id,
        )

        LOG.info(
            "invoice_insert_completed "
            "correlation_id=%s "
            "extracted_vendor_invoice_id=%s",
            message.correlation_id,
            extracted_vendor_invoice_id,
        )

    # =========================================================
    # Publish completion
    # =========================================================

    def _publish_completion(
        self,
        message: ExtractionCompletedMessage,
        extracted_vendor_invoice_id: int,
    ) -> None:
        """
        Publish invoice.insert.completed.

        This happens BEFORE ACK.

        If publishing fails:

            message remains unacknowledged
            +
            state remains INSERTED

        Therefore the next retry will publish the notification
        without inserting into SQL Server again.
        """

        if extracted_vendor_invoice_id <= 0:

            raise ValueError(
                "ExtractedVendorInvoiceId must be greater than zero."
            )

        event = {
            "correlation_id": (
                message.correlation_id
            ),
            "provider_type": (
                message.provider_type
            ),
            "status": "success",
            "extracted_vendor_invoice_id": (
                extracted_vendor_invoice_id
            ),
        }

        LOG.info(
            "invoice_insert_publishing_completion "
            "correlation_id=%s "
            "queue=%s "
            "extracted_vendor_invoice_id=%s",
            message.correlation_id,
            self.config.rabbitmq.notification_queue,
            extracted_vendor_invoice_id,
        )

        self.rabbit_reader.publish_notification(
            event,
            self.config.rabbitmq.notification_queue,
        )

        LOG.info(
            "invoice_insert_completion_published "
            "correlation_id=%s "
            "extracted_vendor_invoice_id=%s",
            message.correlation_id,
            extracted_vendor_invoice_id,
        )

    # =========================================================
    # Processing key
    # =========================================================

    @staticmethod
    def _processing_key(
        message: ExtractionCompletedMessage,
    ) -> str:
        """
        Create deterministic idempotency key.

        The same extraction event must always generate the
        same key.

        Current identity:

            correlation_id + result_blob_path

        SHA-256 is used so the state file does not contain
        potentially long/raw identifiers as dictionary keys.
        """

        correlation_id = (
            str(
                message.correlation_id
            ).strip()
        )

        result_blob_path = (
            str(
                message.result_blob_path or ""
            ).strip()
        )

        if not correlation_id:

            raise ValueError(
                "correlation_id cannot be empty."
            )

        if not result_blob_path:

            raise ValueError(
                "result_blob_path cannot be empty."
            )

        raw_key = (
            f"{correlation_id}:"
            f"{result_blob_path}"
        )

        return hashlib.sha256(
            raw_key.encode(
                "utf-8"
            )
        ).hexdigest()