from __future__ import annotations

import json
import logging
import socket
import ssl
import struct

from dataclasses import dataclass

from config import RabbitMqConfig


LOG = logging.getLogger(
    "invoice_insert.rabbitmq"
)


# ============================================================
# AMQP frame constants
# ============================================================

FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_END = 0xCE


# ============================================================
# Incoming extraction message
# ============================================================

@dataclass(frozen=True)
class ExtractionCompletedMessage:
    """
    Message published by invoice_extraction_worker.

    Expected JSON:

    {
        "correlation_id": "...",
        "provider_type": "...",
        "status": "success",
        "result_blob_path": "..."
    }
    """

    correlation_id: str
    provider_type: str
    status: str
    result_blob_path: str

    delivery_tag: int

    redelivered: bool

    @classmethod
    def from_body(
        cls,
        body: bytes,
        delivery_tag: int,
        redelivered: bool,
    ) -> "ExtractionCompletedMessage":

        try:
            payload = json.loads(
                body.decode("utf-8")
            )

        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:

            raise ValueError(
                "RabbitMQ message must be UTF-8 JSON."
            ) from exc

        if not isinstance(
            payload,
            dict,
        ):

            raise ValueError(
                "RabbitMQ message JSON "
                "must be an object."
            )

        correlation_id = str(
            payload.get(
                "correlation_id",
                "",
            )
        ).strip()

        provider_type = str(
            payload.get(
                "provider_type",
                "",
            )
        ).strip()

        status = str(
            payload.get(
                "status",
                "",
            )
        ).strip().lower()

        result_blob_path = str(
            payload.get(
                "result_blob_path",
                "",
            )
        ).strip()

        if not correlation_id:

            raise ValueError(
                "RabbitMQ message requires "
                "correlation_id."
            )

        if not provider_type:

            raise ValueError(
                "RabbitMQ message requires "
                "provider_type."
            )

        if not status:

            raise ValueError(
                "RabbitMQ message requires "
                "status."
            )

        if not result_blob_path:

            raise ValueError(
                "RabbitMQ message requires "
                "result_blob_path."
            )

        return cls(
            correlation_id=correlation_id,
            provider_type=provider_type,
            status=status,
            result_blob_path=result_blob_path,
            delivery_tag=delivery_tag,
            redelivered=redelivered,
        )


# ============================================================
# RabbitMQ reader
# ============================================================

class RabbitMqExtractionReader:
    """
    Minimal AMQP 0-9-1 consumer.

    Uses:
        basic.get

    The message is intentionally NOT acknowledged
    automatically.

    The worker must call acknowledge() only after:

        RabbitMQ
            ↓
        Azure JSON read
            ↓
        Database insert
            ↓
        SUCCESS
    """

    def __init__(
        self,
        config: RabbitMqConfig,
    ):
        self.config = config

        self.sock: (
            socket.socket | None
        ) = None

        self.channel = 1

        self.frame_max = 131072

    # ========================================================
    # Context manager
    # ========================================================

    def __enter__(
        self,
    ) -> "RabbitMqExtractionReader":

        self.connect()

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:

        self.close()

    # ========================================================
    # Connection
    # ========================================================

    def connect(self) -> None:

        if self.sock is not None:
            return

        LOG.info(
            "rabbitmq_connecting "
            "host=%s port=%s queue=%s",
            self.config.host,
            self.config.port,
            self.config.queue,
        )

        try:

            raw_socket = socket.create_connection(
                (
                    self.config.host,
                    self.config.port,
                ),
                timeout=20,
            )

            LOG.info(
                "rabbitmq_tcp_connected "
                "host=%s port=%s",
                self.config.host,
                self.config.port,
            )

            if self.config.use_ssl:

                context = (
                    ssl.create_default_context()
                )

                self.sock = (
                    context.wrap_socket(
                        raw_socket,
                        server_hostname=(
                            self.config.host
                        ),
                    )
                )

            else:

                self.sock = raw_socket

            self.sock.settimeout(30)

            # AMQP protocol header.
            self.sock.sendall(
                b"AMQP\x00\x00\x09\x01"
            )

            self._handshake()

            LOG.info(
                "rabbitmq_connected "
                "host=%s port=%s "
                "vhost=%s queue=%s",
                self.config.host,
                self.config.port,
                self.config.virtual_host,
                self.config.queue,
            )

        except Exception:

            self.close()

            raise

    # ========================================================
    # Receive one message
    # ========================================================

    def receive(
        self,
    ) -> ExtractionCompletedMessage | None:
        """
        Retrieve at most one message.

        The message remains unacknowledged.

        Returns:
            ExtractionCompletedMessage
            or None when queue is empty.
        """

        if self.sock is None:

            raise RuntimeError(
                "RabbitMQ reader is not connected."
            )

        # ----------------------------------------------------
        # basic.get
        #
        # ticket      = 0
        # queue       = configured queue
        # no_ack      = false
        # ----------------------------------------------------

        request = (
            struct.pack(
                ">H",
                0,
            )
            + self._shortstr(
                self.config.queue
            )
            + b"\x00"
        )

        self._method(
            60,
            70,
            request,
        )

        _, _, payload = (
            self._read_method()
        )

        # ----------------------------------------------------
        # basic.get-empty
        # ----------------------------------------------------

        if payload[:4] == struct.pack(
            ">HH",
            60,
            72,
        ):

            return None

        # ----------------------------------------------------
        # basic.get-ok
        # ----------------------------------------------------

        self._expect(
            payload,
            60,
            71,
        )

        if len(payload) < 13:

            raise RuntimeError(
                "Invalid basic.get-ok response."
            )

        delivery_tag = struct.unpack(
            ">Q",
            payload[4:12],
        )[0]

        redelivered = bool(
            payload[12]
        )

        offset = 13

        # exchange
        _, offset = self._read_shortstr(
            payload,
            offset,
        )

        # routing key
        _, offset = self._read_shortstr(
            payload,
            offset,
        )

        body_size = (
            self._read_content_header()
        )

        body = self._read_body(
            body_size
        )

        message = (
            ExtractionCompletedMessage.from_body(
                body=body,
                delivery_tag=delivery_tag,
                redelivered=redelivered,
            )
        )

        LOG.info(
            "extraction_message_received "
            "correlation_id=%s "
            "provider_type=%s "
            "status=%s "
            "blob_path=%s "
            "redelivered=%s",
            message.correlation_id,
            message.provider_type,
            message.status,
            message.result_blob_path,
            message.redelivered,
        )

        return message

    # ========================================================
    # Acknowledge
    # ========================================================

    def acknowledge(
        self,
        delivery_tag: int,
    ) -> None:

        if self.sock is None:

            raise RuntimeError(
                "RabbitMQ reader is not connected."
            )

        # basic.ack
        #
        # delivery-tag
        # multiple=false
        #
        payload = (
            struct.pack(
                ">Q",
                delivery_tag,
            )
            + b"\x00"
        )

        self._method(
            60,
            80,
            payload,
        )

        LOG.info(
            "rabbitmq_message_acknowledged "
            "delivery_tag=%s",
            delivery_tag,
        )

    # ========================================================
    # Reject
    # ========================================================

    def reject(
        self,
        delivery_tag: int,
        *,
        requeue: bool = True,
    ) -> None:

        if self.sock is None:

            raise RuntimeError(
                "RabbitMQ reader is not connected."
            )

        # basic.reject
        #
        # delivery-tag
        # requeue
        #
        payload = (
            struct.pack(
                ">Q",
                delivery_tag,
            )
            + (
                b"\x01"
                if requeue
                else b"\x00"
            )
        )

        self._method(
            60,
            90,
            payload,
        )

        LOG.warning(
            "rabbitmq_message_rejected "
            "delivery_tag=%s "
            "requeue=%s",
            delivery_tag,
            requeue,
        )

        # ========================================================
    # Publish notification
    # ========================================================

    def publish_notification(
        self,
        event: dict,
        queue: str,
    ) -> None:
        """
        Publish a JSON notification to RabbitMQ.

        Uses the default exchange:
            exchange = ""

        Therefore the queue name is used as
        the routing key.

        Example:

            publish_notification(
                event={
                    "correlation_id": "...",
                    "vendor_invoice_id": 8,
                    "status": "completed",
                },
                queue="invoice.insert.completed",
            )
        """

        if self.sock is None:
            raise RuntimeError(
                "RabbitMQ reader is not connected."
            )

        if not queue:
            raise ValueError(
                "Notification queue cannot be empty."
            )

        if not isinstance(event, dict):
            raise ValueError(
                "Notification event must be a JSON object."
            )

        body = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        LOG.info(
            "rabbitmq_notification_publish_started "
            "queue=%s",
            queue,
        )

        try:

            # ------------------------------------------------
            # basic.publish
            # ------------------------------------------------
            #
            # ticket       = 0
            # exchange     = ""
            # routing-key  = queue
            # mandatory    = false
            #
            # ------------------------------------------------

            publish_args = (
                struct.pack(
                    ">H",
                    0,
                )
                + self._shortstr("")
                + self._shortstr(queue)
                + b"\x00"
            )

            self._method(
                60,
                40,
                publish_args,
            )

            # ------------------------------------------------
            # Content header
            # ------------------------------------------------

            property_flags = (
                (1 << 15)   # content-type
                | (1 << 12) # delivery-mode
            )

            content_properties = (
                struct.pack(
                    ">H",
                    property_flags,
                )
                + self._shortstr(
                    "application/json"
                )
                + b"\x02"   # delivery-mode = persistent
            )

            content_header = (
                struct.pack(
                    ">HHQ",
                    60,      # class-id = basic
                    0,       # weight
                    len(body),
                )
                + content_properties
            )

            self._frame(
                FRAME_HEADER,
                self.channel,
                content_header,
            )

            # ------------------------------------------------
            # Message body
            # ------------------------------------------------

            self._frame(
                FRAME_BODY,
                self.channel,
                body,
            )

            LOG.info(
                "rabbitmq_notification_publish_completed "
                "queue=%s "
                "size=%s",
                queue,
                len(body),
            )

        except Exception:

            LOG.exception(
                "rabbitmq_notification_publish_failed "
                "queue=%s",
                queue,
            )

            raise
    # ========================================================
    # Close
    # ========================================================

    def close(self) -> None:

        if self.sock is None:
            return

        try:

            self._method(
                20,
                40,
                (
                    struct.pack(
                        ">H",
                        0,
                    )
                    + self._shortstr("")
                    + struct.pack(
                        ">HH",
                        0,
                        0,
                    )
                ),
            )

        except Exception:
            pass

        try:
            self.sock.close()

        except OSError:
            pass

        self.sock = None

        LOG.debug(
            "rabbitmq_connection_closed"
        )

    # ========================================================
    # AMQP handshake
    # ========================================================

    def _handshake(self) -> None:

        # ----------------------------------------------------
        # connection.start
        # ----------------------------------------------------

        _, _, start = (
            self._read_method()
        )

        self._expect(
            start,
            10,
            10,
        )

        credentials = (
            b"\x00"
            + self.config.username.encode(
                "utf-8"
            )
            + b"\x00"
            + self.config.password.encode(
                "utf-8"
            )
        )

        response = (
            self._table(
                {
                    "product": (
                        "invoice-insert-worker"
                    ),
                    "platform": (
                        "Python stdlib"
                    ),
                }
            )
            + self._shortstr(
                "PLAIN"
            )
            + self._longstr(
                credentials
            )
            + self._shortstr(
                "en_US"
            )
        )

        self._method(
            10,
            11,
            response,
        )

        # ----------------------------------------------------
        # connection.tune
        # ----------------------------------------------------

        _, _, tune = (
            self._read_method()
        )

        self._expect(
            tune,
            10,
            30,
        )

        if len(tune) < 12:

            raise RuntimeError(
                "Invalid connection.tune response."
            )

        (
            server_channel_max,
            server_frame_max,
            heartbeat,
        ) = struct.unpack(
            ">HIH",
            tune[4:12],
        )

        # We need only one channel.
        channel_max = (
            server_channel_max
            or 2047
        )

        self.frame_max = (
            server_frame_max
            or self.frame_max
        )

        self._method(
            10,
            31,
            struct.pack(
                ">HIH",
                channel_max,
                self.frame_max,
                heartbeat,
            ),
        )

        # ----------------------------------------------------
        # connection.open
        # ----------------------------------------------------

        virtual_host = (
            self.config.virtual_host
            or "/"
        )

        self._method(
            10,
            40,
            (
                self._shortstr(
                    virtual_host
                )
                + self._shortstr("")
                + b"\x00"
            ),
        )

        _, _, opened = (
            self._read_method()
        )

        self._expect(
            opened,
            10,
            41,
        )

        # ----------------------------------------------------
        # channel.open
        # ----------------------------------------------------

        self._method(
            20,
            10,
            self._shortstr(""),
        )

        _, _, channel_opened = (
            self._read_method()
        )

        self._expect(
            channel_opened,
            20,
            11,
        )

        # ----------------------------------------------------
        # queue.declare
        # ----------------------------------------------------

        queue_arguments = (
            struct.pack(
                ">H",
                0,
            )
            + self._shortstr(
                self.config.queue
            )
            + b"\x02"
            + self._table({})
        )

        self._method(
            50,
            10,
            queue_arguments,
        )

        _, _, declared = (
            self._read_method()
        )

        self._expect(
            declared,
            50,
            11,
        )

        LOG.info(
            "rabbitmq_queue_ready "
            "queue=%s",
            self.config.queue,
        )

    # ========================================================
    # AMQP method helpers
    # ========================================================

    def _method(
        self,
        class_id: int,
        method_id: int,
        args: bytes,
    ) -> None:

        channel = (
            0
            if class_id == 10
            else self.channel
        )

        payload = (
            struct.pack(
                ">HH",
                class_id,
                method_id,
            )
            + args
        )

        self._frame(
            FRAME_METHOD,
            channel,
            payload,
        )

    def _frame(
        self,
        frame_type: int,
        channel: int,
        payload: bytes,
    ) -> None:

        if self.sock is None:

            raise RuntimeError(
                "RabbitMQ socket is not connected."
            )

        if (
            self.frame_max
            and len(payload)
            > self.frame_max
        ):

            raise ValueError(
                "AMQP frame exceeds negotiated "
                f"frame_max={self.frame_max}."
            )

        header = struct.pack(
            ">BHI",
            frame_type,
            channel,
            len(payload),
        )

        self.sock.sendall(
            header
            + payload
            + bytes([FRAME_END])
        )

    # ========================================================
    # AMQP frame reader
    # ========================================================

    def _read_method(self):

        while True:

            frame_type, channel, payload = (
                self._read_frame()
            )

            if frame_type == FRAME_METHOD:

                if len(payload) < 4:

                    raise RuntimeError(
                        "Invalid AMQP method frame."
                    )

                return (
                    frame_type,
                    channel,
                    payload,
                )

            if (
                frame_type
                == FRAME_HEARTBEAT
            ):

                continue

            raise RuntimeError(
                "Unexpected AMQP frame received: "
                f"type={frame_type} "
                f"channel={channel}"
            )

    def _read_content_header(
        self,
    ) -> int:

        (
            frame_type,
            channel,
            payload,
        ) = self._read_frame()

        if (
            frame_type != FRAME_HEADER
            or channel != self.channel
            or len(payload) < 12
        ):

            raise RuntimeError(
                "Invalid AMQP content header."
            )

        (
            class_id,
            _weight,
            body_size,
        ) = struct.unpack(
            ">HHQ",
            payload[:12],
        )

        if class_id != 60:

            raise RuntimeError(
                "Unexpected AMQP content class."
            )

        return body_size

    def _read_body(
        self,
        body_size: int,
    ) -> bytes:

        chunks = bytearray()

        while len(chunks) < body_size:

            (
                frame_type,
                channel,
                payload,
            ) = self._read_frame()

            if (
                frame_type != FRAME_BODY
                or channel != self.channel
            ):

                raise RuntimeError(
                    "Invalid AMQP content body frame."
                )

            chunks.extend(payload)

        if len(chunks) != body_size:

            raise RuntimeError(
                "AMQP message body size mismatch."
            )

        return bytes(chunks)

    def _read_frame(self):

        header = self._read_exact(
            7
        )

        (
            frame_type,
            channel,
            size,
        ) = struct.unpack(
            ">BHI",
            header,
        )

        payload = self._read_exact(
            size
        )

        if (
            self._read_exact(1)
            != bytes([FRAME_END])
        ):

            raise RuntimeError(
                "Invalid AMQP frame terminator."
            )

        return (
            frame_type,
            channel,
            payload,
        )

    def _read_exact(
        self,
        length: int,
    ) -> bytes:

        if self.sock is None:

            raise RuntimeError(
                "RabbitMQ socket is not connected."
            )

        data = bytearray()

        while len(data) < length:

            chunk = self.sock.recv(
                length - len(data)
            )

            if not chunk:

                raise ConnectionError(
                    "RabbitMQ closed the connection."
                )

            data.extend(chunk)

        return bytes(data)

    # ========================================================
    # Validation
    # ========================================================

    @staticmethod
    def _expect(
        payload: bytes,
        class_id: int,
        method_id: int,
    ) -> None:

        if len(payload) < 4:

            raise RuntimeError(
                "Invalid AMQP method response."
            )

        actual = struct.unpack(
            ">HH",
            payload[:4],
        )

        expected = (
            class_id,
            method_id,
        )

        if actual != expected:

            raise RuntimeError(
                "Unexpected AMQP server response: "
                f"expected={expected} "
                f"actual={actual}"
            )

    # ========================================================
    # AMQP string/table helpers
    # ========================================================

    @staticmethod
    def _shortstr(
        value: str | bytes,
    ) -> bytes:

        raw = (
            value
            if isinstance(value, bytes)
            else value.encode(
                "utf-8"
            )
        )

        if len(raw) > 255:

            raise ValueError(
                "AMQP short string is too long."
            )

        return (
            bytes([len(raw)])
            + raw
        )

    @staticmethod
    def _read_shortstr(
        payload: bytes,
        offset: int,
    ) -> tuple[str, int]:

        if offset >= len(payload):

            raise RuntimeError(
                "Invalid AMQP short string."
            )

        length = payload[offset]

        end = (
            offset
            + 1
            + length
        )

        if end > len(payload):

            raise RuntimeError(
                "Truncated AMQP short string."
            )

        return (
            payload[
                offset + 1:end
            ].decode(
                "utf-8",
                errors="replace",
            ),
            end,
        )

    @staticmethod
    def _longstr(
        raw: bytes,
    ) -> bytes:

        return (
            struct.pack(
                ">I",
                len(raw),
            )
            + raw
        )

    @classmethod
    def _table(
        cls,
        values: dict[str, str],
    ) -> bytes:

        body = b"".join(
            cls._shortstr(key)
            + b"S"
            + cls._longstr(
                value.encode("utf-8")
            )
            for key, value in values.items()
        )

        return (
            struct.pack(
                ">I",
                len(body),
            )
            + body
        )