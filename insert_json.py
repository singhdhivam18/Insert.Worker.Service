from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pyodbc


LOG = logging.getLogger("invoice_insert.database")


HEADER_TABLE = "dbo.vendor_invoice_extracted"
ITEM_TABLE = "dbo.vendor_invoice_items_extracted"


class JsonDataInserter:
    """
    Persists the current Worker-2 extraction JSON into SQL Server.

    Expected input structure:

    {
        "correlation_id": "...",
        "provider_type": "IMAP",
        "file_path": "...",
        "extracted_invoice_line_item": {
            "success": true,
            "data": {
                "... invoice fields ...",
                "line_items": [
                    {
                        "description": "...",
                        "qty": 1,
                        "unit_price": 100,
                        "amount": 100,
                        "hsn": "...",
                        "tax_rate": 18,
                        "t_nt": "T"
                    }
                ]
            },
            "processing_time_ms": 7880,
            "model_used": "expense_rpt",
            "pages": 1,
            "extraction_id": "..."
        }
    }

    Database behavior:

        vendor_invoice_extracted
                1
                |
                | 1 -> many
                |
        vendor_invoice_items_extracted
    """

    DEFAULT_BRANCH_CODE = "BLR"
    DEFAULT_COMP_CODE = "TPL"
    DEFAULT_ACCT_YEAR = "2026-27"
    DEFAULT_PROCESSING_STAGE = "Extracted"

    def __init__(
        self,
        connection_string: str,
    ) -> None:

        if not isinstance(connection_string, str):
            raise TypeError(
                "connection_string must be a string."
            )

        connection_string = connection_string.strip()

        if not connection_string:
            raise ValueError(
                "Database connection string cannot be empty."
            )

        self.connection_string = connection_string

    # =========================================================
    # PUBLIC INSERT
    # =========================================================

    def insert(
        self,
        extracted_json: dict[str, Any],
        requested_by: str = "invoice_insert_worker",
    ) -> int:
        """
        Insert one invoice header and zero or more invoice items.

        Returns:
            Generated vendor_invoice_extracted.id
        """

        if not isinstance(extracted_json, dict):
            raise ValueError(
                "extracted_json must be a JSON object."
            )

        if not isinstance(requested_by, str):
            raise TypeError(
                "requested_by must be a string."
            )

        requested_by = requested_by.strip()

        if not requested_by:
            raise ValueError(
                "requested_by cannot be empty."
            )

        # -----------------------------------------------------
        # 1. Extract top-level envelope
        # -----------------------------------------------------

        correlation_id = self._uuid(
            extracted_json.get("correlation_id")
        )

        provider_type = self._string(
            extracted_json.get("provider_type")
        )

        file_path = self._string(
            extracted_json.get("file_path")
        )

        # These fields are not present in the current JSON,
        # so preserve the existing application defaults.
        branch_code = self.DEFAULT_BRANCH_CODE

        comp_code = self._string(
            extracted_json.get("comp_code")
        )

        if comp_code is None:
            comp_code = self._string(
                extracted_json.get("company_code")
            )

        if comp_code is None:
            comp_code = self.DEFAULT_COMP_CODE

        acct_year = self._string(
            extracted_json.get("acct_year")
        )

        if acct_year is None:
            acct_year = self._string(
                extracted_json.get("accounting_year")
            )

        if acct_year is None:
            acct_year = self.DEFAULT_ACCT_YEAR

        # -----------------------------------------------------
        # 2. Extract provider response + actual invoice data
        # -----------------------------------------------------

        provider_response = self._get_provider_response(
            extracted_json
        )

        invoice_data = self._get_invoice_data(
            provider_response
        )

        line_items = self._get_line_items(
            invoice_data
        )

        # -----------------------------------------------------
        # 3. Validate provider success
        # -----------------------------------------------------

        success = provider_response.get("success")

        if success is False:
            error = provider_response.get("error")

            raise ValueError(
                "Invoice extraction failed: "
                f"{error if error is not None else provider_response}"
            )

        LOG.info(
            "database_invoice_data_prepared "
            "correlation_id=%s "
            "provider_type=%s "
            "branch_code=%s "
            "comp_code=%s "
            "acct_year=%s "
            "line_item_count=%s",
            correlation_id,
            provider_type,
            branch_code,
            comp_code,
            acct_year,
            len(line_items),
        )

        # -----------------------------------------------------
        # 4. Build header INSERT
        # -----------------------------------------------------

        header_sql, header_parameters = (
            self._build_header_insert(
                provider_response=provider_response,
                invoice_data=invoice_data,
                correlation_id=correlation_id,
                acct_year=acct_year,
                branch_code=branch_code,
                comp_code=comp_code,
                file_path=file_path,
                requested_by=requested_by,
            )
        )

        # -----------------------------------------------------
        # 5. Build item INSERT
        # -----------------------------------------------------

        item_sql, item_parameters = (
            self._build_item_insert(
                line_items=line_items,
            )
        )

        connection: pyodbc.Connection | None = None
        cursor: pyodbc.Cursor | None = None

        try:
            # -------------------------------------------------
            # CONNECT
            # -------------------------------------------------

            LOG.info(
                "database_connecting"
            )

            connection = pyodbc.connect(
                self.connection_string,
                timeout=30,
            )

            cursor = connection.cursor()

            LOG.info(
                "database_connected"
            )

            # -------------------------------------------------
            # HEADER INSERT
            # -------------------------------------------------

            LOG.info(
                "database_header_insert_started "
                "table=%s "
                "correlation_id=%s",
                HEADER_TABLE,
                correlation_id,
            )

            cursor.execute(
                header_sql,
                header_parameters,
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "Header insert did not return "
                    "the generated vendor invoice ID."
                )

            if row[0] is None:
                raise RuntimeError(
                    "Header insert returned a NULL "
                    "vendor invoice ID."
                )

            try:
                vendor_invoice_id = int(row[0])
            except (
                TypeError,
                ValueError,
            ) as exc:
                raise RuntimeError(
                    "Header insert returned an invalid "
                    "vendor invoice ID."
                ) from exc

            if vendor_invoice_id <= 0:
                raise RuntimeError(
                    "Generated vendor invoice ID must "
                    "be greater than zero."
                )

            LOG.info(
                "database_header_insert_completed "
                "vendor_invoice_id=%s "
                "correlation_id=%s",
                vendor_invoice_id,
                correlation_id,
            )

            # -------------------------------------------------
            # ITEM INSERTS
            # -------------------------------------------------

            if line_items:
                LOG.info(
                    "database_item_insert_started "
                    "table=%s "
                    "vendor_invoice_id=%s "
                    "item_count=%s",
                    ITEM_TABLE,
                    vendor_invoice_id,
                    len(line_items),
                )

                for index, parameters in enumerate(
                    item_parameters,
                    start=1,
                ):
                    cursor.execute(
                        item_sql,
                        (
                            vendor_invoice_id,
                            *parameters,
                        ),
                    )

                    LOG.info(
                        "database_item_insert_completed "
                        "vendor_invoice_id=%s "
                        "item_number=%s "
                        "item_count=%s",
                        vendor_invoice_id,
                        index,
                        len(line_items),
                    )
            else:
                LOG.info(
                    "database_no_line_items "
                    "vendor_invoice_id=%s",
                    vendor_invoice_id,
                )

            # -------------------------------------------------
            # COMMIT
            # -------------------------------------------------

            connection.commit()

            LOG.info(
                "database_transaction_committed "
                "vendor_invoice_id=%s "
                "correlation_id=%s",
                vendor_invoice_id,
                correlation_id,
            )

            return vendor_invoice_id

        except pyodbc.Error as exc:

            self._rollback(
                connection,
                correlation_id,
            )

            LOG.exception(
                "database_insert_failed "
                "correlation_id=%s",
                correlation_id,
            )

            raise RuntimeError(
                "Database operation failed."
            ) from exc

        except Exception:

            self._rollback(
                connection,
                correlation_id,
            )

            LOG.exception(
                "database_insert_failed "
                "correlation_id=%s",
                correlation_id,
            )

            raise

        finally:

            if cursor is not None:
                try:
                    cursor.close()
                except pyodbc.Error:
                    LOG.warning(
                        "database_cursor_close_failed",
                        exc_info=True,
                    )

            if connection is not None:
                try:
                    connection.close()
                except pyodbc.Error:
                    LOG.warning(
                        "database_connection_close_failed",
                        exc_info=True,
                    )

            LOG.debug(
                "database_connection_closed"
            )

    # =========================================================
    # ROLLBACK
    # =========================================================

    @staticmethod
    def _rollback(
        connection: pyodbc.Connection | None,
        correlation_id: uuid.UUID,
    ) -> None:

        if connection is None:
            return

        try:
            connection.rollback()

            LOG.warning(
                "database_transaction_rolled_back "
                "correlation_id=%s",
                correlation_id,
            )

        except pyodbc.Error:
            LOG.exception(
                "database_rollback_failed"
            )

    # =========================================================
    # PROVIDER RESPONSE
    # =========================================================

    @staticmethod
    def _get_provider_response(
        extracted_json: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Extract:

        extracted_json
            -> extracted_invoice_line_item

        from the current Worker-2 JSON.
        """

        provider_response = extracted_json.get(
            "extracted_invoice_line_item"
        )

        if not isinstance(
            provider_response,
            dict,
        ):
            raise ValueError(
                "Missing or invalid "
                "'extracted_invoice_line_item'."
            )

        return provider_response

    # =========================================================
    # INVOICE DATA
    # =========================================================

    @staticmethod
    def _get_invoice_data(
        provider_response: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Extract:

        extracted_invoice_line_item
            -> data

        This is the actual DocXtract invoice object.
        """

        invoice_data = provider_response.get(
            "data"
        )

        if not isinstance(
            invoice_data,
            dict,
        ):
            raise ValueError(
                "Missing or invalid "
                "'data' inside "
                "'extracted_invoice_line_item'."
            )

        if not invoice_data:
            raise ValueError(
                "Extracted invoice data cannot be empty."
            )

        return invoice_data

    # =========================================================
    # LINE ITEMS
    # =========================================================

    @staticmethod
    def _get_line_items(
        invoice_data: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Extract the current DocXtract line_items array.
        """

        line_items = invoice_data.get(
            "line_items"
        )

        if line_items is None:
            return []

        if not isinstance(
            line_items,
            list,
        ):
            raise ValueError(
                "'line_items' must be a JSON array."
            )

        normalized_items: list[dict[str, Any]] = []

        for index, item in enumerate(
            line_items,
            start=1,
        ):

            if not isinstance(
                item,
                dict,
            ):
                raise ValueError(
                    f"line_items[{index}] must be "
                    "a JSON object."
                )

            normalized_items.append(item)

        return normalized_items

    # =========================================================
    # HEADER INSERT
    # =========================================================

    def _build_header_insert(
        self,
        provider_response: dict[str, Any],
        invoice_data: dict[str, Any],
        correlation_id: uuid.UUID,
        acct_year: str,
        branch_code: str,
        comp_code: str,
        file_path: str | None,
        requested_by: str,
    ) -> tuple[str, tuple[Any, ...]]:
        """
        Build INSERT for dbo.vendor_invoice_extracted.
        """

        sql = f"""
        INSERT INTO {HEADER_TABLE}
        (
            correlation_id,
            acct_year,
            branch_code,
            comp_code,
            module,
            vendor_name,
            vendor_address,
            vendor_gstin,
            vendor_pan,
            vendor_state,
            vendor_registered,
            customer_name,
            customer_gstin,
            invoice_no,
            invoice_type,
            invoice_date,
            due_date,
            document_year,
            csr_no,
            rcm_invoice_no,
            place_of_supply,
            location_of_supply,
            nature_of_supply,
            currency_code,
            sub_total,
            tax_total,
            invoice_total,
            charge_type,
            tds_rate,
            tds_amount,
            tds_tax_amount,
            narration,
            total_pages,
            extraction_id,
            model_used,
            processing_time_ms,
            confidence_score,
            document_path,
            processing_stage,
            total_cgst_amount,
            total_sgst_amount,
            total_igst_amount,
            is_deleted,
            deleted_at,
            deleted_by,
            created_by,
            modified_at,
            modified_by,
            is_ebv_created
        )
        OUTPUT INSERTED.id
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
        """

        parameters = (
            # correlation_id
            correlation_id,

            # acct_year
            acct_year,

            # branch_code
            branch_code,

            # comp_code
            comp_code,

            # module
            self._string(
                self._first(
                    invoice_data,
                    "module",
                )
            ),

            # vendor_name
            self._string(
                self._first(
                    invoice_data,
                    "vendor_name",
                    "vendor",
                    "supplier_name",
                )
            ),

            # vendor_address
            self._string(
                self._first(
                    invoice_data,
                    "vendor_address",
                    "supplier_address",
                )
            ),

            # vendor_gstin
            self._string(
                self._first(
                    invoice_data,
                    "gstin",
                    "vendor_gstin",
                    "supplier_gstin",
                )
            ),

            # vendor_pan
            self._string(
                self._first(
                    invoice_data,
                    "vendor_pan",
                    "pan",
                )
            ),

            # vendor_state
            self._string(
                self._first(
                    invoice_data,
                    "vendor_state",
                    "supplier_state",
                )
            ),

            # vendor_registered
            self._boolean(
                self._first(
                    invoice_data,
                    "is_registered",
                    "vendor_registered",
                    "supplier_registered",
                )
            ),

            # customer_name
            self._string(
                self._first(
                    invoice_data,
                    "client_name",
                    "customer_name",
                    "buyer_name",
                )
            ),

            # customer_gstin
            self._string(
                self._first(
                    invoice_data,
                    "client_gstin",
                    "customer_gstin",
                    "buyer_gstin",
                )
            ),

            # invoice_no
            self._string(
                self._first(
                    invoice_data,
                    "invoice_no",
                    "invoice_number",
                )
            ),

            # invoice_type
            self._string(
                self._first(
                    invoice_data,
                    "invoice_type",
                )
            ),

            # invoice_date
            self._to_date(
                self._first(
                    invoice_data,
                    "invoice_date",
                )
            ),

            # due_date
            self._to_date(
                self._first(
                    invoice_data,
                    "due_date",
                )
            ),

            # document_year
            self._string(
                self._first(
                    invoice_data,
                    "doc_year",
                    "document_year",
                )
            ),

            # csr_no
            self._string(
                self._first(
                    invoice_data,
                    "csr_no",
                )
            ),

            # rcm_invoice_no
            self._string(
                self._first(
                    invoice_data,
                    "rcm_invoice_no",
                )
            ),

            # place_of_supply
            self._string(
                self._first(
                    invoice_data,
                    "place_of_supply",
                )
            ),

            # location_of_supply
            self._string(
                self._first(
                    invoice_data,
                    "los",
                    "location_of_supply",
                )
            ),

            # nature_of_supply
            self._string(
                self._first(
                    invoice_data,
                    "nos",
                    "nature_of_supply",
                )
            ),

            # currency_code
            self._string(
                self._first(
                    invoice_data,
                    "currency",
                    "currency_code",
                )
            ),

            # sub_total
            self._decimal(
                self._first(
                    invoice_data,
                    "sub_total",
                    "subtotal",
                )
            ),

            # tax_total
            self._decimal(
                self._first(
                    invoice_data,
                    "tax_total",
                    "total_tax",
                )
            ),

            # invoice_total
            self._decimal(
                self._first(
                    invoice_data,
                    "total",
                    "invoice_total",
                    "total_amount",
                    "grand_total",
                )
            ),

            # charge_type
            self._string(
                self._first(
                    invoice_data,
                    "charge_type",
                )
            ),

            # tds_rate
            self._decimal(
                self._first(
                    invoice_data,
                    "tds_rate",
                )
            ),

            # tds_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "tds_amount",
                )
            ),

            # tds_tax_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "tds_tax_amount",
                )
            ),

            # narration
            self._string(
                self._first(
                    invoice_data,
                    "narration",
                )
            ),

            # total_pages
            self._integer(
                self._first(
                    provider_response,
                    "pages",
                )
            ),

            # extraction_id
            self._string(
                self._first(
                    provider_response,
                    "extraction_id",
                )
            ),

            # model_used
            self._string(
                self._first(
                    provider_response,
                    "model_used",
                )
            ),

            # processing_time_ms
            self._integer(
                self._first(
                    provider_response,
                    "processing_time_ms",
                )
            ),

            # confidence_score
            # Current DocXtract response does not provide it.
            None,

            # document_path
            file_path,

            # processing_stage
            self.DEFAULT_PROCESSING_STAGE,

            # total_cgst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "Total CGST",
                    "total_cgst_amount",
                    "cgst_total",
                )
            ),

            # total_sgst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "Total SGST Amount",
                    "total_sgst_amount",
                    "sgst_total",
                )
            ),

            # total_igst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "Total IGST Amount",
                    "total_igst_amount",
                    "igst_total",
                )
            ),

            # is_deleted
            False,

            # deleted_at
            None,

            # deleted_by
            None,

            # created_by
            requested_by,

            # modified_at
            None,

            # modified_by
            None,

            # is_ebv_created
            False,
        )

        placeholder_count = sql.count("?")

        if placeholder_count != len(parameters):
            raise RuntimeError(
                "Header SQL parameter count mismatch: "
                f"SQL expects {placeholder_count}, "
                f"parameters={len(parameters)}."
            )

        LOG.debug(
            "header_insert_parameter_count=%s",
            len(parameters),
        )

        return sql, parameters

    # =========================================================
    # ITEM INSERT
    # =========================================================

    def _build_item_insert(
        self,
        line_items: list[dict[str, Any]],
    ) -> tuple[
        str,
        list[tuple[Any, ...]],
    ]:
        """
        Build one parameter tuple for every line item.

        Current DocXtract line-item fields:

            description
            qty
            unit_price
            amount
            hsn
            tax_rate
            t_nt

        Current API does NOT provide line-item-specific:

            cgst_percentage
            cgst_amount
            sgst_percentage
            sgst_amount
            igst_percentage
            igst_amount
            tax_amount
            total_amount

        Those fields therefore remain NULL rather than
        inventing values.
        """

        sql = f"""
        INSERT INTO {ITEM_TABLE}
        (
            vendor_invoice_id,
            description,
            quantity,
            unit_price,
            taxable_amount,
            hsn_code,
            tax_rate,
            t_nt,
            cgst_percentage,
            cgst_amount,
            sgst_percentage,
            sgst_amount,
            igst_percentage,
            igst_amount,
            tax_amount,
            total_amount
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?
        );
        """

        parameters: list[tuple[Any, ...]] = []

        for index, item in enumerate(
            line_items,
            start=1,
        ):

            if not isinstance(item, dict):
                raise ValueError(
                    f"line_items[{index}] must be an object."
                )

            parameters.append(
                (
                    # description
                    self._string(
                        self._first(
                            item,
                            "description",
                        )
                    ),

                    # quantity
                    self._decimal(
                        self._first(
                            item,
                            "qty",
                            "quantity",
                        )
                    ),

                    # unit_price
                    self._decimal(
                        self._first(
                            item,
                            "unit_price",
                        )
                    ),

                    # taxable_amount
                    #
                    # Current DocXtract JSON provides
                    # "amount". In the supplied sample:
                    #
                    # 286778 + 450 = 287228 subtotal
                    #
                    # therefore it is mapped to taxable_amount.
                    self._decimal(
                        self._first(
                            item,
                            "amount",
                            "taxable_amount",
                        )
                    ),

                    # hsn_code
                    self._string(
                        self._first(
                            item,
                            "hsn",
                            "hsn_code",
                        )
                    ),

                    # tax_rate
                    self._decimal(
                        self._first(
                            item,
                            "tax_rate",
                        )
                    ),

                    # t_nt
                    self._string(
                        self._first(
                            item,
                            "t_nt",
                        )
                    ),

                    # cgst_percentage
                    None,

                    # cgst_amount
                    None,

                    # sgst_percentage
                    None,

                    # sgst_amount
                    None,

                    # igst_percentage
                    None,

                    # igst_amount
                    None,

                    # tax_amount
                    None,

                    # total_amount
                    None,
                )
            )

        placeholder_count = sql.count("?")

        if placeholder_count != 16:
            raise RuntimeError(
                "Item SQL placeholder count mismatch: "
                f"expected 16, got {placeholder_count}."
            )

        LOG.debug(
            "item_insert_parameter_count=%s",
            len(parameters),
        )

        return sql, parameters

    # =========================================================
    # FIRST AVAILABLE VALUE
    # =========================================================

    @staticmethod
    def _first(
        data: dict[str, Any],
        *keys: str,
    ) -> Any:

        for key in keys:

            if key in data:

                value = data[key]

                if value is not None:
                    return value

        return None

    # =========================================================
    # STRING
    # =========================================================

    @staticmethod
    def _string(
        value: Any,
    ) -> str | None:

        if value is None:
            return None

        text = str(value).strip()

        return text if text else None

    # =========================================================
    # UUID
    # =========================================================

    @staticmethod
    def _uuid(
        value: Any,
    ) -> uuid.UUID:

        if value is None:
            raise ValueError(
                "correlation_id is required."
            )

        if isinstance(
            value,
            uuid.UUID,
        ):
            return value

        text = str(value).strip()

        if not text:
            raise ValueError(
                "correlation_id cannot be empty."
            )

        try:
            return uuid.UUID(text)

        except ValueError as exc:

            raise ValueError(
                f"Invalid correlation_id UUID: {value!r}"
            ) from exc

    # =========================================================
    # DECIMAL
    # =========================================================

    @staticmethod
    def _decimal(
        value: Any,
    ) -> Decimal | None:

        if value is None:
            return None

        if isinstance(value, str):

            value = value.strip()

            if not value:
                return None

        try:
            return Decimal(str(value))

        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ) as exc:

            raise ValueError(
                f"Invalid decimal value: {value!r}"
            ) from exc

    # =========================================================
    # INTEGER
    # =========================================================

    @staticmethod
    def _integer(
        value: Any,
    ) -> int | None:

        if value is None:
            return None

        if isinstance(value, str):

            value = value.strip()

            if not value:
                return None

        try:
            return int(value)

        except (
            ValueError,
            TypeError,
        ) as exc:

            raise ValueError(
                f"Invalid integer value: {value!r}"
            ) from exc

    # =========================================================
    # BOOLEAN
    # =========================================================

    @staticmethod
    def _boolean(
        value: Any,
    ) -> bool | None:

        if value is None:
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, str):

            text = value.strip().lower()

            if text in {
                "true",
                "1",
                "yes",
            }:
                return True

            if text in {
                "false",
                "0",
                "no",
            }:
                return False

        if isinstance(value, int):

            if value == 1:
                return True

            if value == 0:
                return False

        raise ValueError(
            f"Invalid boolean value: {value!r}"
        )

    # =========================================================
    # DATE
    # =========================================================

    @staticmethod
    def _to_date(
        value: Any,
    ) -> date | None:

        if value is None:
            return None

        if isinstance(
            value,
            datetime,
        ):
            return value.date()

        if isinstance(
            value,
            date,
        ):
            return value

        text = str(value).strip()

        if not text:
            return None

        try:
            return date.fromisoformat(text)

        except ValueError:
            pass

        try:
            return datetime.fromisoformat(text).date()

        except ValueError as exc:

            raise ValueError(
                f"Invalid date value: {value!r}. "
                "Expected YYYY-MM-DD or ISO datetime."
            ) from exc