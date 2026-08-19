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
    """Persists extracted invoice data into SQL Server."""

    DEFAULT_BRANCH_CODE = "BLR"

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
    #PUBLIC INSERT
    def insert(
        self,
        extracted_json: dict[str, Any],
        requested_by: str = "invoice_insert_worker",
    ) -> int:
        """
        Insert one invoice header and one invoice item.

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
        #Extract envelope values
        correlation_id = self._uuid(
            extracted_json.get("correlation_id")
        )

        provider_type = self._string(
            extracted_json.get("provider_type")
        )

        file_path = self._string(
            extracted_json.get("file_path")
        )

        branch_code = self.DEFAULT_BRANCH_CODE

        comp_code = self._string(
            extracted_json.get("comp_code")
        )

        if comp_code is None:
            comp_code = self._string(
                extracted_json.get("company_code")
            )

        if comp_code is None:
            comp_code = "TPL"

        acct_year = self._string(
            extracted_json.get("acct_year")
        )

        if acct_year is None:
            acct_year = self._string(
                extracted_json.get("accounting_year")
            )

        if acct_year is None:
            acct_year = "2026-27"
        #Extract invoice data
        invoice_data = self._get_invoice_data(
            extracted_json
        )

        LOG.info(
            "database_invoice_data_prepared "
            "correlation_id=%s "
            "provider_type=%s "
            "branch_code=%s "
            "comp_code=%s "
            "acct_year=%s",
            correlation_id,
            provider_type,
            branch_code,
            comp_code,
            acct_year,
        )
        #Build SQL
        header_sql, header_parameters = (
            self._build_header_insert(
                extracted_json=extracted_json,
                invoice_data=invoice_data,
                correlation_id=correlation_id,
                acct_year=acct_year,
                branch_code=branch_code,
                comp_code=comp_code,
                file_path=file_path,
                requested_by=requested_by,
            )
        )

        item_sql, item_parameters = (
            self._build_item_insert(
                invoice_data
            )
        )

        connection: pyodbc.Connection | None = None
        cursor: pyodbc.Cursor | None = None

        try:
            """connect"""
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
            """HEADER INSERT"""
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
            #OUTPUT INSERTED.id
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
            #ITEM INSERT
            LOG.info(
                "database_item_insert_started "
                "table=%s "
                "vendor_invoice_id=%s",
                ITEM_TABLE,
                vendor_invoice_id,
            )

            cursor.execute(
                item_sql,
                (
                    vendor_invoice_id,
                    *item_parameters,
                ),
            )

            LOG.info(
                "database_item_insert_completed "
                "vendor_invoice_id=%s",
                vendor_invoice_id,
            )
            #commit
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
    """ROLL BACKs"""
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

    #extracted data
    @staticmethod
    def _get_invoice_data(
        extracted_json: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Extract actual invoice object.

        Expected current structure:

        {
            "extracted_invoice_line_item": {
                "line_items": {
                    ...
                }
            }
        }
        """

        extracted_invoice_line_item = (
            extracted_json.get(
                "extracted_invoice_line_item"
            )
        )

        if not isinstance(
            extracted_invoice_line_item,
            dict,
        ):
            raise ValueError(
                "Missing or invalid "
                "'extracted_invoice_line_item'."
            )

        invoice_data = (
            extracted_invoice_line_item.get(
                "line_items"
            )
        )

        if not isinstance(
            invoice_data,
            dict,
        ):
            raise ValueError(
                "Missing or invalid 'line_items' "
                "inside 'extracted_invoice_line_item'."
            )

        if not invoice_data:
            raise ValueError(
                "Extracted invoice data cannot be empty."
            )

        return invoice_data

    """Header Insert"""
    def _build_header_insert(
        self,
        extracted_json: dict[str, Any],
        invoice_data: dict[str, Any],
        correlation_id: uuid.UUID,
        acct_year: str,
        branch_code: str,
        comp_code: str,
        file_path: str | None,
        requested_by: str,
    ) -> tuple[str, tuple[Any, ...]]:
        """Build INSERT for vendor_invoice_extracted."""

        sql = f"""
        INSERT INTO {HEADER_TABLE}
        (
            correlation_id,acct_year,branch_code,comp_code,module,vendor_name,vendor_address,vendor_gstin,
            vendor_pan,vendor_state,vendor_registered,customer_name,customer_gstin,invoice_no,invoice_type,invoice_date,due_date,
            document_year,csr_no,rcm_invoice_no,place_of_supply,location_of_supply,nature_of_supply,currency_code,sub_total,
            tax_total,invoice_total,charge_type,tds_rate,tds_amount,tds_tax_amount,narration,total_pages,extraction_id,
            model_used,processing_time_ms,confidence_score,document_path,processing_stage,total_cgst_amount,
            total_sgst_amount,total_igst_amount,is_deleted,deleted_at,deleted_by,
            created_by,modified_at,modified_by,is_ebv_created
        )
        OUTPUT INSERTED.id
        VALUES
        (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
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
                    "vendor_gstin",
                    "supplier_gstin",
                    "gstin",
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
                    "vendor_registered",
                    "supplier_registered",
                )
            ),
            # customer_name
            self._string(
                self._first(
                    invoice_data,
                    "customer_name",
                    "buyer_name",
                )
            ),
            # customer_gstin
            self._string(
                self._first(
                    invoice_data,
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
                    "location_of_supply",
                )
            ),
            # nature_of_supply
            self._string(
                self._first(
                    invoice_data,
                    "nature_of_supply",
                )
            ),

            # currency_code
            self._string(
                self._first(
                    invoice_data,
                    "currency_code",
                    "currency",
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
                    invoice_data,
                    "total_pages",
                )
            ),
            # extraction_id
            self._string(
                self._first(
                    extracted_json,
                    "extraction_id",
                )
            ),
            # model_used
            self._string(
                self._first(
                    extracted_json,
                    "model_used",
                )
            ),
            # processing_time_ms
            self._integer(
                self._first(
                    extracted_json,
                    "processing_time_ms",
                )
            ),
            # confidence_score
            self._decimal(
                self._first(
                    extracted_json,
                    "confidence_score",
                )
            ),
            # document_path
            file_path,
            # processing_stage
            self.DEFAULT_PROCESSING_STAGE,
            # total_cgst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "total_cgst_amount",
                    "cgst_total",
                )
            ),
            # total_sgst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "total_sgst_amount",
                    "sgst_total",
                )
            ),
            # total_igst_amount
            self._decimal(
                self._first(
                    invoice_data,
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
    """item insert"""
    def _build_item_insert(
        self,
        invoice_data: dict[str, Any],
    ) -> tuple[str, tuple[Any, ...]]:
        """
        Build INSERT for vendor_invoice_items_extracted.

        vendor_invoice_id is intentionally excluded from the
        parameters returned here.

        insert() obtains the generated header ID and prepends it
        before executing this SQL.
        """

        sql = f"""
        INSERT INTO {ITEM_TABLE}
        (
            vendor_invoice_id,description,quantity,unit_price,taxable_amount,hsn_code,tax_rate,t_nt,cgst_percentage,cgst_amount,
            sgst_percentage,sgst_amount,igst_percentage,igst_amount,tax_amount,total_amount
        )
        VALUES
        (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        );
        """

        parameters = (
            # description
            self._string(
                self._first(
                    invoice_data,
                    "description",
                )
            ),

            # quantity
            self._decimal(
                self._first(
                    invoice_data,
                    "quantity",
                )
            ),

            # unit_price
            self._decimal(
                self._first(
                    invoice_data,
                    "unit_price",
                )
            ),

            # taxable_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "taxable_amount",
                )
            ),

            # hsn_code
            self._string(
                self._first(
                    invoice_data,
                    "hsn_code",
                    "hsn",
                )
            ),

            # tax_rate
            self._decimal(
                self._first(
                    invoice_data,
                    "tax_rate",
                )
            ),

            # t_nt
            self._string(
                self._first(
                    invoice_data,
                    "t_nt",
                )
            ),

            # cgst_percentage
            self._decimal(
                self._first(
                    invoice_data,
                    "cgst_percentage",
                )
            ),

            # cgst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "cgst_amount",
                )
            ),

            # sgst_percentage
            self._decimal(
                self._first(
                    invoice_data,
                    "sgst_percentage",
                )
            ),

            # sgst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "sgst_amount",
                )
            ),

            # igst_percentage
            self._decimal(
                self._first(
                    invoice_data,
                    "igst_percentage",
                )
            ),

            # igst_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "igst_amount",
                )
            ),

            # tax_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "tax_amount",
                )
            ),

            # total_amount
            self._decimal(
                self._first(
                    invoice_data,
                    "total_amount",
                )
            ),
        )

        placeholder_count = sql.count("?")

        expected_count = len(parameters) + 1

        if placeholder_count != expected_count:
            raise RuntimeError(
                "Item SQL parameter count mismatch: "
                f"SQL expects {placeholder_count}, "
                f"parameters={expected_count}."
            )

        LOG.debug(
            "item_insert_parameter_count=%s",
            expected_count,
        )

        return sql, parameters

    # ========================================================
    # FIRST AVAILABLE VALUE
    # ========================================================

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

    # ========================================================
    # STRING
    # ========================================================

    @staticmethod
    def _string(
        value: Any,
    ) -> str | None:

        if value is None:
            return None

        text = str(value).strip()

        return text if text else None

    # ========================================================
    # UUID
    # ========================================================

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

            # Handles both:
            #
            # c6c60f4e9d224e748bc70052a1039334
            #
            # and:
            #
            # c6c60f4e-9d22-4e74-8bc7-0052a1039334
            return uuid.UUID(text)

        except ValueError as exc:

            raise ValueError(
                f"Invalid correlation_id UUID: {value!r}"
            ) from exc

    # ========================================================
    # DECIMAL
    # ========================================================

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

            return Decimal(
                str(value)
            )

        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ) as exc:

            raise ValueError(
                f"Invalid decimal value: {value!r}"
            ) from exc
    """interger"""
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
    #Boolean
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
    """date"""
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

            return date.fromisoformat(
                text
            )

        except ValueError:
            pass

        try:

            return datetime.fromisoformat(
                text
            ).date()

        except ValueError as exc:

            raise ValueError(
                f"Invalid date value: {value!r}. "
                "Expected YYYY-MM-DD or ISO datetime."
            ) from exc