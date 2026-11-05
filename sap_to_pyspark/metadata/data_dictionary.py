"""
SAP Data Dictionary Query & Translation Service
Provides lookups for tables, fields, data types, and German-to-English naming mappings.
"""

from typing import Dict, List, Optional, Tuple
from sap_to_pyspark.metadata.tables import SAP_TABLES, SAPTableInfo, SAPFieldInfo


class SAPDataDictionary:
    """Central repository and helper for SAP Data Dictionary definitions."""

    # Default common join paths between classic SAP tables
    COMMON_JOIN_KEYS: Dict[Tuple[str, str], List[str]] = {
        ("VBAK", "VBAP"): ["VBELN"],
        ("VBAP", "VBAK"): ["VBELN"],
        ("VBAK", "KNA1"): ["KUNNR"],
        ("VBAP", "MAKT"): ["MATNR"],
        ("VBAP", "MARA"): ["MATNR"],
        ("VBAP", "MARC"): ["MATNR", "WERKS"],
        ("VBAP", "MARD"): ["MATNR", "WERKS", "LGORT"],
        ("EKKO", "EKPO"): ["EBELN"],
        ("EKPO", "EKKO"): ["EBELN"],
        ("EKKO", "LFA1"): ["LIFNR"],
        ("EKPO", "MAKT"): ["MATNR"],
        ("EKPO", "MARA"): ["MATNR"],
        ("BKPF", "BSEG"): ["BUKRS", "BELNR", "GJAHR"],
        ("BSEG", "BKPF"): ["BUKRS", "BELNR", "GJAHR"],
        ("BSEG", "SKAT"): ["HKONT"],
        ("BSEG", "KNA1"): ["KUNNR"],
        ("BSEG", "LFA1"): ["LIFNR"],
        ("MKPF", "MSEG"): ["MBLNR", "MJAHR"],
        ("MSEG", "MKPF"): ["MBLNR", "MJAHR"],
        ("MSEG", "MAKT"): ["MATNR"],
        ("MSEG", "MARD"): ["MATNR", "WERKS", "LGORT"],
        ("AUFK", "AFKO"): ["AUFNR"],
        ("AFKO", "AUFK"): ["AUFNR"],
    }

    # Universal field alias defaults (when table is not specified)
    GLOBAL_FIELD_ALIASES: Dict[str, str] = {
        "MANDT": "client_id",
        "VBELN": "sales_document_number",
        "POSNR": "item_number",
        "EBELN": "purchase_order_number",
        "EBELP": "po_item_number",
        "BELNR": "accounting_doc_number",
        "GJAHR": "fiscal_year",
        "BUZEI": "line_item_number",
        "BUKRS": "company_code",
        "WERKS": "plant_code",
        "LGORT": "storage_location",
        "MATNR": "material_number",
        "KUNNR": "customer_number",
        "LIFNR": "vendor_number",
        "VKORG": "sales_org",
        "VTWEG": "distribution_channel",
        "SPART": "division",
        "ERDAT": "creation_date",
        "AEDAT": "change_date",
        "BLDAT": "document_date",
        "BUDAT": "posting_date",
        "NETWR": "net_amount",
        "DMBTR": "amount_local_currency",
        "WRBTR": "amount_document_currency",
        "WAERS": "currency_code",
        "WAERK": "doc_currency_code",
        "MENGE": "quantity",
        "KWMENG": "order_quantity",
        "MEINS": "base_unit_of_measure",
        "VRKME": "sales_unit_of_measure",
        "SHKZG": "debit_credit_indicator",
        "BWART": "movement_type",
        "AUFNR": "order_number",
        "KOSTL": "cost_center",
        "PRCTR": "profit_center",
        "SAKNR": "gl_account_number",
        "HKONT": "gl_account_number",
        "RACCT": "gl_account_number",
    }

    @classmethod
    def get_table(cls, table_name: str) -> Optional[SAPTableInfo]:
        """Returns metadata for an SAP table."""
        return SAP_TABLES.get(table_name.upper().strip())

    @classmethod
    def list_tables(cls) -> List[str]:
        """Returns all registered SAP tables."""
        return sorted(list(SAP_TABLES.keys()))

    @classmethod
    def get_field_info(cls, table_name: str, field_name: str) -> Optional[SAPFieldInfo]:
        """Returns metadata for a specific field in a table."""
        tbl = cls.get_table(table_name)
        if tbl:
            return tbl.get_field(field_name)
        return None

    @classmethod
    def suggest_english_alias(cls, table_name: Optional[str], field_name: str) -> str:
        """Suggests a clean English snake_case alias for an SAP field."""
        fname = field_name.upper().strip()
        if table_name:
            tbl = cls.get_table(table_name)
            if tbl:
                finfo = tbl.get_field(fname)
                if finfo and finfo.english_name:
                    return finfo.english_name

        if fname in cls.GLOBAL_FIELD_ALIASES:
            return cls.GLOBAL_FIELD_ALIASES[fname]

        return fname.lower()

    @classmethod
    def suggest_join_keys(cls, left_table: str, right_table: str) -> List[str]:
        """Finds known standard join keys between two SAP tables."""
        pair = (left_table.upper().strip(), right_table.upper().strip())
        if pair in cls.COMMON_JOIN_KEYS:
            return cls.COMMON_JOIN_KEYS[pair]

        # Check overlapping primary keys
        left_info = cls.get_table(left_table)
        right_info = cls.get_table(right_table)
        if left_info and right_info:
            overlap = [k for k in left_info.primary_keys if k in right_info.primary_keys]
            if overlap:
                return overlap
            # Check fields with same name that are key in either
            matching_fields = [k for k in left_info.fields if k in right_info.fields and k not in ("MANDT",)]
            if matching_fields:
                return matching_fields[:1]

        return []
