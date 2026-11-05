"""
Registry of Standard SAP T-Code Specifications
Contains production-ready specifications for widely used SAP reporting T-Codes.
"""

from typing import Dict, List, Optional
from sap_to_pyspark.engine.models import (
    TCodeSpec,
    JoinCondition,
    FilterCondition,
    SelectField,
    AggregationSpec,
)


class TCodeRegistry:
    """Repository of pre-configured enterprise SAP T-Code conversion models."""

    _REGISTRY: Dict[str, TCodeSpec] = {}

    @classmethod
    def register(cls, spec: TCodeSpec) -> None:
        cls._REGISTRY[spec.tcode.upper()] = spec

    @classmethod
    def get(cls, tcode: str) -> Optional[TCodeSpec]:
        return cls._REGISTRY.get(tcode.upper())

    @classmethod
    def list_tcodes(cls) -> List[str]:
        return sorted(list(cls._REGISTRY.keys()))

    @classmethod
    def get_all(cls) -> Dict[str, TCodeSpec]:
        return cls._REGISTRY.copy()


# -------------------------------------------------------------
# 1. VA05: Sales Order List Display
# -------------------------------------------------------------
VA05_SPEC = TCodeSpec(
    tcode="VA05",
    title="List of Sales Orders",
    description="Replicates SAP VA05 / VA05N sales order item reporting with customer and material details.",
    module="SD",
    base_table="VBAK",
    target_table_name="gold_sales_orders_va05",
    joins=[
        JoinCondition(
            left_table="VBAK",
            right_table="VBAP",
            join_keys={"VBELN": "VBELN"},
            join_type="inner",
        ),
        JoinCondition(
            left_table="VBAK",
            right_table="KNA1",
            join_keys={"KUNNR": "KUNNR"},
            join_type="left",
            is_broadcast=True,
        ),
        JoinCondition(
            left_table="VBAP",
            right_table="MAKT",
            join_keys={"MATNR": "MATNR"},
            join_type="left",
            is_broadcast=True,
        ),
    ],
    filters=[
        FilterCondition(table="VBAK", field="VBTYP", operator="=", value="C"),
        FilterCondition(table="MAKT", field="SPRAS", operator="=", value="E"),
    ],
    fields=[
        SelectField(table="VBAK", field="VBELN", alias="sales_order_number"),
        SelectField(table="VBAP", field="POSNR", alias="item_number", cast_type="IntegerType"),
        SelectField(table="VBAK", field="ERDAT", alias="order_date", transform_rule="date_yyyymmdd"),
        SelectField(table="VBAK", field="AUART", alias="order_type"),
        SelectField(table="VBAK", field="VKORG", alias="sales_organization"),
        SelectField(table="VBAK", field="KUNNR", alias="customer_number"),
        SelectField(table="KNA1", field="NAME1", alias="customer_name"),
        SelectField(table="KNA1", field="LAND1", alias="customer_country"),
        SelectField(table="VBAP", field="MATNR", alias="material_number"),
        SelectField(table="MAKT", field="MAKTX", alias="material_description"),
        SelectField(table="VBAP", field="WERKS", alias="plant"),
        SelectField(table="VBAP", field="KWMENG", alias="order_quantity", cast_type="DecimalType(15,3)"),
        SelectField(table="VBAP", field="VRKME", alias="sales_unit"),
        SelectField(table="VBAP", field="NETPR", alias="net_price", cast_type="DecimalType(11,2)"),
        SelectField(table="VBAP", field="NETWR", alias="item_net_amount", cast_type="DecimalType(15,2)"),
        SelectField(table="VBAK", field="WAERK", alias="currency"),
    ],
    partition_by=["order_date"],
    target_format="delta",
)
TCodeRegistry.register(VA05_SPEC)


# -------------------------------------------------------------
# 2. FBL3N: G/L Account Line Item Display
# -------------------------------------------------------------
FBL3N_SPEC = TCodeSpec(
    tcode="FBL3N",
    title="G/L Account Line Item Display",
    description="Replicates SAP FBL3N / FAGLL03 general ledger line items with debit/credit sign resolution and G/L texts.",
    module="FI",
    base_table="BKPF",
    target_table_name="gold_gl_line_items_fbl3n",
    joins=[
        JoinCondition(
            left_table="BKPF",
            right_table="BSEG",
            join_keys={"BUKRS": "BUKRS", "BELNR": "BELNR", "GJAHR": "GJAHR"},
            join_type="inner",
        ),
        JoinCondition(
            left_table="BSEG",
            right_table="SKAT",
            join_keys={"HKONT": "SAKNR"},
            join_type="left",
            is_broadcast=True,
        ),
        JoinCondition(
            left_table="BSEG",
            right_table="KNA1",
            join_keys={"KUNNR": "KUNNR"},
            join_type="left",
            is_broadcast=True,
        ),
        JoinCondition(
            left_table="BSEG",
            right_table="LFA1",
            join_keys={"LIFNR": "LIFNR"},
            join_type="left",
            is_broadcast=True,
        ),
    ],
    filters=[
        FilterCondition(table="SKAT", field="SPRAS", operator="=", value="E"),
    ],
    fields=[
        SelectField(table="BKPF", field="BUKRS", alias="company_code"),
        SelectField(table="BKPF", field="BELNR", alias="document_number"),
        SelectField(table="BKPF", field="GJAHR", alias="fiscal_year", cast_type="IntegerType"),
        SelectField(table="BSEG", field="BUZEI", alias="line_item", cast_type="IntegerType"),
        SelectField(table="BKPF", field="BLDAT", alias="document_date", transform_rule="date_yyyymmdd"),
        SelectField(table="BKPF", field="BUDAT", alias="posting_date", transform_rule="date_yyyymmdd"),
        SelectField(table="BKPF", field="BLART", alias="document_type"),
        SelectField(table="BSEG", field="HKONT", alias="gl_account"),
        SelectField(table="SKAT", field="TXT50", alias="gl_account_name"),
        SelectField(table="BSEG", field="BSCHL", alias="posting_key"),
        SelectField(table="BSEG", field="SHKZG", alias="debit_credit_code"),
        SelectField(
            table="BSEG",
            field="DMBTR",
            alias="signed_amount_local",
            transform_rule="sign_shkzg",
            cast_type="DecimalType(13,2)",
        ),
        SelectField(table="BKPF", field="WAERS", alias="currency"),
        SelectField(table="BSEG", field="KOSTL", alias="cost_center"),
        SelectField(table="BSEG", field="PRCTR", alias="profit_center"),
        SelectField(table="BSEG", field="SGTXT", alias="line_item_text"),
    ],
    partition_by=["fiscal_year", "company_code"],
    target_format="delta",
)
TCodeRegistry.register(FBL3N_SPEC)


# -------------------------------------------------------------
# 3. MB5B: Stock on Posting Date
# -------------------------------------------------------------
MB5B_SPEC = TCodeSpec(
    tcode="MB5B",
    title="Stock on Posting Date",
    description="Replicates SAP MB5B stock movements analysis, computing receipts, issues, and valuated stock balances.",
    module="MM",
    base_table="MSEG",
    target_table_name="gold_inventory_movements_mb5b",
    joins=[
        JoinCondition(
            left_table="MSEG",
            right_table="MKPF",
            join_keys={"MBLNR": "MBLNR", "MJAHR": "MJAHR"},
            join_type="inner",
        ),
        JoinCondition(
            left_table="MSEG",
            right_table="MAKT",
            join_keys={"MATNR": "MATNR"},
            join_type="left",
            is_broadcast=True,
        ),
    ],
    filters=[
        FilterCondition(table="MAKT", field="SPRAS", operator="=", value="E"),
    ],
    fields=[
        SelectField(table="MSEG", field="WERKS", alias="plant"),
        SelectField(table="MSEG", field="LGORT", alias="storage_location"),
        SelectField(table="MSEG", field="MATNR", alias="material_number"),
        SelectField(table="MAKT", field="MAKTX", alias="material_description"),
        SelectField(table="MSEG", field="MBLNR", alias="material_document"),
        SelectField(table="MKPF", field="BUDAT", alias="posting_date", transform_rule="date_yyyymmdd"),
        SelectField(table="MSEG", field="BWART", alias="movement_type"),
        SelectField(table="MSEG", field="SHKZG", alias="debit_credit_code"),
        SelectField(
            table="MSEG",
            field="MENGE",
            alias="movement_quantity",
            transform_rule="sign_shkzg",
            cast_type="DecimalType(13,3)",
        ),
        SelectField(table="MSEG", field="MEINS", alias="base_unit"),
        SelectField(
            table="MSEG",
            field="DMBTR",
            alias="movement_amount_local",
            transform_rule="sign_shkzg",
            cast_type="DecimalType(13,2)",
        ),
        SelectField(table="MSEG", field="WAERS", alias="currency"),
    ],
    aggregations=AggregationSpec(
        group_by=["plant", "storage_location", "material_number", "material_description", "base_unit"],
        aggregations={
            "total_net_quantity": "sum(movement_quantity)",
            "total_net_amount": "sum(movement_amount_local)",
        },
    ),
    partition_by=["plant"],
    target_format="delta",
)
TCodeRegistry.register(MB5B_SPEC)


# -------------------------------------------------------------
# 4. ME2M: Purchasing Documents by Material
# -------------------------------------------------------------
ME2M_SPEC = TCodeSpec(
    tcode="ME2M",
    title="Purchasing Documents by Material",
    description="Replicates SAP ME2M purchase order items list with vendor and pricing details.",
    module="MM",
    base_table="EKPO",
    target_table_name="gold_po_by_material_me2m",
    joins=[
        JoinCondition(
            left_table="EKPO",
            right_table="EKKO",
            join_keys={"EBELN": "EBELN"},
            join_type="inner",
        ),
        JoinCondition(
            left_table="EKKO",
            right_table="LFA1",
            join_keys={"LIFNR": "LIFNR"},
            join_type="left",
            is_broadcast=True,
        ),
        JoinCondition(
            left_table="EKPO",
            right_table="MAKT",
            join_keys={"MATNR": "MATNR"},
            join_type="left",
            is_broadcast=True,
        ),
    ],
    filters=[
        FilterCondition(table="MAKT", field="SPRAS", operator="=", value="E"),
        FilterCondition(table="EKPO", field="LOEKZ", operator="IS NULL", value=None),
    ],
    fields=[
        SelectField(table="EKPO", field="EBELN", alias="purchase_order"),
        SelectField(table="EKPO", field="EBELP", alias="po_item", cast_type="IntegerType"),
        SelectField(table="EKKO", field="AEDAT", alias="po_date", transform_rule="date_yyyymmdd"),
        SelectField(table="EKKO", field="BUKRS", alias="company_code"),
        SelectField(table="EKKO", field="LIFNR", alias="vendor_number"),
        SelectField(table="LFA1", field="NAME1", alias="vendor_name"),
        SelectField(table="EKPO", field="MATNR", alias="material_number"),
        SelectField(table="MAKT", field="MAKTX", alias="material_description"),
        SelectField(table="EKPO", field="WERKS", alias="plant"),
        SelectField(table="EKPO", field="LGORT", alias="storage_location"),
        SelectField(table="EKPO", field="MENGE", alias="po_quantity", cast_type="DecimalType(13,3)"),
        SelectField(table="EKPO", field="MEINS", alias="po_unit"),
        SelectField(table="EKPO", field="NETPR", alias="net_price", cast_type="DecimalType(11,2)"),
        SelectField(table="EKPO", field="NETWR", alias="net_order_value", cast_type="DecimalType(13,2)"),
        SelectField(table="EKKO", field="WAERS", alias="currency"),
    ],
    partition_by=["company_code"],
    target_format="delta",
)
TCodeRegistry.register(ME2M_SPEC)


# -------------------------------------------------------------
# 5. COOIS: Production Order Operational Overview
# -------------------------------------------------------------
COOIS_SPEC = TCodeSpec(
    tcode="COOIS",
    title="Production Order Operational Overview",
    description="Replicates SAP COOIS production orders tracking with planned dates and target quantities.",
    module="PP",
    base_table="AUFK",
    target_table_name="gold_production_orders_coois",
    joins=[
        JoinCondition(
            left_table="AUFK",
            right_table="AFKO",
            join_keys={"AUFNR": "AUFNR"},
            join_type="inner",
        ),
        JoinCondition(
            left_table="AFKO",
            right_table="MAKT",
            join_keys={"PLNBEZ": "MATNR"},
            join_type="left",
            is_broadcast=True,
        ),
    ],
    filters=[
        FilterCondition(table="AUFK", field="AUTYP", operator="=", value=10),
        FilterCondition(table="MAKT", field="SPRAS", operator="=", value="E"),
    ],
    fields=[
        SelectField(table="AUFK", field="AUFNR", alias="production_order"),
        SelectField(table="AUFK", field="AUFART", alias="order_type"),
        SelectField(table="AUFK", field="WERKS", alias="plant"),
        SelectField(table="AUFK", field="BUKRS", alias="company_code"),
        SelectField(table="AFKO", field="PLNBEZ", alias="produced_material"),
        SelectField(table="MAKT", field="MAKTX", alias="material_description"),
        SelectField(table="AFKO", field="GSTRP", alias="basic_start_date", transform_rule="date_yyyymmdd"),
        SelectField(table="AFKO", field="GLTRP", alias="basic_finish_date", transform_rule="date_yyyymmdd"),
        SelectField(table="AFKO", field="GAMNG", alias="target_quantity", cast_type="DecimalType(13,3)"),
        SelectField(table="AFKO", field="GMEIN", alias="order_unit"),
        SelectField(table="AUFK", field="KTEXT", alias="order_description"),
    ],
    partition_by=["plant"],
    target_format="delta",
)
TCodeRegistry.register(COOIS_SPEC)
