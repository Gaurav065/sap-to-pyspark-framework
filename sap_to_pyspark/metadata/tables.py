"""
SAP Table Definitions & Field Metadata
Comprehensive catalog of core SAP ECC / S4HANA tables across SD, MM, FI, and PP modules.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class SAPFieldInfo:
    field_name: str
    sap_type: str  # CHAR, NUMC, DATS, TIMS, CURR, QUAN, DEC, INT4
    length: int
    decimals: int = 0
    english_name: str = ""
    description: str = ""
    is_key: bool = False
    spark_type: str = "StringType"
    format_rule: Optional[str] = None  # e.g., 'date_yyyymmdd', 'sign_shkzg', 'currency_shift'


@dataclass
class SAPTableInfo:
    table_name: str
    module: str  # SD, MM, FI, CO, PP, Master
    description: str
    primary_keys: List[str]
    fields: Dict[str, SAPFieldInfo] = field(default_factory=dict)

    def get_field(self, name: str) -> Optional[SAPFieldInfo]:
        return self.fields.get(name.upper())


# Catalog of core tables
SAP_TABLES: Dict[str, SAPTableInfo] = {
    "VBAK": SAPTableInfo(
        table_name="VBAK",
        module="SD",
        description="Sales Document: Header Data",
        primary_keys=["VBELN"],
        fields={
            "VBELN": SAPFieldInfo("VBELN", "CHAR", 10, english_name="sales_document", description="Sales and Distribution Document Number", is_key=True),
            "ERDAT": SAPFieldInfo("ERDAT", "DATS", 8, english_name="creation_date", description="Date on which the record was created", spark_type="DateType", format_rule="date_yyyymmdd"),
            "ERZET": SAPFieldInfo("ERZET", "TIMS", 6, english_name="creation_time", description="Entry time"),
            "ERNAM": SAPFieldInfo("ERNAM", "CHAR", 12, english_name="created_by", description="Name of Person who Created the Object"),
            "ANGDT": SAPFieldInfo("ANGDT", "DATS", 8, english_name="quotation_valid_from", description="Quotation valid from", spark_type="DateType", format_rule="date_yyyymmdd"),
            "BNDDT": SAPFieldInfo("BNDDT", "DATS", 8, english_name="quotation_valid_to", description="Quotation valid to", spark_type="DateType", format_rule="date_yyyymmdd"),
            "AUDAT": SAPFieldInfo("AUDAT", "DATS", 8, english_name="document_date", description="Document Date", spark_type="DateType", format_rule="date_yyyymmdd"),
            "VBTYP": SAPFieldInfo("VBTYP", "CHAR", 1, english_name="document_category", description="SD document category (C=Order, H=Returns)"),
            "AUART": SAPFieldInfo("AUART", "CHAR", 4, english_name="sales_doc_type", description="Sales Document Type"),
            "NETWR": SAPFieldInfo("NETWR", "CURR", 15, decimals=2, english_name="net_value", description="Net Value of the Sales Order", spark_type="DecimalType(15,2)"),
            "WAERK": SAPFieldInfo("WAERK", "CHAR", 5, english_name="doc_currency", description="SD document currency"),
            "VKORG": SAPFieldInfo("VKORG", "CHAR", 4, english_name="sales_org", description="Sales Organization"),
            "VTWEG": SAPFieldInfo("VTWEG", "CHAR", 2, english_name="distr_channel", description="Distribution Channel"),
            "SPART": SAPFieldInfo("SPART", "CHAR", 2, english_name="division", description="Division"),
            "KUNNR": SAPFieldInfo("KUNNR", "CHAR", 10, english_name="sold_to_party", description="Sold-to party / Customer Number"),
            "LIFSK": SAPFieldInfo("LIFSK", "CHAR", 2, english_name="delivery_block", description="Delivery block (document header)"),
            "FAKSK": SAPFieldInfo("FAKSK", "CHAR", 2, english_name="billing_block", description="Billing block in SD document"),
        }
    ),
    "VBAP": SAPTableInfo(
        table_name="VBAP",
        module="SD",
        description="Sales Document: Item Data",
        primary_keys=["VBELN", "POSNR"],
        fields={
            "VBELN": SAPFieldInfo("VBELN", "CHAR", 10, english_name="sales_document", description="Sales Document Number", is_key=True),
            "POSNR": SAPFieldInfo("POSNR", "NUMC", 6, english_name="sales_item", description="Sales Document Item", is_key=True, spark_type="IntegerType"),
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number"),
            "MATKL": SAPFieldInfo("MATKL", "CHAR", 9, english_name="material_group", description="Material Group"),
            "ARKTX": SAPFieldInfo("ARKTX", "CHAR", 40, english_name="item_description", description="Short Text for Sales Order Item"),
            "KWMENG": SAPFieldInfo("KWMENG", "QUAN", 15, decimals=3, english_name="order_quantity", description="Cumulative Order Quantity", spark_type="DecimalType(15,3)"),
            "VRKME": SAPFieldInfo("VRKME", "CHAR", 3, english_name="sales_unit", description="Sales unit of measure"),
            "NETPR": SAPFieldInfo("NETPR", "CURR", 11, decimals=2, english_name="net_price", description="Net price", spark_type="DecimalType(11,2)"),
            "NETWR": SAPFieldInfo("NETWR", "CURR", 15, decimals=2, english_name="item_net_value", description="Net value of the item", spark_type="DecimalType(15,2)"),
            "WAERK": SAPFieldInfo("WAERK", "CHAR", 5, english_name="item_currency", description="SD document currency"),
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant (Own or External)"),
            "LGORT": SAPFieldInfo("LGORT", "CHAR", 4, english_name="storage_location", description="Storage Location"),
            "PSTYV": SAPFieldInfo("PSTYV", "CHAR", 4, english_name="item_category", description="Sales document item category"),
            "ABGRU": SAPFieldInfo("ABGRU", "CHAR", 2, english_name="rejection_reason", description="Reason for rejection"),
        }
    ),
    "KNA1": SAPTableInfo(
        table_name="KNA1",
        module="Master",
        description="General Data in Customer Master",
        primary_keys=["KUNNR"],
        fields={
            "KUNNR": SAPFieldInfo("KUNNR", "CHAR", 10, english_name="customer_number", description="Customer Number", is_key=True),
            "LAND1": SAPFieldInfo("LAND1", "CHAR", 3, english_name="country_key", description="Country Key"),
            "NAME1": SAPFieldInfo("NAME1", "CHAR", 35, english_name="customer_name", description="Name 1"),
            "NAME2": SAPFieldInfo("NAME2", "CHAR", 35, english_name="customer_name_2", description="Name 2"),
            "ORT01": SAPFieldInfo("ORT01", "CHAR", 35, english_name="city", description="City"),
            "PSTLZ": SAPFieldInfo("PSTLZ", "CHAR", 10, english_name="postal_code", description="Postal Code"),
            "REGIO": SAPFieldInfo("REGIO", "CHAR", 3, english_name="region", description="Region (State, Province, County)"),
            "STRAS": SAPFieldInfo("STRAS", "CHAR", 35, english_name="street", description="House number and street"),
            "TELF1": SAPFieldInfo("TELF1", "CHAR", 16, english_name="telephone_1", description="First telephone number"),
            "KTOKD": SAPFieldInfo("KTOKD", "CHAR", 4, english_name="account_group", description="Customer Account Group"),
        }
    ),
    "MAKT": SAPTableInfo(
        table_name="MAKT",
        module="Master",
        description="Material Descriptions",
        primary_keys=["MATNR", "SPRAS"],
        fields={
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number", is_key=True),
            "SPRAS": SAPFieldInfo("SPRAS", "CHAR", 1, english_name="language_key", description="Language Key", is_key=True),
            "MAKTX": SAPFieldInfo("MAKTX", "CHAR", 40, english_name="material_description", description="Material description in target language"),
            "MAKTG": SAPFieldInfo("MAKTG", "CHAR", 40, english_name="material_desc_upper", description="Material description in capital letters"),
        }
    ),
    "MARA": SAPTableInfo(
        table_name="MARA",
        module="Master",
        description="General Material Data",
        primary_keys=["MATNR"],
        fields={
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number", is_key=True),
            "ERSDA": SAPFieldInfo("ERSDA", "DATS", 8, english_name="created_date", description="Created On", spark_type="DateType", format_rule="date_yyyymmdd"),
            "ERNAM": SAPFieldInfo("ERNAM", "CHAR", 12, english_name="created_by", description="Created By"),
            "MTART": SAPFieldInfo("MTART", "CHAR", 4, english_name="material_type", description="Material Type (FERT, ROH, HALB)"),
            "MATKL": SAPFieldInfo("MATKL", "CHAR", 9, english_name="material_group", description="Material Group"),
            "MEINS": SAPFieldInfo("MEINS", "CHAR", 3, english_name="base_uom", description="Base Unit of Measure"),
            "BRGEW": SAPFieldInfo("BRGEW", "QUAN", 13, decimals=3, english_name="gross_weight", description="Gross Weight", spark_type="DecimalType(13,3)"),
            "NTGEW": SAPFieldInfo("NTGEW", "QUAN", 13, decimals=3, english_name="net_weight", description="Net Weight", spark_type="DecimalType(13,3)"),
            "GEWEI": SAPFieldInfo("GEWEI", "CHAR", 3, english_name="weight_unit", description="Weight Unit"),
        }
    ),
    "MARC": SAPTableInfo(
        table_name="MARC",
        module="MM",
        description="Plant Data for Material",
        primary_keys=["MATNR", "WERKS"],
        fields={
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number", is_key=True),
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant", is_key=True),
            "DISPO": SAPFieldInfo("DISPO", "CHAR", 3, english_name="mrp_controller", description="MRP Controller"),
            "BESKZ": SAPFieldInfo("BESKZ", "CHAR", 1, english_name="procurement_type", description="Procurement Type (E=Inhouse, F=External)"),
            "SOBSL": SAPFieldInfo("SOBSL", "CHAR", 2, english_name="special_procurement", description="Special Procurement Type"),
        }
    ),
    "MARD": SAPTableInfo(
        table_name="MARD",
        module="MM",
        description="Storage Location Data for Material",
        primary_keys=["MATNR", "WERKS", "LGORT"],
        fields={
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number", is_key=True),
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant", is_key=True),
            "LGORT": SAPFieldInfo("LGORT", "CHAR", 4, english_name="storage_location", description="Storage Location", is_key=True),
            "LABST": SAPFieldInfo("LABST", "QUAN", 13, decimals=3, english_name="unrestricted_stock", description="Valuated Unrestricted-Use Stock", spark_type="DecimalType(13,3)"),
            "UMLME": SAPFieldInfo("UMLME", "QUAN", 13, decimals=3, english_name="transit_transfer_stock", description="Stock in transfer (plant to plant)", spark_type="DecimalType(13,3)"),
            "INSME": SAPFieldInfo("INSME", "QUAN", 13, decimals=3, english_name="quality_inspection_stock", description="Stock in Quality Inspection", spark_type="DecimalType(13,3)"),
            "EINME": SAPFieldInfo("EINME", "QUAN", 13, decimals=3, english_name="restricted_stock", description="Total Restricted-Use Stock", spark_type="DecimalType(13,3)"),
            "SPEME": SAPFieldInfo("SPEME", "QUAN", 13, decimals=3, english_name="blocked_stock", description="Blocked Stock", spark_type="DecimalType(13,3)"),
        }
    ),
    "MSEG": SAPTableInfo(
        table_name="MSEG",
        module="MM",
        description="Material Document: Segment / Movement Item",
        primary_keys=["MBLNR", "MJAHR", "ZEILE"],
        fields={
            "MBLNR": SAPFieldInfo("MBLNR", "CHAR", 10, english_name="material_document", description="Number of Material Document", is_key=True),
            "MJAHR": SAPFieldInfo("MJAHR", "NUMC", 4, english_name="doc_year", description="Material Document Year", is_key=True, spark_type="IntegerType"),
            "ZEILE": SAPFieldInfo("ZEILE", "NUMC", 4, english_name="doc_item", description="Item in Material Document", is_key=True, spark_type="IntegerType"),
            "BWART": SAPFieldInfo("BWART", "CHAR", 3, english_name="movement_type", description="Movement Type (101=GR, 201=GI Cost Center, etc.)"),
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number"),
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant"),
            "LGORT": SAPFieldInfo("LGORT", "CHAR", 4, english_name="storage_location", description="Storage Location"),
            "SHKZG": SAPFieldInfo("SHKZG", "CHAR", 1, english_name="debit_credit_indicator", description="Debit/Credit Indicator (S=Debit, H=Credit)", format_rule="sign_shkzg"),
            "MENGE": SAPFieldInfo("MENGE", "QUAN", 13, decimals=3, english_name="quantity", description="Quantity", spark_type="DecimalType(13,3)"),
            "MEINS": SAPFieldInfo("MEINS", "CHAR", 3, english_name="base_uom", description="Base Unit of Measure"),
            "DMBTR": SAPFieldInfo("DMBTR", "CURR", 13, decimals=2, english_name="amount_local_currency", description="Amount in local currency", spark_type="DecimalType(13,2)"),
            "WAERS": SAPFieldInfo("WAERS", "CHAR", 5, english_name="currency", description="Currency Key"),
            "EBELN": SAPFieldInfo("EBELN", "CHAR", 10, english_name="purchase_order", description="Purchase Order Number"),
            "EBELP": SAPFieldInfo("EBELP", "NUMC", 5, english_name="po_item", description="Item Number of Purchasing Document", spark_type="IntegerType"),
        }
    ),
    "MKPF": SAPTableInfo(
        table_name="MKPF",
        module="MM",
        description="Material Document: Header",
        primary_keys=["MBLNR", "MJAHR"],
        fields={
            "MBLNR": SAPFieldInfo("MBLNR", "CHAR", 10, english_name="material_document", description="Number of Material Document", is_key=True),
            "MJAHR": SAPFieldInfo("MJAHR", "NUMC", 4, english_name="doc_year", description="Material Document Year", is_key=True, spark_type="IntegerType"),
            "BLDAT": SAPFieldInfo("BLDAT", "DATS", 8, english_name="document_date", description="Document Date in Document", spark_type="DateType", format_rule="date_yyyymmdd"),
            "BUDAT": SAPFieldInfo("BUDAT", "DATS", 8, english_name="posting_date", description="Posting Date in the Document", spark_type="DateType", format_rule="date_yyyymmdd"),
            "CPUDT": SAPFieldInfo("CPUDT", "DATS", 8, english_name="entry_date", description="Day On Which Accounting Entry Was Entered", spark_type="DateType", format_rule="date_yyyymmdd"),
            "USNAM": SAPFieldInfo("USNAM", "CHAR", 12, english_name="user_name", description="User Name"),
        }
    ),
    "EKKO": SAPTableInfo(
        table_name="EKKO",
        module="MM",
        description="Purchasing Document Header",
        primary_keys=["EBELN"],
        fields={
            "EBELN": SAPFieldInfo("EBELN", "CHAR", 10, english_name="purchase_order", description="Purchasing Document Number", is_key=True),
            "BUKRS": SAPFieldInfo("BUKRS", "CHAR", 4, english_name="company_code", description="Company Code"),
            "BSTYP": SAPFieldInfo("BSTYP", "CHAR", 1, english_name="purchasing_doc_cat", description="Purchasing Document Category (F=PO)"),
            "BSART": SAPFieldInfo("BSART", "CHAR", 4, english_name="purchasing_doc_type", description="Purchasing Document Type"),
            "AEDAT": SAPFieldInfo("AEDAT", "DATS", 8, english_name="creation_date", description="Date of Creation", spark_type="DateType", format_rule="date_yyyymmdd"),
            "ERNAM": SAPFieldInfo("ERNAM", "CHAR", 12, english_name="created_by", description="Created By"),
            "LIFNR": SAPFieldInfo("LIFNR", "CHAR", 10, english_name="vendor_number", description="Vendor Account Number"),
            "WAERS": SAPFieldInfo("WAERS", "CHAR", 5, english_name="currency", description="Currency Key"),
            "EKORG": SAPFieldInfo("EKORG", "CHAR", 4, english_name="purchasing_org", description="Purchasing Organization"),
            "EKGRP": SAPFieldInfo("EKGRP", "CHAR", 3, english_name="purchasing_group", description="Purchasing Group"),
        }
    ),
    "EKPO": SAPTableInfo(
        table_name="EKPO",
        module="MM",
        description="Purchasing Document Item",
        primary_keys=["EBELN", "EBELP"],
        fields={
            "EBELN": SAPFieldInfo("EBELN", "CHAR", 10, english_name="purchase_order", description="Purchasing Document Number", is_key=True),
            "EBELP": SAPFieldInfo("EBELP", "NUMC", 5, english_name="po_item", description="Item Number of Purchasing Document", is_key=True, spark_type="IntegerType"),
            "LOEKZ": SAPFieldInfo("LOEKZ", "CHAR", 1, english_name="deletion_flag", description="Deletion indicator in purchasing document"),
            "TXZ01": SAPFieldInfo("TXZ01", "CHAR", 40, english_name="short_text", description="Short Text"),
            "MATNR": SAPFieldInfo("MATNR", "CHAR", 18, english_name="material_number", description="Material Number"),
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant"),
            "LGORT": SAPFieldInfo("LGORT", "CHAR", 4, english_name="storage_location", description="Storage Location"),
            "MENGE": SAPFieldInfo("MENGE", "QUAN", 13, decimals=3, english_name="po_quantity", description="Purchase Order Quantity", spark_type="DecimalType(13,3)"),
            "MEINS": SAPFieldInfo("MEINS", "CHAR", 3, english_name="po_unit", description="Purchase Order Unit of Measure"),
            "NETPR": SAPFieldInfo("NETPR", "CURR", 11, decimals=2, english_name="net_price", description="Net Price in Purchasing Document", spark_type="DecimalType(11,2)"),
            "PEINH": SAPFieldInfo("PEINH", "DEC", 5, decimals=0, english_name="price_unit", description="Price unit", spark_type="DecimalType(5,0)"),
            "NETWR": SAPFieldInfo("NETWR", "CURR", 13, decimals=2, english_name="net_order_value", description="Net Order Value in PO Currency", spark_type="DecimalType(13,2)"),
        }
    ),
    "LFA1": SAPTableInfo(
        table_name="LFA1",
        module="Master",
        description="General Data in Vendor Master",
        primary_keys=["LIFNR"],
        fields={
            "LIFNR": SAPFieldInfo("LIFNR", "CHAR", 10, english_name="vendor_number", description="Account Number of Vendor or Creditor", is_key=True),
            "LAND1": SAPFieldInfo("LAND1", "CHAR", 3, english_name="country_key", description="Country Key"),
            "NAME1": SAPFieldInfo("NAME1", "CHAR", 35, english_name="vendor_name", description="Name 1"),
            "ORT01": SAPFieldInfo("ORT01", "CHAR", 35, english_name="city", description="City"),
            "PSTLZ": SAPFieldInfo("PSTLZ", "CHAR", 10, english_name="postal_code", description="Postal Code"),
            "REGIO": SAPFieldInfo("REGIO", "CHAR", 3, english_name="region", description="Region"),
            "STRAS": SAPFieldInfo("STRAS", "CHAR", 35, english_name="street", description="House number and street"),
        }
    ),
    "BKPF": SAPTableInfo(
        table_name="BKPF",
        module="FI",
        description="Accounting Document Header",
        primary_keys=["BUKRS", "BELNR", "GJAHR"],
        fields={
            "BUKRS": SAPFieldInfo("BUKRS", "CHAR", 4, english_name="company_code", description="Company Code", is_key=True),
            "BELNR": SAPFieldInfo("BELNR", "CHAR", 10, english_name="accounting_doc_number", description="Accounting Document Number", is_key=True),
            "GJAHR": SAPFieldInfo("GJAHR", "NUMC", 4, english_name="fiscal_year", description="Fiscal Year", is_key=True, spark_type="IntegerType"),
            "BLART": SAPFieldInfo("BLART", "CHAR", 2, english_name="document_type", description="Document Type (SA=G/L, DR=Cust Inv, DZ=Payment)"),
            "BLDAT": SAPFieldInfo("BLDAT", "DATS", 8, english_name="document_date", description="Document Date in Document", spark_type="DateType", format_rule="date_yyyymmdd"),
            "BUDAT": SAPFieldInfo("BUDAT", "DATS", 8, english_name="posting_date", description="Posting Date in the Document", spark_type="DateType", format_rule="date_yyyymmdd"),
            "MONAT": SAPFieldInfo("MONAT", "NUMC", 2, english_name="fiscal_period", description="Fiscal Period", spark_type="IntegerType"),
            "CPUDT": SAPFieldInfo("CPUDT", "DATS", 8, english_name="entry_date", description="Day On Which Accounting Entry Was Entered", spark_type="DateType", format_rule="date_yyyymmdd"),
            "USNAM": SAPFieldInfo("USNAM", "CHAR", 12, english_name="user_name", description="User Name"),
            "WAERS": SAPFieldInfo("WAERS", "CHAR", 5, english_name="currency", description="Currency Key"),
            "KURSF": SAPFieldInfo("KURSF", "DEC", 9, decimals=5, english_name="exchange_rate", description="Exchange rate", spark_type="DecimalType(9,5)"),
        }
    ),
    "BSEG": SAPTableInfo(
        table_name="BSEG",
        module="FI",
        description="Accounting Document Segment (Line Item)",
        primary_keys=["BUKRS", "BELNR", "GJAHR", "BUZEI"],
        fields={
            "BUKRS": SAPFieldInfo("BUKRS", "CHAR", 4, english_name="company_code", description="Company Code", is_key=True),
            "BELNR": SAPFieldInfo("BELNR", "CHAR", 10, english_name="accounting_doc_number", description="Accounting Document Number", is_key=True),
            "GJAHR": SAPFieldInfo("GJAHR", "NUMC", 4, english_name="fiscal_year", description="Fiscal Year", is_key=True, spark_type="IntegerType"),
            "BUZEI": SAPFieldInfo("BUZEI", "NUMC", 3, english_name="line_item", description="Number of Line Item Within Accounting Document", is_key=True, spark_type="IntegerType"),
            "BSCHL": SAPFieldInfo("BSCHL", "CHAR", 2, english_name="posting_key", description="Posting Key (40=Debit G/L, 50=Credit G/L)"),
            "KOART": SAPFieldInfo("KOART", "CHAR", 1, english_name="account_type", description="Account type (S=G/L, D=Customer, K=Vendor, A=Asset)"),
            "SHKZG": SAPFieldInfo("SHKZG", "CHAR", 1, english_name="debit_credit_indicator", description="Debit/Credit Indicator (S=Debit, H=Credit)", format_rule="sign_shkzg"),
            "DMBTR": SAPFieldInfo("DMBTR", "CURR", 13, decimals=2, english_name="amount_in_local_curr", description="Amount in Local Currency", spark_type="DecimalType(13,2)"),
            "WRBTR": SAPFieldInfo("WRBTR", "CURR", 13, decimals=2, english_name="amount_in_doc_curr", description="Amount in Document Currency", spark_type="DecimalType(13,2)"),
            "SGTXT": SAPFieldInfo("SGTXT", "CHAR", 50, english_name="item_text", description="Item Text"),
            "HKONT": SAPFieldInfo("HKONT", "CHAR", 10, english_name="gl_account", description="General Ledger Account"),
            "KUNNR": SAPFieldInfo("KUNNR", "CHAR", 10, english_name="customer_number", description="Customer Number"),
            "LIFNR": SAPFieldInfo("LIFNR", "CHAR", 10, english_name="vendor_number", description="Vendor Number"),
            "KOSTL": SAPFieldInfo("KOSTL", "CHAR", 10, english_name="cost_center", description="Cost Center"),
            "PRCTR": SAPFieldInfo("PRCTR", "CHAR", 10, english_name="profit_center", description="Profit Center"),
            "AUGBL": SAPFieldInfo("AUGBL", "CHAR", 10, english_name="clearing_document", description="Clearing Document Number"),
            "AUGDT": SAPFieldInfo("AUGDT", "DATS", 8, english_name="clearing_date", description="Clearing Date", spark_type="DateType", format_rule="date_yyyymmdd"),
        }
    ),
    "ACDOCA": SAPTableInfo(
        table_name="ACDOCA",
        module="FI",
        description="Universal Journal Entry Line Items (S/4HANA)",
        primary_keys=["RCLNT", "RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN"],
        fields={
            "RLDNR": SAPFieldInfo("RLDNR", "CHAR", 2, english_name="ledger", description="Ledger in General Ledger Accounting", is_key=True),
            "RBUKRS": SAPFieldInfo("RBUKRS", "CHAR", 4, english_name="company_code", description="Company Code", is_key=True),
            "GJAHR": SAPFieldInfo("GJAHR", "NUMC", 4, english_name="fiscal_year", description="Fiscal Year", is_key=True, spark_type="IntegerType"),
            "BELNR": SAPFieldInfo("BELNR", "CHAR", 10, english_name="accounting_doc_number", description="Accounting Document Number", is_key=True),
            "DOCLN": SAPFieldInfo("DOCLN", "CHAR", 6, english_name="line_item", description="6-place line item", is_key=True),
            "BUDAT": SAPFieldInfo("BUDAT", "DATS", 8, english_name="posting_date", description="Posting Date", spark_type="DateType", format_rule="date_yyyymmdd"),
            "BLDAT": SAPFieldInfo("BLDAT", "DATS", 8, english_name="document_date", description="Document Date", spark_type="DateType", format_rule="date_yyyymmdd"),
            "RACCT": SAPFieldInfo("RACCT", "CHAR", 10, english_name="account_number", description="G/L Account Number"),
            "PRCTR": SAPFieldInfo("PRCTR", "CHAR", 10, english_name="profit_center", description="Profit Center"),
            "RCNTR": SAPFieldInfo("RCNTR", "CHAR", 10, english_name="cost_center", description="Cost Center"),
            "HSL": SAPFieldInfo("HSL", "CURR", 23, decimals=2, english_name="amount_in_local_currency", description="Amount in Local Currency", spark_type="DecimalType(23,2)"),
            "KSL": SAPFieldInfo("KSL", "CURR", 23, decimals=2, english_name="amount_in_global_currency", description="Amount in Global Currency", spark_type="DecimalType(23,2)"),
            "OSL": SAPFieldInfo("OSL", "CURR", 23, decimals=2, english_name="amount_in_transaction_currency", description="Amount in Transaction Currency", spark_type="DecimalType(23,2)"),
            "DRCRK": SAPFieldInfo("DRCRK", "CHAR", 1, english_name="debit_credit_code", description="Debit/Credit Indicator (S/H)"),
        }
    ),
    "SKAT": SAPTableInfo(
        table_name="SKAT",
        module="FI",
        description="G/L Account Master Record (Text)",
        primary_keys=["SPRAS", "KTOPL", "SAKNR"],
        fields={
            "SPRAS": SAPFieldInfo("SPRAS", "CHAR", 1, english_name="language_key", description="Language Key", is_key=True),
            "KTOPL": SAPFieldInfo("KTOPL", "CHAR", 4, english_name="chart_of_accounts", description="Chart of Accounts", is_key=True),
            "SAKNR": SAPFieldInfo("SAKNR", "CHAR", 10, english_name="gl_account", description="G/L Account Number", is_key=True),
            "TXT20": SAPFieldInfo("TXT20", "CHAR", 20, english_name="gl_short_text", description="G/L account short text"),
            "TXT50": SAPFieldInfo("TXT50", "CHAR", 50, english_name="gl_long_text", description="G/L account long text"),
        }
    ),
    "T001": SAPTableInfo(
        table_name="T001",
        module="Master",
        description="Company Codes",
        primary_keys=["BUKRS"],
        fields={
            "BUKRS": SAPFieldInfo("BUKRS", "CHAR", 4, english_name="company_code", description="Company Code", is_key=True),
            "BUTXT": SAPFieldInfo("BUTXT", "CHAR", 25, english_name="company_name", description="Name of Company Code or Company"),
            "ORT01": SAPFieldInfo("ORT01", "CHAR", 25, english_name="city", description="City"),
            "LAND1": SAPFieldInfo("LAND1", "CHAR", 3, english_name="country_key", description="Country Key"),
            "WAERS": SAPFieldInfo("WAERS", "CHAR", 5, english_name="currency", description="Currency Key"),
            "KTOPL": SAPFieldInfo("KTOPL", "CHAR", 4, english_name="chart_of_accounts", description="Chart of Accounts"),
        }
    ),
    "T001W": SAPTableInfo(
        table_name="T001W",
        module="Master",
        description="Plants/Branches",
        primary_keys=["WERKS"],
        fields={
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant", is_key=True),
            "NAME1": SAPFieldInfo("NAME1", "CHAR", 30, english_name="plant_name", description="Name 1"),
            "BWKEY": SAPFieldInfo("BWKEY", "CHAR", 4, english_name="valuation_area", description="Valuation area"),
            "KUNNR": SAPFieldInfo("KUNNR", "CHAR", 10, english_name="customer_num_plant", description="Customer number of plant"),
            "LIFNR": SAPFieldInfo("LIFNR", "CHAR", 10, english_name="vendor_num_plant", description="Vendor number of plant"),
            "LAND1": SAPFieldInfo("LAND1", "CHAR", 3, english_name="country_key", description="Country Key"),
        }
    ),
    "AUFK": SAPTableInfo(
        table_name="AUFK",
        module="PP",
        description="Order Master Data",
        primary_keys=["AUFNR"],
        fields={
            "AUFNR": SAPFieldInfo("AUFNR", "CHAR", 12, english_name="order_number", description="Order Number", is_key=True),
            "AUFART": SAPFieldInfo("AUFART", "CHAR", 4, english_name="order_type", description="Order Type"),
            "AUTYP": SAPFieldInfo("AUTYP", "NUMC", 2, english_name="order_category", description="Order category", spark_type="IntegerType"),
            "KTEXT": SAPFieldInfo("KTEXT", "CHAR", 40, english_name="order_description", description="Description"),
            "BUKRS": SAPFieldInfo("BUKRS", "CHAR", 4, english_name="company_code", description="Company Code"),
            "WERKS": SAPFieldInfo("WERKS", "CHAR", 4, english_name="plant", description="Plant"),
        }
    ),
    "AFKO": SAPTableInfo(
        table_name="AFKO",
        module="PP",
        description="Order Header Data PP Orders",
        primary_keys=["AUFNR"],
        fields={
            "AUFNR": SAPFieldInfo("AUFNR", "CHAR", 12, english_name="order_number", description="Order Number", is_key=True),
            "GLTRP": SAPFieldInfo("GLTRP", "DATS", 8, english_name="basic_finish_date", description="Basic finish date", spark_type="DateType", format_rule="date_yyyymmdd"),
            "GSTRP": SAPFieldInfo("GSTRP", "DATS", 8, english_name="basic_start_date", description="Basic start date", spark_type="DateType", format_rule="date_yyyymmdd"),
            "GAMNG": SAPFieldInfo("GAMNG", "QUAN", 13, decimals=3, english_name="target_quantity", description="Total order quantity", spark_type="DecimalType(13,3)"),
            "GMEIN": SAPFieldInfo("GMEIN", "CHAR", 3, english_name="base_unit", description="Base Unit of Measure of the Order"),
            "PLNBEZ": SAPFieldInfo("PLNBEZ", "CHAR", 18, english_name="material_number", description="Material Number"),
        }
    ),
}
