"""
Synthetic SAP Mock Data Generator
Generates realistic Bronze-layer SAP tables for unit testing and interactive pipeline simulation.
"""

from typing import Dict, Any, List
import pandas as pd


class SAPMockDataGenerator:
    """Creates realistic sample SAP Bronze tables as pandas or Spark DataFrames."""

    @classmethod
    def generate_all_raw_data(cls) -> Dict[str, pd.DataFrame]:
        """Returns in-memory synthetic tables for all core SAP entities."""
        return {
            "VBAK": cls.generate_vbak(),
            "VBAP": cls.generate_vbap(),
            "KNA1": cls.generate_kna1(),
            "MAKT": cls.generate_makt(),
            "MARA": cls.generate_mara(),
            "MARC": cls.generate_marc(),
            "MARD": cls.generate_mard(),
            "BKPF": cls.generate_bkpf(),
            "BSEG": cls.generate_bseg(),
            "SKAT": cls.generate_skat(),
            "LFA1": cls.generate_lfa1(),
            "MSEG": cls.generate_mseg(),
            "MKPF": cls.generate_mkpf(),
            "EKKO": cls.generate_ekko(),
            "EKPO": cls.generate_ekpo(),
            "AUFK": cls.generate_aufk(),
            "AFKO": cls.generate_afko(),
            "T001": cls.generate_t001(),
        }

    @classmethod
    def generate_vbak(cls) -> pd.DataFrame:
        data = [
            {"VBELN": "0000000001", "ERDAT": "20261015", "ERZET": "093000", "ERNAM": "JDOE", "VBTYP": "C", "AUART": "OR", "NETWR": 3500.00, "WAERK": "USD", "VKORG": "1000", "VTWEG": "10", "SPART": "00", "KUNNR": "0000001001"},
            {"VBELN": "0000000002", "ERDAT": "20261018", "ERZET": "111500", "ERNAM": "ASMITH", "VBTYP": "C", "AUART": "OR", "NETWR": 12500.00, "WAERK": "EUR", "VKORG": "1000", "VTWEG": "10", "SPART": "00", "KUNNR": "0000001002"},
            {"VBELN": "0000000003", "ERDAT": "20261020", "ERZET": "142000", "ERNAM": "JDOE", "VBTYP": "H", "AUART": "RE", "NETWR": 800.00, "WAERK": "USD", "VKORG": "2000", "VTWEG": "20", "SPART": "00", "KUNNR": "0000001001"},
            {"VBELN": "0000000004", "ERDAT": "20261025", "ERZET": "164500", "ERNAM": "BWONG", "VBTYP": "C", "AUART": "OR", "NETWR": 6200.00, "WAERK": "USD", "VKORG": "1000", "VTWEG": "10", "SPART": "00", "KUNNR": "0000001003"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_vbap(cls) -> pd.DataFrame:
        data = [
            {"VBELN": "0000000001", "POSNR": 10, "MATNR": "MAT-A100", "MATKL": "ENG01", "ARKTX": "Industrial Bearing 100mm", "KWMENG": 10.0, "VRKME": "EA", "NETPR": 200.00, "NETWR": 2000.00, "WAERK": "USD", "WERKS": "1000", "LGORT": "0001"},
            {"VBELN": "0000000001", "POSNR": 20, "MATNR": "MAT-B200", "MATKL": "ENG02", "ARKTX": "Hydraulic Seal Ring", "KWMENG": 15.0, "VRKME": "EA", "NETPR": 100.00, "NETWR": 1500.00, "WAERK": "USD", "WERKS": "1000", "LGORT": "0001"},
            {"VBELN": "0000000002", "POSNR": 10, "MATNR": "MAT-C300", "MATKL": "ELEC", "ARKTX": "AC Motor 5HP", "KWMENG": 5.0, "VRKME": "EA", "NETPR": 2500.00, "NETWR": 12500.00, "WAERK": "EUR", "WERKS": "1000", "LGORT": "0002"},
            {"VBELN": "0000000003", "POSNR": 10, "MATNR": "MAT-A100", "MATKL": "ENG01", "ARKTX": "Industrial Bearing 100mm", "KWMENG": 4.0, "VRKME": "EA", "NETPR": 200.00, "NETWR": 800.00, "WAERK": "USD", "WERKS": "2000", "LGORT": "0001"},
            {"VBELN": "0000000004", "POSNR": 10, "MATNR": "MAT-B200", "MATKL": "ENG02", "ARKTX": "Hydraulic Seal Ring", "KWMENG": 62.0, "VRKME": "EA", "NETPR": 100.00, "NETWR": 6200.00, "WAERK": "USD", "WERKS": "1000", "LGORT": "0001"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_kna1(cls) -> pd.DataFrame:
        data = [
            {"KUNNR": "0000001001", "LAND1": "US", "NAME1": "Acme Global Dynamics", "ORT01": "Chicago", "PSTLZ": "60601", "REGIO": "IL", "STRAS": "100 Michigan Ave"},
            {"KUNNR": "0000001002", "LAND1": "DE", "NAME1": "Bavaria Motoren Werke AG", "ORT01": "Munich", "PSTLZ": "80809", "REGIO": "BY", "STRAS": "Petuelring 130"},
            {"KUNNR": "0000001003", "LAND1": "US", "NAME1": "Pacific Logistic Services", "ORT01": "Seattle", "PSTLZ": "98101", "REGIO": "WA", "STRAS": "400 Pine St"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_makt(cls) -> pd.DataFrame:
        data = [
            {"MATNR": "MAT-A100", "SPRAS": "E", "MAKTX": "Industrial Bearing 100mm", "MAKTG": "INDUSTRIAL BEARING 100MM"},
            {"MATNR": "MAT-A100", "SPRAS": "D", "MAKTX": "Industrielager 100mm", "MAKTG": "INDUSTRIELAGER 100MM"},
            {"MATNR": "MAT-B200", "SPRAS": "E", "MAKTX": "Hydraulic Seal Ring", "MAKTG": "HYDRAULIC SEAL RING"},
            {"MATNR": "MAT-C300", "SPRAS": "E", "MAKTX": "AC Motor 5HP High Torque", "MAKTG": "AC MOTOR 5HP HIGH TORQUE"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_mara(cls) -> pd.DataFrame:
        data = [
            {"MATNR": "MAT-A100", "ERSDA": "20240115", "ERNAM": "ADMIN", "MTART": "FERT", "MATKL": "ENG01", "MEINS": "EA", "BRGEW": 2.5, "NTGEW": 2.2, "GEWEI": "KG"},
            {"MATNR": "MAT-B200", "ERSDA": "20240210", "ERNAM": "ADMIN", "MTART": "ROH", "MATKL": "ENG02", "MEINS": "EA", "BRGEW": 0.3, "NTGEW": 0.25, "GEWEI": "KG"},
            {"MATNR": "MAT-C300", "ERSDA": "20240305", "ERNAM": "ADMIN", "MTART": "FERT", "MATKL": "ELEC", "MEINS": "EA", "BRGEW": 18.0, "NTGEW": 16.5, "GEWEI": "KG"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_marc(cls) -> pd.DataFrame:
        data = [
            {"MATNR": "MAT-A100", "WERKS": "1000", "DISPO": "001", "BESKZ": "E", "SOBSL": ""},
            {"MATNR": "MAT-B200", "WERKS": "1000", "DISPO": "002", "BESKZ": "F", "SOBSL": ""},
            {"MATNR": "MAT-C300", "WERKS": "1000", "DISPO": "001", "BESKZ": "E", "SOBSL": ""},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_mard(cls) -> pd.DataFrame:
        data = [
            {"MATNR": "MAT-A100", "WERKS": "1000", "LGORT": "0001", "LABST": 150.0, "UMLME": 0.0, "INSME": 10.0, "EINME": 0.0, "SPEME": 0.0},
            {"MATNR": "MAT-B200", "WERKS": "1000", "LGORT": "0001", "LABST": 420.0, "UMLME": 20.0, "INSME": 0.0, "EINME": 0.0, "SPEME": 0.0},
            {"MATNR": "MAT-C300", "WERKS": "1000", "LGORT": "0002", "LABST": 25.0, "UMLME": 0.0, "INSME": 5.0, "EINME": 0.0, "SPEME": 0.0},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_bkpf(cls) -> pd.DataFrame:
        data = [
            {"BUKRS": "1000", "BELNR": "0100000001", "GJAHR": 2026, "BLART": "SA", "BLDAT": "20261010", "BUDAT": "20261010", "MONAT": 10, "CPUDT": "20261010", "USNAM": "CFO_USER", "WAERS": "USD", "KURSF": 1.0},
            {"BUKRS": "1000", "BELNR": "0100000002", "GJAHR": 2026, "BLART": "DR", "BLDAT": "20261015", "BUDAT": "20261015", "MONAT": 10, "CPUDT": "20261015", "USNAM": "AR_USER", "WAERS": "USD", "KURSF": 1.0},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_bseg(cls) -> pd.DataFrame:
        data = [
            {"BUKRS": "1000", "BELNR": "0100000001", "GJAHR": 2026, "BUZEI": 1, "BSCHL": "40", "KOART": "S", "SHKZG": "S", "DMBTR": 4500.00, "WRBTR": 4500.00, "SGTXT": "Consulting Revenue Accrual", "HKONT": "0000113100", "KUNNR": "", "LIFNR": "", "KOSTL": "CC1001", "PRCTR": "PC100"},
            {"BUKRS": "1000", "BELNR": "0100000001", "GJAHR": 2026, "BUZEI": 2, "BSCHL": "50", "KOART": "S", "SHKZG": "H", "DMBTR": 4500.00, "WRBTR": 4500.00, "SGTXT": "Offset Accrual Account", "HKONT": "0000400000", "KUNNR": "", "LIFNR": "", "KOSTL": "CC1002", "PRCTR": "PC100"},
            {"BUKRS": "1000", "BELNR": "0100000002", "GJAHR": 2026, "BUZEI": 1, "BSCHL": "01", "KOART": "D", "SHKZG": "S", "DMBTR": 8200.00, "WRBTR": 8200.00, "SGTXT": "Customer Invoice Acme", "HKONT": "0000140000", "KUNNR": "0000001001", "LIFNR": "", "KOSTL": "", "PRCTR": "PC100"},
            {"BUKRS": "1000", "BELNR": "0100000002", "GJAHR": 2026, "BUZEI": 2, "BSCHL": "50", "KOART": "S", "SHKZG": "H", "DMBTR": 8200.00, "WRBTR": 8200.00, "SGTXT": "Product Sales Revenue", "HKONT": "0000800000", "KUNNR": "", "LIFNR": "", "KOSTL": "CC2001", "PRCTR": "PC200"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_skat(cls) -> pd.DataFrame:
        data = [
            {"SPRAS": "E", "KTOPL": "CAUS", "SAKNR": "0000113100", "TXT20": "Bank Cash Account", "TXT50": "Main Operating Bank Cash Account"},
            {"SPRAS": "E", "KTOPL": "CAUS", "SAKNR": "0000140000", "TXT20": "A/R Trade Domestic", "TXT50": "Accounts Receivable Trade Domestic Customers"},
            {"SPRAS": "E", "KTOPL": "CAUS", "SAKNR": "0000400000", "TXT20": "Accrued Revenue", "TXT50": "Accrued Revenue Current Assets"},
            {"SPRAS": "E", "KTOPL": "CAUS", "SAKNR": "0000800000", "TXT20": "Product Revenue", "TXT50": "Gross Commercial Product Sales Revenue"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_lfa1(cls) -> pd.DataFrame:
        data = [
            {"LIFNR": "0000005001", "LAND1": "DE", "NAME1": "ThyssenKrupp Materials GmbH", "ORT01": "Essen", "PSTLZ": "45143", "REGIO": "NW", "STRAS": "ThyssenKrupp Allee 1"},
            {"LIFNR": "0000005002", "LAND1": "US", "NAME1": "Steel Dynamics Inc", "ORT01": "Fort Wayne", "PSTLZ": "46804", "REGIO": "IN", "STRAS": "7575 W Jefferson Blvd"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_mseg(cls) -> pd.DataFrame:
        data = [
            {"MBLNR": "5000000001", "MJAHR": 2026, "ZEILE": 1, "BWART": "101", "MATNR": "MAT-A100", "WERKS": "1000", "LGORT": "0001", "SHKZG": "S", "MENGE": 100.0, "MEINS": "EA", "DMBTR": 20000.00, "WAERS": "USD", "EBELN": "4500000001", "EBELP": 10},
            {"MBLNR": "5000000002", "MJAHR": 2026, "ZEILE": 1, "BWART": "261", "MATNR": "MAT-A100", "WERKS": "1000", "LGORT": "0001", "SHKZG": "H", "MENGE": 25.0, "MEINS": "EA", "DMBTR": 5000.00, "WAERS": "USD", "EBELN": "", "EBELP": 0},
            {"MBLNR": "5000000003", "MJAHR": 2026, "ZEILE": 1, "BWART": "101", "MATNR": "MAT-B200", "WERKS": "1000", "LGORT": "0001", "SHKZG": "S", "MENGE": 200.0, "MEINS": "EA", "DMBTR": 20000.00, "WAERS": "USD", "EBELN": "4500000002", "EBELP": 10},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_mkpf(cls) -> pd.DataFrame:
        data = [
            {"MBLNR": "5000000001", "MJAHR": 2026, "BLDAT": "20261001", "BUDAT": "20261001", "CPUDT": "20261001", "USNAM": "WAREHOUSE1"},
            {"MBLNR": "5000000002", "MJAHR": 2026, "BLDAT": "20261005", "BUDAT": "20261005", "CPUDT": "20261005", "USNAM": "PROD_OPER"},
            {"MBLNR": "5000000003", "MJAHR": 2026, "BLDAT": "20261012", "BUDAT": "20261012", "CPUDT": "20261012", "USNAM": "WAREHOUSE1"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_ekko(cls) -> pd.DataFrame:
        data = [
            {"EBELN": "4500000001", "BUKRS": "1000", "BSTYP": "F", "BSART": "NB", "AEDAT": "20260920", "ERNAM": "BUYER1", "LIFNR": "0000005001", "WAERS": "USD", "EKORG": "1000", "EKGRP": "001"},
            {"EBELN": "4500000002", "BUKRS": "1000", "BSTYP": "F", "BSART": "NB", "AEDAT": "20260925", "ERNAM": "BUYER2", "LIFNR": "0000005002", "WAERS": "USD", "EKORG": "1000", "EKGRP": "002"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_ekpo(cls) -> pd.DataFrame:
        data = [
            {"EBELN": "4500000001", "EBELP": 10, "LOEKZ": None, "TXZ01": "Bearing 100mm", "MATNR": "MAT-A100", "WERKS": "1000", "LGORT": "0001", "MENGE": 100.0, "MEINS": "EA", "NETPR": 200.00, "PEINH": 1, "NETWR": 20000.00},
            {"EBELN": "4500000002", "EBELP": 10, "LOEKZ": None, "TXZ01": "Hydraulic Seal", "MATNR": "MAT-B200", "WERKS": "1000", "LGORT": "0001", "MENGE": 200.0, "MEINS": "EA", "NETPR": 100.00, "PEINH": 1, "NETWR": 20000.00},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_aufk(cls) -> pd.DataFrame:
        data = [
            {"AUFNR": "0001000001", "AUFART": "PP01", "AUTYP": 10, "KTEXT": "Production of Motor 5HP Batch 1", "BUKRS": "1000", "WERKS": "1000"},
            {"AUFNR": "0001000002", "AUFART": "PP01", "AUTYP": 10, "KTEXT": "Production of Bearings Batch 5", "BUKRS": "1000", "WERKS": "1000"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_afko(cls) -> pd.DataFrame:
        data = [
            {"AUFNR": "0001000001", "GLTRP": "20261110", "GSTRP": "20261101", "GAMNG": 50.0, "GMEIN": "EA", "PLNBEZ": "MAT-C300"},
            {"AUFNR": "0001000002", "GLTRP": "20261115", "GSTRP": "20261105", "GAMNG": 200.0, "GMEIN": "EA", "PLNBEZ": "MAT-A100"},
        ]
        return pd.DataFrame(data)

    @classmethod
    def generate_t001(cls) -> pd.DataFrame:
        data = [
            {"BUKRS": "1000", "BUTXT": "North America Manufacturing Corp", "ORT01": "New York", "LAND1": "US", "WAERS": "USD", "KTOPL": "CAUS"},
            {"BUKRS": "2000", "BUTXT": "European Distribution SA", "ORT01": "Frankfurt", "LAND1": "DE", "WAERS": "EUR", "KTOPL": "CAUS"},
        ]
        return pd.DataFrame(data)
