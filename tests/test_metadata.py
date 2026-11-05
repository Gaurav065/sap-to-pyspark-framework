"""Unit tests for SAP metadata, data dictionary, and currency/sign rules."""
import unittest
from sap_to_pyspark.metadata.tables import SAP_TABLES
from sap_to_pyspark.metadata.data_dictionary import SAPDataDictionary
from sap_to_pyspark.metadata.currency_rules import CurrencyRules, SignRules, DateRules


class TestSAPMetadata(unittest.TestCase):
    def test_core_tables_registered(self):
        core = ["VBAK", "VBAP", "BKPF", "BSEG", "ACDOCA", "MSEG", "MKPF", "EKKO", "EKPO", "KNA1", "MAKT"]
        for t in core:
            self.assertIn(t, SAP_TABLES)
            tbl = SAPDataDictionary.get_table(t)
            self.assertIsNotNone(tbl)
            self.assertTrue(len(tbl.primary_keys) > 0)
            self.assertTrue(len(tbl.fields) > 0)

    def test_english_alias_suggestions(self):
        self.assertEqual(SAPDataDictionary.suggest_english_alias("VBAK", "VBELN"), "sales_document")
        self.assertEqual(SAPDataDictionary.suggest_english_alias("VBAP", "MATNR"), "material_number")
        self.assertEqual(SAPDataDictionary.suggest_english_alias("BSEG", "DMBTR"), "amount_in_local_curr")
        self.assertEqual(SAPDataDictionary.suggest_english_alias(None, "MANDT"), "client_id")

    def test_join_keys_pathfinder(self):
        keys = SAPDataDictionary.suggest_join_keys("VBAK", "VBAP")
        self.assertEqual(keys, ["VBELN"])

        fi_keys = SAPDataDictionary.suggest_join_keys("BKPF", "BSEG")
        self.assertEqual(fi_keys, ["BUKRS", "BELNR", "GJAHR"])

    def test_currency_and_sign_rules(self):
        self.assertEqual(CurrencyRules.get_factor("JPY"), 100.0)
        self.assertEqual(CurrencyRules.get_factor("KWD"), 0.1)
        self.assertEqual(CurrencyRules.get_factor("USD"), 1.0)

        sign_expr = SignRules.spark_signed_amount_expr("DMBTR")
        self.assertIn("SHKZG", sign_expr)
        self.assertIn("-F.col('DMBTR')", sign_expr)

        date_expr = DateRules.spark_date_expr("ERDAT", "order_date")
        self.assertIn("00000000", date_expr)
        self.assertIn("yyyyMMdd", date_expr)


if __name__ == "__main__":
    unittest.main()
