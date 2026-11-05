"""End-to-End Simulation & Business Logic Tests for SAP to PySpark."""
import unittest
from sap_to_pyspark.engine.registry import TCodeRegistry
from sap_to_pyspark.engine.abap_parser import ABAPQueryParser
from sap_to_pyspark.simulator.runner import SAPPipelineRunner


class TestEndToEndSimulation(unittest.TestCase):
    def setUp(self):
        self.runner = SAPPipelineRunner()

    def test_e2e_va05_sales_orders(self):
        spec = TCodeRegistry.get("VA05")
        res = self.runner.run_spec(spec)
        self.assertTrue(res["success"], msg=res.get("error"))
        self.assertGreater(res["row_count"], 0)
        self.assertIn("sales_order_number", res["columns"])
        self.assertIn("material_description", res["columns"])
        self.assertIn("order_quantity", res["columns"])

        # Check sample data values
        sample = res["sample_data"][0]
        self.assertIsNotNone(sample["sales_order_number"])
        self.assertIsNotNone(sample["customer_name"])

    def test_e2e_fbl3n_gl_sign_resolution(self):
        spec = TCodeRegistry.get("FBL3N")
        res = self.runner.run_spec(spec)
        self.assertTrue(res["success"], msg=res.get("error"))
        self.assertGreater(res["row_count"], 0)
        self.assertIn("signed_amount_local", res["columns"])
        self.assertIn("debit_credit_code", res["columns"])

        # Verify Credit ('H') amounts are negative and Debit ('S') are positive
        for row in res["sample_data"]:
            code = row.get("debit_credit_code")
            amt = row.get("signed_amount_local")
            if code == "H":
                self.assertLess(amt, 0, f"Expected credit amount to be negative, got {amt}")
            elif code == "S":
                self.assertGreater(amt, 0, f"Expected debit amount to be positive, got {amt}")

    def test_e2e_mb5b_inventory_aggregation(self):
        spec = TCodeRegistry.get("MB5B")
        res = self.runner.run_spec(spec)
        self.assertTrue(res["success"], msg=res.get("error"))
        self.assertGreater(res["row_count"], 0)
        self.assertIn("total_net_quantity", res["columns"])
        self.assertIn("total_net_amount", res["columns"])

    def test_e2e_me2m_purchasing(self):
        spec = TCodeRegistry.get("ME2M")
        res = self.runner.run_spec(spec)
        self.assertTrue(res["success"], msg=res.get("error"))
        self.assertGreater(res["row_count"], 0)
        self.assertIn("purchase_order", res["columns"])
        self.assertIn("vendor_name", res["columns"])

    def test_e2e_coois_production_orders(self):
        spec = TCodeRegistry.get("COOIS")
        res = self.runner.run_spec(spec)
        self.assertTrue(res["success"], msg=res.get("error"))
        self.assertGreater(res["row_count"], 0)
        self.assertIn("production_order", res["columns"])

    def test_e2e_custom_abap_query(self):
        abap = """
        SELECT vbak~vbeln, vbap~posnr, vbap~matnr, kna1~name1
          FROM vbak
          INNER JOIN vbap ON vbap~vbeln = vbak~vbeln
          LEFT JOIN kna1 ON kna1~kunnr = vbak~kunnr
          WHERE vbak~vbtyp = 'C'
        """
        parser = ABAPQueryParser()
        spec = parser.parse(abap, tcode="Z_TEST_CUSTOM")
        res = self.runner.run_spec(spec)
        self.assertTrue(res["success"], msg=res.get("error"))
        self.assertGreater(res["row_count"], 0)


if __name__ == "__main__":
    unittest.main()
