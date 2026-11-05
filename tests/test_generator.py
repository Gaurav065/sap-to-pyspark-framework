"""Unit tests for PySpark code and test suite generator."""
import unittest
from sap_to_pyspark.engine.registry import TCodeRegistry
from sap_to_pyspark.engine.generator import PySparkCodeGenerator


class TestPySparkCodeGenerator(unittest.TestCase):
    def setUp(self):
        self.generator = PySparkCodeGenerator()

    def test_all_standard_tcodes_generate_valid_python(self):
        tcodes = TCodeRegistry.list_tcodes()
        self.assertTrue(len(tcodes) >= 5)

        for tc in tcodes:
            spec = TCodeRegistry.get(tc)
            self.assertIsNotNone(spec)

            # 1. Generate Pipeline Script
            pipeline_code = self.generator.generate_pipeline_code(spec)
            self.assertIn("def extract_bronze", pipeline_code)
            self.assertIn("def transform_silver", pipeline_code)
            self.assertIn("def load_target", pipeline_code)
            self.assertIn("def run_pipeline", pipeline_code)

            # Compile AST to ensure zero syntax errors
            try:
                compile(pipeline_code, f"<pipeline_{tc}>", "exec")
            except SyntaxError as e:
                self.fail(f"Generated PySpark pipeline for {tc} failed syntax compilation: {e}")

            # 2. Generate Unit Test Script
            test_code = self.generator.generate_unit_test_code(spec)
            self.assertIn(f"class Test{spec.tcode}Pipeline", test_code)
            try:
                compile(test_code, f"<test_{tc}>", "exec")
            except SyntaxError as e:
                self.fail(f"Generated unit test for {tc} failed syntax compilation: {e}")

    def test_va05_specific_elements(self):
        spec = TCodeRegistry.get("VA05")
        code = self.generator.generate_pipeline_code(spec)
        self.assertIn("F.broadcast(tables['MAKT']", code)
        self.assertIn("F.to_date", code)
        self.assertIn("gold_sales_orders_va05", code)

    def test_fbl3n_sign_logic(self):
        spec = TCodeRegistry.get("FBL3N")
        code = self.generator.generate_pipeline_code(spec)
        self.assertIn("F.when(F.col('bseg.SHKZG') == 'H'", code)
        self.assertIn("-F.col('bseg.DMBTR')", code)


if __name__ == "__main__":
    unittest.main()
