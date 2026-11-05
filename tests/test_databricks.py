"""Unit tests for Databricks Serverless job and DAB bundle generation."""
import unittest
import json
from sap_to_pyspark.engine.registry import TCodeRegistry
from sap_to_pyspark.engine.databricks import DatabricksJobBuilder


class TestDatabricksJobBuilder(unittest.TestCase):
    def test_generate_serverless_job_json(self):
        spec = TCodeRegistry.get("VA05")
        job = DatabricksJobBuilder.generate_serverless_job_json(spec, unity_catalog="prod_catalog", unity_schema="gold_sap")

        self.assertEqual(job["name"], "SAP_ETL_VA05_Serverless")
        self.assertEqual(len(job["tasks"]), 1)
        task = job["tasks"][0]
        self.assertIn("spark_python_task", task)
        self.assertEqual(task["environment_key"], "Serverless_Compute")
        self.assertIn("va05_pipeline.py", task["spark_python_task"]["python_file"])

        # Validate JSON serialization
        job_json_str = json.dumps(job)
        self.assertIn("Serverless_Compute", job_json_str)

    def test_generate_dab_bundle_yaml(self):
        spec = TCodeRegistry.get("FBL3N")
        yaml_content = DatabricksJobBuilder.generate_dab_bundle_yaml(spec)

        self.assertIn("bundle:", yaml_content)
        self.assertIn("sap_pipeline_fbl3n", yaml_content)
        self.assertIn("serverless_env", yaml_content)
        self.assertIn("fbl3n_pipeline.py", yaml_content)


if __name__ == "__main__":
    unittest.main()
