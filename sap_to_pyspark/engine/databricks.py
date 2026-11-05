"""
Databricks Deployment & Serverless Job Generator
Generates Databricks Workflow Job definitions, Serverless compute tasks,
and Databricks Asset Bundles (DABs) for SAP PySpark pipelines.
"""

import json
from typing import Dict, Any, Optional
from sap_to_pyspark.engine.models import TCodeSpec


class DatabricksJobBuilder:
    """Builds Databricks Jobs (Serverless Workflows) and DABs configuration."""

    @classmethod
    def generate_serverless_job_json(
        cls,
        spec: TCodeSpec,
        workspace_script_path: Optional[str] = None,
        unity_catalog: str = "main",
        unity_schema: str = "sap_lakehouse",
    ) -> Dict[str, Any]:
        """
        Generates a modern Databricks Serverless Workflow Job definition JSON.
        Serverless compute provides instant cold-start execution (sub-10s) with zero cluster management.
        """
        script_path = workspace_script_path or f"/Workspace/Shared/sap_pipelines/{spec.tcode.lower()}_pipeline.py"
        job_name = f"SAP_ETL_{spec.tcode}_Serverless"

        job_spec = {
            "name": job_name,
            "description": f"Automated Serverless Databricks Workflow for SAP T-Code {spec.tcode} ({spec.title})",
            "tags": {
                "source": "SAP",
                "tcode": spec.tcode,
                "module": spec.module,
                "framework": "sap_to_pyspark",
                "compute": "serverless"
            },
            "tasks": [
                {
                    "task_key": f"run_{spec.tcode.lower()}_etl",
                    "description": f"Extracts Bronze SAP {spec.base_table} tables, harmonizes Silver, and writes {spec.target_table_name} Gold Delta table",
                    "spark_python_task": {
                        "python_file": script_path,
                        "parameters": [
                            "--catalog", unity_catalog,
                            "--schema", unity_schema,
                            "--tcode", spec.tcode
                        ]
                    },
                    "environment_key": "Serverless_Compute"
                }
            ],
            "environments": [
                {
                    "environment_key": "Serverless_Compute",
                    "spec": {
                        "client": "1"
                    }
                }
            ],
            "parameters": [
                {
                    "name": "catalog",
                    "default": unity_catalog
                },
                {
                    "name": "schema",
                    "default": unity_schema
                }
            ]
        }
        return job_spec

    @classmethod
    def generate_dab_bundle_yaml(
        cls,
        spec: TCodeSpec,
        unity_catalog: str = "main",
        unity_schema: str = "sap_lakehouse",
    ) -> str:
        """
        Generates a Databricks Asset Bundle (DAB) databricks.yml definition.
        DABs are the enterprise standard for deploying multi-environment Databricks jobs via CI/CD.
        """
        yaml_content = f"""# Databricks Asset Bundle for SAP Modernization Pipeline: {spec.tcode}
bundle:
  name: sap_pipeline_{spec.tcode.lower()}

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      root_path: /Workspace/Users/${{workspace.current_user.userName}}/.bundle/${{bundle.name}}/dev

  prod:
    mode: production
    workspace:
      root_path: /Workspace/Shared/.bundle/${{bundle.name}}/prod
    run_as:
      service_principal_name: sp-sap-lakehouse-pipeline

resources:
  jobs:
    {spec.tcode.lower()}_serverless_job:
      name: "[${{bundle.target}}] SAP_ETL_{spec.tcode}"
      description: "Serverless Databricks ETL for SAP T-Code {spec.tcode} ({spec.title})"
      tasks:
        - task_key: run_pipeline
          spark_python_task:
            python_file: src/{spec.tcode.lower()}_pipeline.py
            parameters:
              - "--catalog"
              - "{unity_catalog}"
              - "--schema"
              - "{unity_schema}"
          environment_key: serverless_env

      environments:
        - environment_key: serverless_env
          spec:
            client: "1"
"""
        return yaml_content
