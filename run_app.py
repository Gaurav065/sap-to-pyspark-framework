"""
Launcher for SAP to PySpark Studio Web Application.
Usage:
    python run_app.py
    or
    streamlit run run_app.py
"""

import sys
import os
import subprocess

if __name__ == "__main__":
    app_path = os.path.join(os.path.dirname(__file__), "sap_to_pyspark", "app", "web_app.py")
    cmd = [sys.executable, "-m", "streamlit", "run", app_path]
    print(f"Starting SAP to PySpark Conversion Studio: {' '.join(cmd)}")
    subprocess.run(cmd)
