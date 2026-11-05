"""
Streamlit Web Application for SAP T-Code to PySpark Migration
Interactive visual workspace for selecting, converting, simulating, and exporting SAP logic to PySpark.
"""

import json
import streamlit as st
import pandas as pd
from sap_to_pyspark.engine.registry import TCodeRegistry
from sap_to_pyspark.engine.generator import PySparkCodeGenerator
from sap_to_pyspark.engine.abap_parser import ABAPQueryParser
from sap_to_pyspark.engine.databricks import DatabricksJobBuilder
from sap_to_pyspark.simulator.runner import SAPPipelineRunner
from sap_to_pyspark.metadata.data_dictionary import SAPDataDictionary
from sap_to_pyspark.metadata.tables import SAP_TABLES

# Page configuration
st.set_page_config(
    page_title="SAP to PySpark Conversion Studio",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for polished enterprise look
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        border-radius: 8px;
        padding: 12px;
        border-left: 4px solid #2563EB;
    }
</style>
""", unsafe_allow_html=True)


def main():
    st.markdown('<div class="main-header">⚡ SAP T-Code to PySpark Conversion Studio</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Automate the reverse-engineering and migration of SAP ERP business reports to Lakehouse PySpark pipelines</div>', unsafe_allow_html=True)

    tabs = st.tabs([
        "📦 Standard T-Codes Migrator",
        "🔄 ABAP OpenSQL Converter",
        "📖 SAP Data Dictionary Explorer",
        "💡 SAP Quirks & Engineering Rules"
    ])

    # -------------------------------------------------------------
    # TAB 1: Standard T-Codes Migrator
    # -------------------------------------------------------------
    with tabs[0]:
        st.subheader("Select and Configure Standard SAP T-Code")
        
        all_specs = TCodeRegistry.get_all()
        tcode_options = list(all_specs.keys())

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            selected_tcode = st.selectbox("Select SAP T-Code", tcode_options, index=0)
            spec = all_specs[selected_tcode]
        with col2:
            st.text_input("SAP Module", value=spec.module, disabled=True)
        with col3:
            st.text_input("Business Title", value=spec.title, disabled=True)

        st.info(f"**Description:** {spec.description}")

        # Configuration options
        with st.expander("⚙️ Pipeline Configuration & Target Table Settings", expanded=True):
            cfg_col1, cfg_col2, cfg_col3 = st.columns(3)
            with cfg_col1:
                target_table = st.text_input("Target Lakehouse Table", value=f"{spec.target_layer}.{spec.target_table_name}")
            with cfg_col2:
                target_format = st.selectbox("Storage Format", ["delta", "parquet"], index=0)
            with cfg_col3:
                partition_cols = st.multiselect("Partition Columns", spec.partition_by, default=spec.partition_by)

        # Architectural Overview
        st.write("### 🏗️ Data Architecture & Join Model")
        arch_col1, arch_col2 = st.columns([1, 1])

        with arch_col1:
            st.write(f"**Base Table:** `{spec.base_table}`")
            join_rows = []
            for j in spec.joins:
                join_rows.append({
                    "Left Table": j.left_table,
                    "Right Table": j.right_table,
                    "Join Type": j.join_type,
                    "Join Keys": str(j.join_keys),
                    "Broadcast Hint": "⚡ Yes" if j.is_broadcast else "No",
                })
            st.dataframe(pd.DataFrame(join_rows), use_container_width=True)

        with arch_col2:
            st.write(f"**Output Field Projections ({len(spec.fields)} fields):**")
            field_rows = []
            for f in spec.fields:
                field_rows.append({
                    "Source Table": f.table or spec.base_table,
                    "Field": f.field,
                    "PySpark Alias": f.alias,
                    "Cast Type": f.cast_type or "String",
                    "Rule": f.transform_rule or "-",
                })
            st.dataframe(pd.DataFrame(field_rows), use_container_width=True, height=210)

        # Code Generation
        generator = PySparkCodeGenerator()
        pipeline_code = generator.generate_pipeline_code(spec)
        unit_test_code = generator.generate_unit_test_code(spec)

        st.write("### 💻 Generated PySpark Pipeline Code & Deployment")
        code_tab1, code_tab2, code_tab3 = st.tabs([
            "🚀 Production PySpark Script", 
            "🧪 PySpark Unit Tests", 
            "🧱 Databricks Serverless Job & DABs"
        ])
        with code_tab1:
            st.code(pipeline_code, language="python")
            st.download_button(
                label=f"⬇️ Download {spec.tcode}_pipeline.py",
                data=pipeline_code,
                file_name=f"{spec.tcode.lower()}_pipeline.py",
                mime="text/x-python",
            )
        with code_tab2:
            st.code(unit_test_code, language="python")
            st.download_button(
                label=f"⬇️ Download test_{spec.tcode.lower()}_pipeline.py",
                data=unit_test_code,
                file_name=f"test_{spec.tcode.lower()}_pipeline.py",
                mime="text/x-python",
            )
        with code_tab3:
            dbx_job = DatabricksJobBuilder.generate_serverless_job_json(spec)
            dbx_dab = DatabricksJobBuilder.generate_dab_bundle_yaml(spec)
            dbx_sub1, dbx_sub2 = st.tabs(["⚡ Serverless Job Definition (JSON)", "📦 Databricks Asset Bundle (YAML)"])
            with dbx_sub1:
                st.code(json.dumps(dbx_job, indent=2), language="json")
                st.download_button(
                    label=f"⬇️ Download {spec.tcode.lower()}_job_serverless.json",
                    data=json.dumps(dbx_job, indent=2),
                    file_name=f"{spec.tcode.lower()}_job_serverless.json",
                    mime="application/json",
                )
            with dbx_sub2:
                st.code(dbx_dab, language="yaml")
                st.download_button(
                    label="⬇️ Download databricks.yml (DABs)",
                    data=dbx_dab,
                    file_name="databricks.yml",
                    mime="text/yaml",
                )

        # Live Simulation & Testing
        st.write("---")
        st.write("### 🧪 Live End-to-End Simulation")
        st.write("Execute this transformation against simulated SAP Bronze data to verify output correctness:")

        if st.button(f"▶️ Run Live Simulation for {spec.tcode}", type="primary"):
            runner = SAPPipelineRunner()
            with st.spinner("Executing transformation pipeline against synthetic Bronze tables..."):
                res = runner.run_spec(spec)

            if res["success"]:
                st.success(f"Pipeline executed successfully in {res['duration_ms']} ms!")
                m1, m2, m3 = st.columns(3)
                m1.metric("Output Rows Produced", res["row_count"])
                m2.metric("Output Columns", len(res["columns"]))
                m3.metric("Execution Latency", f"{res['duration_ms']} ms")

                st.write("**Sample Output Records:**")
                st.dataframe(pd.DataFrame(res["sample_data"]), use_container_width=True)

                with st.expander("Detailed Execution Trace & Filter Diagnostics"):
                    for d in res["diagnostics"]:
                        st.write(f"- {d}")
            else:
                st.error(f"Simulation failed: {res.get('error')}")

    # -------------------------------------------------------------
    # TAB 2: ABAP OpenSQL Converter
    # -------------------------------------------------------------
    with tabs[1]:
        st.subheader("Translate Custom ABAP OpenSQL to PySpark")
        st.write("Paste your SAP ABAP `SELECT` statement or SQL query below to convert it to a full PySpark pipeline:")

        sample_abap_1 = """SELECT vbak~vbeln, vbak~erdat, vbap~posnr, vbap~matnr, vbap~kwmeng, kna1~name1
  FROM vbak
  INNER JOIN vbap ON vbap~vbeln = vbak~vbeln
  LEFT OUTER JOIN kna1 ON kna1~kunnr = vbak~kunnr
  WHERE vbak~vbtyp = 'C' AND vbak~netwr > 1000
  INTO TABLE @DATA(lt_orders)."""

        sample_abap_2 = """SELECT bkpf~bukrs, bkpf~belnr, bkpf~gjahr, bseg~buzei, bseg~hkont, bseg~shkzg, bseg~dmbtr
  FROM bkpf
  INNER JOIN bseg ON bkpf~bukrs = bseg~bukrs AND bkpf~belnr = bseg~belnr AND bkpf~gjahr = bseg~gjahr
  WHERE bkpf~gjahr = 2026
  INTO TABLE @DATA(lt_gl_items)."""

        query_choice = st.selectbox("Load Sample Query or Write Custom", ["Sample 1: Sales Orders (VBAK + VBAP + KNA1)", "Sample 2: Financial Postings (BKPF + BSEG)", "Custom ABAP Query"])
        
        default_query = sample_abap_1 if "Sales" in query_choice else (sample_abap_2 if "Financial" in query_choice else "")
        user_query = st.text_area("ABAP OpenSQL Query", value=default_query, height=180)

        col_q1, col_q2 = st.columns(2)
        with col_q1:
            custom_tcode = st.text_input("Custom T-Code / Name", value="Z_CUSTOM_REPORT")
        with col_q2:
            custom_target_tbl = st.text_input("Custom Target Table Name", value="gold_custom_report")

        if st.button("⚡ Translate Query to PySpark", type="primary"):
            if not user_query.strip():
                st.warning("Please provide an ABAP query.")
            else:
                try:
                    parser = ABAPQueryParser()
                    custom_spec = parser.parse(
                        query=user_query,
                        tcode=custom_tcode,
                        target_table_name=custom_target_tbl
                    )
                    gen = PySparkCodeGenerator()
                    trans_code = gen.generate_pipeline_code(custom_spec)

                    st.success("Successfully parsed ABAP AST and generated PySpark pipeline!")
                    st.code(trans_code, language="python")

                    # Live test runner
                    st.write("#### 🧪 Test Translated Pipeline")
                    test_runner = SAPPipelineRunner()
                    test_res = test_runner.run_spec(custom_spec)
                    if test_res["success"]:
                        st.info(f"Simulated execution returned {test_res['row_count']} rows.")
                        if test_res["sample_data"]:
                            st.dataframe(pd.DataFrame(test_res["sample_data"]), use_container_width=True)
                    else:
                        st.warning(f"Note on simulation: {test_res.get('error')}")

                except Exception as ex:
                    st.error(f"Error parsing ABAP query: {str(ex)}")

    # -------------------------------------------------------------
    # TAB 3: SAP Data Dictionary Explorer
    # -------------------------------------------------------------
    with tabs[2]:
        st.subheader("SAP Data Dictionary & Field Translation Explorer")
        st.write("Browse standard SAP ERP tables, German technical names, English business aliases, and data types.")

        all_tables = SAPDataDictionary.list_tables()
        selected_table = st.selectbox("Select SAP Table to Inspect", all_tables, index=0)

        tbl_info = SAPDataDictionary.get_table(selected_table)
        if tbl_info:
            c1, c2, c3 = st.columns(3)
            c1.metric("Table", tbl_info.table_name)
            c2.metric("Module", tbl_info.module)
            c3.metric("Primary Keys", ", ".join(tbl_info.primary_keys))

            st.write(f"**Description:** {tbl_info.description}")

            field_list = []
            for fname, finfo in tbl_info.fields.items():
                field_list.append({
                    "SAP Technical Name": finfo.field_name,
                    "English Business Name": finfo.english_name,
                    "SAP Type": f"{finfo.sap_type}({finfo.length})",
                    "Spark Type": finfo.spark_type,
                    "Is Key": "🔑 Yes" if finfo.is_key else "",
                    "Rule": finfo.format_rule or "-",
                    "Description": finfo.description,
                })
            st.dataframe(pd.DataFrame(field_list), use_container_width=True, height=350)

        st.write("---")
        st.write("#### 🔍 Join Key Pathfinder")
        col_j1, col_j2 = st.columns(2)
        with col_j1:
            t1 = st.selectbox("Source Table 1", all_tables, index=0)
        with col_j2:
            t2 = st.selectbox("Target Table 2", all_tables, index=1)

        suggested_keys = SAPDataDictionary.suggest_join_keys(t1, t2)
        if suggested_keys:
            st.success(f"Suggested Standard Join Keys between **{t1}** and **{t2}**: `{suggested_keys}`")
        else:
            st.warning(f"No direct primary key overlap found between {t1} and {t2}. Custom bridge or mapping table required.")

    # -------------------------------------------------------------
    # TAB 4: SAP Quirks & Engineering Rules
    # -------------------------------------------------------------
    with tabs[3]:
        st.subheader("SAP Lakehouse Engineering Rules & Gotchas")
        
        with st.expander("1. Debit / Credit Sign Logic (`SHKZG`)", expanded=True):
            st.markdown("""
            In SAP accounting tables like `BSEG` and `ACDOCA`, transaction amounts (`DMBTR`, `WRBTR`, `HSL`) are stored as **positive absolute values**.
            The field `SHKZG` indicates whether the line is a Debit or Credit:
            - **`S`** (*Soll*) = Debit (Positive)
            - **`H`** (*Haben*) = Credit (Negative)
            
            **PySpark Equivalent:**
            ```python
            F.when(F.col("SHKZG") == "H", -F.col("DMBTR")).otherwise(F.col("DMBTR"))
            ```
            """)

        with st.expander("2. Implicit Currency Decimals (`TCURX`)", expanded=True):
            st.markdown("""
            By default, SAP databases store all currency values with **2 implicit decimal places**.
            For currencies that don't have 2 decimals (e.g., Japanese Yen `JPY` with 0 decimals, Kuwaiti Dinar `KWD` with 3 decimals), the raw database value is multiplied/divided by powers of 10!
            
            **PySpark Equivalent:**
            ```python
            F.when(F.col("WAERS").isin(['JPY', 'KRW']), F.col("NETWR") * 100.0) \\
             .when(F.col("WAERS").isin(['KWD', 'BHD']), F.col("NETWR") * 0.1) \\
             .otherwise(F.col("NETWR"))
            ```
            """)

        with st.expander("3. SAP Date String Formats (`DATS` '00000000')", expanded=True):
            st.markdown("""
            SAP stores dates as 8-character strings (`YYYYMMDD`). When a date is empty or unassigned, SAP frequently writes `'00000000'`. If you run `to_date()` blindly in Spark, `'00000000'` will fail or yield invalid date records.
            
            **PySpark Equivalent:**
            ```python
            F.when((F.col("ERDAT").isNull()) | (F.col("ERDAT") == '00000000') | (F.col("ERDAT") == ''), F.lit(None)) \\
             .otherwise(F.to_date(F.col("ERDAT"), 'yyyyMMdd'))
            ```
            """)

        with st.expander("4. ECC vs. S/4HANA Architecture Differences", expanded=True):
            st.markdown("""
            - **SAP ECC (ERP 6.0)**: Accounting is split into Header (`BKPF`), Line Items (`BSEG`), and secondary index tables (`BSIS`, `BSAS`, `BSID`, `BSAD`). `BSEG` in older ECC is a cluster table.
            - **SAP S/4HANA**: Introduces **`ACDOCA` (Universal Journal)** which merges G/L, CO, Asset Accounting, and Material Ledger into a single 300+ column table, eliminating the need for complex joins.
            """)


if __name__ == "__main__":
    main()
