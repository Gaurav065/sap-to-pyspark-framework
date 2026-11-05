# SAP to PySpark Modernization Framework & Databricks Studio

> **An Enterprise-Grade, End-to-End Tested Solution for Reverse-Engineering, Translating, and Deploying SAP ERP Logic (T-Codes & ABAP) to Modern Lakehouse Pipelines (Databricks Serverless / Delta Lake).**

---

## 1. Overview & Business Value

Traditional SAP ERP systems (ECC 6.0 & S/4HANA) store business-critical reporting logic inside proprietary Transaction Codes (T-Codes) and ABAP programs. This framework bridges the gap between SAP and modern cloud Lakehouse platforms by automating:

* **T-Code Reverse Engineering**: Decouples business logic from SAP GUI into clean PySpark transformations.
* **ABAP OpenSQL Parsing**: Translates raw SAP queries (with `table~field` syntax, `@DATA` declarations, and `JOIN`s) directly into PySpark Abstract Syntax Trees (AST).
* **Automated Pipeline Generation**: Emits Medallion architecture pipelines (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) writing to managed Delta Lake tables.
* **Automated Unit Testing**: Generates companion test suites for every converted pipeline.
* **One-Click Databricks Serverless Deployment**: Deploys pipelines as Serverless Workflow Jobs in Unity Catalog with zero cluster management.

---

## 2. Architecture & Design Principles

```
                       ┌──────────────────────────────────────────────┐
                       │                Input Modality                │
                       │  - Standard T-Codes (VA05, FBL3N, MB5B...)   │
                       │  - Raw ABAP OpenSQL / Standard SQL Queries   │
                       │  - Declarative TCodeSpec (JSON / YAML)       │
                       └──────────────────────┬───────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              Core Modernization Engine                                  │
│                                                                                         │
│   ┌────────────────────────┐   ┌───────────────────────────┐   ┌────────────────────┐   │
│   │    ABAP Query Parser   │──▶│    TCodeSpec AST Model    │◀──│  T-Code Registry   │   │
│   │  (OpenSQL -> AST Spec) │   │     (Pydantic Schema)     │   │(VA05, FBL3N, MB5B) │   │
│   └────────────────────────┘   └─────────────┬─────────────┘   └────────────────────┘   │
│                                              │                                          │
│   ┌────────────────────────┐                 ▼                 ┌────────────────────┐   │
│   │   SAP Data Dictionary  │◀───── PySpark Code Generator ────▶│ SAP Domain Rules   │   │
│   │  (Tables, Fields, Joins│          (AST Compiler)           │(SHKZG, TCURX, DATS)│   │
│   └────────────────────────┘                 │                 └────────────────────┘   │
└──────────────────────────────────────────────┼──────────────────────────────────────────┘
                                               │
                        ┌──────────────────────┴──────────────────────┐
                        ▼                                             ▼
┌─────────────────────────────────────────────┐   ┌─────────────────────────────────────────┐
│            Generated Artifacts              │   │         Simulation & Test Suite         │
│  - Modular PySpark ETL Pipeline             │   │  - Synthetic SAP Bronze Data Generator  │
│  - Automated PySpark Test Suite             │   │  - Dynamic Pipeline Simulation Engine   │
│  - Databricks Serverless Job Definition     │   │  - 100% Logic Verification (18 tests)   │
│  - Databricks Asset Bundle (DABs YAML)      │   │                                         │
└─────────────────────────────────────────────┘   └─────────────────────────────────────────┘
                        │                                             ▲
                        └──────────────────────┬──────────────────────┘
                                               │
                        ┌──────────────────────┴──────────────────────┐
                        ▼                                             ▼
             ┌─────────────────────┐                       ┌─────────────────────┐
             │ Interactive Web UI  │                       │    Developer CLI    │
             │   (Streamlit App)   │                       │   (Rich Terminal)   │
             └─────────────────────┘                       └─────────────────────┘
```

---

## 3. Real-World SAP Quirks Handled Out-of-the-Box

1. **Debit / Credit Sign Resolution (`SHKZG`)**:
   In SAP accounting tables (`BSEG`, `ACDOCA`), amounts are stored as positive absolute values with an indicator:
   * **`S`** (*Soll*) = Debit (Positive)
   * **`H`** (*Haben*) = Credit (Negative)
   * *Generated Spark Logic:* `F.when(F.col("SHKZG") == "H", -F.col("DMBTR")).otherwise(F.col("DMBTR"))`
2. **Implicit Currency Decimal Shifts (`TCURX`)**:
   SAP stores currency amounts with 2 default decimal places. For currencies with 0 decimals (JPY, KRW) or 3 decimals (KWD, BHD), power-of-10 multipliers are applied automatically.
3. **Date String Formatting & Null Zero-Dates (`DATS`)**:
   Converts 8-character strings (`YYYYMMDD`) to Spark `DateType` while mapping unassigned SAP dates (`'00000000'`) safely to `NULL`.
4. **Broadcast Join Optimization**:
   Master data and text tables (`MAKT`, `T001`, `KNA1`) are automatically wrapped in `F.broadcast(...)` to eliminate shuffle bottlenecks.

---

## 4. Repository Structure

```
sap_to_pyspark_framework/
├── sap_to_pyspark/                  # Core Python Framework Package
│   ├── metadata/                    # SAP Data Dictionary & Domain Quirks
│   │   ├── tables.py                # Schema catalog for SD, MM, FI, PP tables
│   │   ├── data_dictionary.py       # Field translations & join pathfinder
│   │   └── currency_rules.py        # SHKZG sign logic & TCURX decimal shift
│   ├── engine/                      # Modernization Engine
│   │   ├── models.py                # Pydantic AST models (TCodeSpec, Joins, Filters)
│   │   ├── abap_parser.py           # ABAP OpenSQL lexer, parser, and cleaner
│   │   ├── generator.py             # Production PySpark pipeline & test code generator
│   │   ├── registry.py              # Standard pre-configured SAP T-Codes catalog
│   │   └── databricks.py            # Databricks Serverless Job and DABs bundle builder
│   ├── simulator/                   # Verification & Testing Framework
│   │   ├── mock_data.py             # Realistic synthetic SAP Bronze datasets
│   │   └── runner.py                # In-memory execution and validation engine
│   └── app/                         # User Applications
│       ├── cli.py                   # Terminal CLI with rich formatting
│       └── web_app.py               # Streamlit visual migration studio
├── tests/                           # Complete Automated Test Suite (18 tests)
│   ├── test_metadata.py             # Data dictionary and naming tests
│   ├── test_abap_parser.py          # ABAP query parser tests
│   ├── test_generator.py            # Code generator syntax tests
│   ├── test_e2e_simulation.py       # End-to-end transformation & business tests
│   └── test_databricks.py           # Databricks Serverless & DABs tests
├── databricks/                      # Verified Databricks Deployment Assets
│   ├── sap_va05_job_serverless.json # Ready-to-deploy Databricks Workflow Job
│   └── va05_pipeline.py             # Databricks Serverless notebook script
├── run_app.py                       # One-click Streamlit Web Studio launcher
├── requirements.txt                 # Python dependencies
├── SAP_SPECIFICATION.md             # In-depth architectural specification
└── README.md                        # This file
```

---

## 5. Supported Standard SAP T-Codes

The framework includes pre-configured, verified specifications for classic SAP ERP reports:

| T-Code | Module | Title | Base Table | Joined Tables | Output Gold Table |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`VA05`** | SD | List of Sales Orders | `VBAK` | `VBAP`, `KNA1`, `MAKT` | `gold_sales_orders_va05` |
| **`FBL3N`** | FI | G/L Account Line Items | `BKPF` | `BSEG`, `SKAT`, `KNA1`, `LFA1` | `gold_gl_line_items_fbl3n` |
| **`MB5B`** | MM | Stock on Posting Date | `MSEG` | `MKPF`, `MAKT` | `gold_inventory_movements_mb5b` |
| **`ME2M`** | MM | POs by Material | `EKPO` | `EKKO`, `LFA1`, `MAKT` | `gold_po_by_material_me2m` |
| **`COOIS`** | PP | Production Orders | `AUFK` | `AFKO`, `MAKT` | `gold_production_orders_coois` |

---

## 6. Team Quickstart Guide

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Run the Automated Test Suite (100% Pass Rate)
Run the full unit and integration test suite:
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
*Expected: 18 passed in ~0.09s.*

---

### Step 3: Launch the Visual Web Studio (Streamlit)
Launch the interactive web application in your browser:
```bash
python run_app.py
# Or:
streamlit run sap_to_pyspark/app/web_app.py
```

**Features in the Web App:**
1. **Standard T-Codes Migrator:** Select any T-Code, configure Delta partition schemes, preview PySpark code, and test with live simulation.
2. **ABAP OpenSQL Converter:** Paste any custom ABAP `SELECT` statement and instantly generate PySpark ETL code.
3. **Databricks Deployment Tab:** View and download Databricks Serverless Job definitions (`.json`) and Databricks Asset Bundles (`databricks.yml`).
4. **Data Dictionary Explorer:** Search any SAP table or field to see English business aliases, data types, and primary key join paths.

---

### Step 4: Using the Command-Line Interface (CLI)

#### 1. List Available T-Codes:
```bash
python -m sap_to_pyspark.app.cli list
```

#### 2. Inspect T-Code Metadata & Joins:
```bash
python -m sap_to_pyspark.app.cli info --tcode VA05
```

#### 3. Generate PySpark Pipeline & Unit Tests:
```bash
python -m sap_to_pyspark.app.cli generate --tcode VA05 --output pipelines/va05_pipeline.py --test-output tests/test_va05_pipeline.py
```

#### 4. Parse Raw ABAP Query to PySpark:
```bash
python -m sap_to_pyspark.app.cli parse-abap --query "SELECT vbak~vbeln, vbap~posnr, vbap~matnr FROM vbak INNER JOIN vbap ON vbap~vbeln = vbak~vbeln WHERE vbak~vbtyp = 'C'" --output pipelines/custom_orders.py
```

#### 5. Run Live Simulation against Mock SAP Bronze Data:
```bash
python -m sap_to_pyspark.app.cli test --tcode FBL3N
```

#### 6. Export Databricks Serverless Job & DABs Bundle:
```bash
python -m sap_to_pyspark.app.cli export-databricks --tcode VA05 --output-job databricks/job.json --output-dab databricks/databricks.yml
```

---

## 7. Deploying to Databricks Serverless Workflows (Verified Live)

The framework has been tested and verified live on Databricks Serverless compute (`cloud.databricks.com`):

### 1. Upload Notebook Script to Workspace:
```powershell
databricks workspace mkdirs /Users/<your-email>/sap_pipelines
databricks workspace import /Users/<your-email>/sap_pipelines/va05_notebook --file databricks/va05_pipeline.py --format SOURCE --language PYTHON --overwrite
```

### 2. Create the Serverless Databricks Job:
```powershell
databricks jobs create --json @databricks/sap_va05_job_serverless.json
```

### 3. Run the Job on Serverless Compute:
```powershell
databricks jobs run-now <job_id>
```

### 4. Verify in Unity Catalog:
The pipeline writes directly to Unity Catalog:
* Bronze: `workspace.default.vbak`, `vbap`, `kna1`, `makt`
* Gold: `workspace.default.gold_sales_orders_va05`

---

## 8. Git & Sharing

This directory is initialized as a standalone Git repository. You can add your company's remote and push:
```bash
git remote add origin https://github.com/your-org/sap-to-pyspark-modernization.git
git branch -M main
git push -u origin main
```
