"""
Pipeline Simulation & Test Execution Runner
Executes transformation specifications against synthetic SAP Bronze datasets to verify
business logic, schema validity, and row outputs.
"""

import time
from typing import Dict, Any, List, Optional
import pandas as pd
from sap_to_pyspark.engine.models import TCodeSpec
from sap_to_pyspark.simulator.mock_data import SAPMockDataGenerator


class SAPPipelineRunner:
    """Executes SAP transformation logic against mock datasets for verification."""

    def __init__(self, raw_data: Optional[Dict[str, pd.DataFrame]] = None):
        self.raw_data = raw_data or SAPMockDataGenerator.generate_all_raw_data()

    def run_spec(self, spec: TCodeSpec) -> Dict[str, Any]:
        """
        Executes the TCodeSpec transformations and returns structured verification results.
        """
        start_time = time.time()
        diagnostics: List[str] = []

        base_tbl = spec.base_table
        if base_tbl not in self.raw_data:
            return {
                "success": False,
                "error": f"Base table {base_tbl} not found in mock data generator.",
                "duration_ms": 0,
            }

        df = self.raw_data[base_tbl].copy()
        # Add table prefix to columns to prevent collision during joins
        df.columns = [f"{base_tbl.lower()}.{c.upper()}" for c in df.columns]
        diagnostics.append(f"Loaded base table {base_tbl} with {len(df)} initial rows.")

        # Execute Joins
        for join in spec.joins:
            r_tbl = join.right_table
            l_tbl = join.left_table
            if r_tbl not in self.raw_data:
                diagnostics.append(f"Warning: Mock data for table {r_tbl} missing, skipping join.")
                continue

            r_df = self.raw_data[r_tbl].copy()
            r_df.columns = [f"{r_tbl.lower()}.{c.upper()}" for c in r_df.columns]

            left_keys = [f"{l_tbl.lower()}.{lk.upper()}" for lk in join.join_keys.keys()]
            right_keys = [f"{r_tbl.lower()}.{rk.upper()}" for rk in join.join_keys.values()]

            # Perform merge
            how = "inner" if join.join_type == "inner" else "left"
            df = pd.merge(
                df,
                r_df,
                left_on=left_keys,
                right_on=right_keys,
                how=how,
            )
            diagnostics.append(f"Joined {r_tbl} ({how}) on {join.join_keys} -> {len(df)} rows.")

        # Execute Filters
        for flt in spec.filters:
            field_u = flt.field.upper()
            col_name = f"{flt.table.lower()}.{field_u}" if flt.table else None
            # Find matching column
            if not col_name or col_name not in df.columns:
                matching = [c for c in df.columns if c.upper().endswith(f".{field_u}")]
                if matching:
                    col_name = matching[0]

            if col_name and col_name in df.columns:
                op = flt.operator.upper()
                val = flt.value
                init_len = len(df)
                if op == "=":
                    df = df[df[col_name] == val]
                elif op in ("!=", "<>"):
                    df = df[df[col_name] != val]
                elif op == ">":
                    df = df[df[col_name] > val]
                elif op == "<":
                    df = df[df[col_name] < val]
                elif op == ">=":
                    df = df[df[col_name] >= val]
                elif op == "<=":
                    df = df[df[col_name] <= val]
                elif op == "IS NULL":
                    df = df[df[col_name].isna()]
                elif op == "IS NOT NULL":
                    df = df[df[col_name].notna()]
                elif op == "IN" and isinstance(val, list):
                    df = df[df[col_name].isin(val)]
                diagnostics.append(f"Filtered {col_name} {op} {val} ({init_len} -> {len(df)} rows).")

        # Execute Projections & Transformations
        result_cols: Dict[str, pd.Series] = {}
        for fld in spec.fields:
            field_u = fld.field.upper()
            src_col = f"{fld.table.lower()}.{field_u}" if fld.table else None
            if not src_col or src_col not in df.columns:
                matching = [c for c in df.columns if c.upper().endswith(f".{field_u}")]
                src_col = matching[0] if matching else None

            alias = fld.alias or fld.field.lower()

            if src_col and src_col in df.columns:
                series = df[src_col].copy()

                # Rule: date_yyyymmdd
                if fld.transform_rule == "date_yyyymmdd":
                    series = pd.to_datetime(series.astype(str), format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")

                # Rule: sign_shkzg
                elif fld.transform_rule == "sign_shkzg":
                    shkzg_col = f"{fld.table.lower()}.SHKZG" if fld.table else None
                    if not shkzg_col or shkzg_col not in df.columns:
                        shkzg_match = [c for c in df.columns if c.endswith(".SHKZG")]
                        shkzg_col = shkzg_match[0] if shkzg_match else None

                    num_series = pd.to_numeric(series, errors="coerce").fillna(0.0)
                    if shkzg_col and shkzg_col in df.columns:
                        # Credit 'H' is negative
                        multiplier = df[shkzg_col].map(lambda x: -1.0 if x == "H" else 1.0)
                        series = num_series * multiplier
                    else:
                        series = num_series

                # Cast numeric types if requested
                elif fld.cast_type and ("Decimal" in fld.cast_type or "Float" in fld.cast_type):
                    series = pd.to_numeric(series, errors="coerce")
                elif fld.cast_type and ("Integer" in fld.cast_type or "Long" in fld.cast_type):
                    series = pd.to_numeric(series, errors="coerce").fillna(0).astype(int)

                result_cols[alias] = series
            else:
                result_cols[alias] = pd.Series([None] * len(df))

        out_df = pd.DataFrame(result_cols)

        # Execute Aggregations (Gold layer) if specified
        if spec.aggregations and spec.aggregations.aggregations:
            grp_cols = [c for c in spec.aggregations.group_by if c in out_df.columns]
            if grp_cols:
                agg_dict = {}
                for alias, agg_expr in spec.aggregations.aggregations.items():
                    # Parse sum(col) etc.
                    for func in ("sum", "count", "mean", "min", "max"):
                        if agg_expr.lower().startswith(f"{func}(") and agg_expr.endswith(")"):
                            col = agg_expr[len(func) + 1 : -1].strip()
                            pd_func = "mean" if func == "avg" else func
                            if col in out_df.columns:
                                agg_dict[alias] = (col, pd_func)
                if agg_dict:
                    out_df = out_df.groupby(grp_cols).agg(**agg_dict).reset_index()
                    diagnostics.append(f"Aggregated by {grp_cols} -> {len(out_df)} rows.")

        elapsed = round((time.time() - start_time) * 1000, 2)

        return {
            "success": True,
            "row_count": len(out_df),
            "columns": list(out_df.columns),
            "sample_data": out_df.head(10).to_dict(orient="records"),
            "diagnostics": diagnostics,
            "duration_ms": elapsed,
        }
