"""
Domain models for SAP T-Code specifications and AST representations.
"""

from typing import List, Dict, Optional, Any, Literal
from pydantic import BaseModel, Field


class JoinCondition(BaseModel):
    """Specification of a join between two SAP tables."""
    left_table: str
    right_table: str
    join_keys: Dict[str, str] = Field(
        ...,
        description="Key mapping between left and right table, e.g. {'VBELN': 'VBELN'}"
    )
    join_type: Literal["inner", "left", "right", "full"] = "inner"
    is_broadcast: bool = Field(
        default=False,
        description="Whether to apply PySpark broadcast join (recommended for master/text tables like MAKT, T001)"
    )


class FilterCondition(BaseModel):
    """Specification of a WHERE filter condition."""
    table: Optional[str] = None
    field: str
    operator: Literal["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL", "BETWEEN"] = "="
    value: Any = None
    raw_expr: Optional[str] = None


class SelectField(BaseModel):
    """Specification of an output field projection."""
    table: Optional[str] = None
    field: str
    alias: Optional[str] = None
    cast_type: Optional[str] = None  # e.g., 'IntegerType()', 'DecimalType(15,2)', 'DateType()'
    transform_rule: Optional[str] = None  # 'date_yyyymmdd', 'sign_shkzg', 'currency_shift', 'custom'
    custom_expr: Optional[str] = None


class AggregationSpec(BaseModel):
    """Specification of post-join aggregations for Gold / reporting layer."""
    group_by: List[str] = Field(default_factory=list)
    aggregations: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of result column alias to aggregate function, e.g. {'total_revenue': 'sum(net_amount)'}"
    )


class TCodeSpec(BaseModel):
    """
    Complete declarative specification for converting an SAP T-Code or report into PySpark.
    """
    tcode: str
    title: str
    description: str
    module: str  # SD, MM, FI, CO, PP, Custom
    base_table: str
    target_table_name: str
    joins: List[JoinCondition] = Field(default_factory=list)
    filters: List[FilterCondition] = Field(default_factory=list)
    fields: List[SelectField] = Field(default_factory=list)
    aggregations: Optional[AggregationSpec] = None
    partition_by: List[str] = Field(default_factory=list)
    target_format: Literal["delta", "parquet"] = "delta"
    source_layer: str = "bronze"  # e.g., 'bronze' or 'raw_sap'
    target_layer: str = "silver"  # e.g., 'silver' or 'gold'

    def get_all_tables(self) -> List[str]:
        """Returns ordered unique list of all source tables used in this spec."""
        tables = [self.base_table]
        for j in self.joins:
            if j.left_table not in tables:
                tables.append(j.left_table)
            if j.right_table not in tables:
                tables.append(j.right_table)
        return tables
