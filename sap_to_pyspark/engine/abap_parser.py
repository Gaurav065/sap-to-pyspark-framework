"""
ABAP OpenSQL and SQL Parser for SAP T-Code Query Translation
Parses ABAP OpenSQL statements (including table~column syntax, INTO TABLE, host variables)
and extracts structured AST into a TCodeSpec model.
"""

import re
from typing import Dict, List, Tuple, Optional
from sap_to_pyspark.engine.models import (
    TCodeSpec,
    JoinCondition,
    FilterCondition,
    SelectField,
)
from sap_to_pyspark.metadata.data_dictionary import SAPDataDictionary


class ABAPQueryParser:
    """Parses ABAP OpenSQL / Standard SQL queries into a TCodeSpec."""

    BROADCAST_TABLE_CANDIDATES = {"MAKT", "T001", "T001W", "SKAT", "MARA"}

    def __init__(self, data_dict: Optional[SAPDataDictionary] = None):
        self.data_dict = data_dict or SAPDataDictionary()

    def clean_abap_sql(self, query: str) -> str:
        """Removes ABAP-specific syntax like @DATA, INTO TABLE, UP TO n ROWS, trailing periods."""
        q = query.strip()
        # Remove trailing period common in ABAP statements
        if q.endswith("."):
            q = q[:-1].strip()

        # Remove INTO CORRESPONDING FIELDS OF TABLE @DATA(...) or similar
        q = re.sub(r"\s+INTO\s+(?:CORRESPONDING\s+FIELDS\s+OF\s+)?(?:TABLE\s+)?@?[A-Za-z0-9_()]+", "", q, flags=re.IGNORECASE)
        # Remove UP TO n ROWS
        q = re.sub(r"\s+UP\s+TO\s+\d+\s+ROWS", "", q, flags=re.IGNORECASE)
        # Remove PACKAGE SIZE n
        q = re.sub(r"\s+PACKAGE\s+SIZE\s+\d+", "", q, flags=re.IGNORECASE)
        # Remove host variable @ symbol before variables (e.g. @lv_date -> '20230101' or lv_date)
        q = re.sub(r"@([A-Za-z0-9_]+)", r"\1", q)

        return q

    def parse(
        self,
        query: str,
        tcode: str = "CUSTOM_Z",
        title: str = "Custom ABAP Query Translation",
        target_table_name: str = "custom_sap_report"
    ) -> TCodeSpec:
        """
        Parses an ABAP OpenSQL query and constructs a TCodeSpec.
        """
        clean_q = self.clean_abap_sql(query)

        # Match SELECT ... FROM ... [JOIN...] [WHERE...]
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM\s+(.*)", clean_q, flags=re.IGNORECASE | re.DOTALL)
        if not select_match:
            raise ValueError("Invalid SQL: Must contain SELECT and FROM clauses")

        select_part = select_match.group(1).strip()
        rest_part = select_match.group(2).strip()

        # Separate WHERE clause if present
        where_match = re.search(r"^(.*?)\s+WHERE\s+(.*)$", rest_part, flags=re.IGNORECASE | re.DOTALL)
        if where_match:
            from_join_part = where_match.group(1).strip()
            where_part = where_match.group(2).strip()
        else:
            from_join_part = rest_part
            where_part = ""

        # Parse FROM and JOINS
        base_table, table_aliases, joins = self._parse_from_and_joins(from_join_part)

        # Parse SELECT fields
        fields = self._parse_select_fields(select_part, base_table, table_aliases)

        # Parse WHERE filters
        filters = self._parse_where_filters(where_part, table_aliases)

        return TCodeSpec(
            tcode=tcode,
            title=title,
            description=f"Generated from ABAP query on base table {base_table}",
            module="Custom",
            base_table=base_table,
            target_table_name=target_table_name,
            joins=joins,
            filters=filters,
            fields=fields,
            target_format="delta",
        )

    def _parse_from_and_joins(
        self, from_join_part: str
    ) -> Tuple[str, Dict[str, str], List[JoinCondition]]:
        """Parses the FROM table and JOIN clauses."""
        table_aliases: Dict[str, str] = {}  # alias -> real_table_name
        joins: List[JoinCondition] = []

        # Tokenize by JOIN
        tokens = re.split(
            r"\s+(INNER\s+JOIN|LEFT\s+OUTER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN)\s+",
            from_join_part,
            flags=re.IGNORECASE
        )

        base_clause = tokens[0].strip()
        base_parts = base_clause.split()
        base_table = base_parts[0].upper()
        if len(base_parts) > 1 and base_parts[1].upper() != "AS":
            table_aliases[base_parts[1].upper()] = base_table
        elif len(base_parts) > 2 and base_parts[1].upper() == "AS":
            table_aliases[base_parts[2].upper()] = base_table
        else:
            table_aliases[base_table] = base_table

        # Process join pairs
        i = 1
        while i < len(tokens):
            join_type_raw = tokens[i].upper()
            join_clause = tokens[i + 1].strip()
            i += 2

            join_type = "inner"
            if "LEFT" in join_type_raw:
                join_type = "left"
            elif "RIGHT" in join_type_raw:
                join_type = "right"
            elif "FULL" in join_type_raw:
                join_type = "full"

            # Parse target table and ON condition
            # Format: table_name [AS alias] ON left_key = right_key [AND ...]
            on_match = re.search(r"^(.*?)\s+ON\s+(.*)$", join_clause, flags=re.IGNORECASE)
            if not on_match:
                continue

            tbl_part = on_match.group(1).strip().split()
            target_table = tbl_part[0].upper()
            target_alias = target_table
            if len(tbl_part) > 1 and tbl_part[1].upper() != "AS":
                target_alias = tbl_part[1].upper()
            elif len(tbl_part) > 2 and tbl_part[1].upper() == "AS":
                target_alias = tbl_part[2].upper()
            table_aliases[target_alias] = target_table

            on_conditions_str = on_match.group(2).strip()
            # Split by AND
            cond_pairs = re.split(r"\s+AND\s+", on_conditions_str, flags=re.IGNORECASE)
            join_keys: Dict[str, str] = {}
            for cond in cond_pairs:
                parts = cond.split("=")
                if len(parts) == 2:
                    left_key_full = self._normalize_col(parts[0].strip())
                    right_key_full = self._normalize_col(parts[1].strip())
                    left_col = left_key_full.split(".")[-1].upper()
                    right_col = right_key_full.split(".")[-1].upper()
                    join_keys[left_col] = right_col

            is_broadcast = target_table in self.BROADCAST_TABLE_CANDIDATES

            joins.append(
                JoinCondition(
                    left_table=base_table,
                    right_table=target_table,
                    join_keys=join_keys if join_keys else {"MANDT": "MANDT"},
                    join_type=join_type,
                    is_broadcast=is_broadcast,
                )
            )

        return base_table, table_aliases, joins

    def _normalize_col(self, col: str) -> str:
        """Converts ABAP table~col into standard table.col format."""
        return col.replace("~", ".").strip()

    def _parse_select_fields(
        self, select_part: str, base_table: str, table_aliases: Dict[str, str]
    ) -> List[SelectField]:
        """Parses SELECT expressions into SelectField models."""
        fields: List[SelectField] = []
        raw_items = [s.strip() for s in select_part.split(",") if s.strip()]

        for item in raw_items:
            # Check for AS alias
            as_match = re.search(r"^(.*?)\s+(?:AS\s+)?([A-Za-z0-9_]+)$", item, flags=re.IGNORECASE)
            if as_match and not ("(" in item and not item.endswith(")")):
                expr = as_match.group(1).strip()
                alias = as_match.group(2).strip()
            else:
                expr = item
                alias = None

            norm_expr = self._normalize_col(expr)
            table_name = base_table
            field_name = norm_expr

            if "." in norm_expr:
                prefix, fname = norm_expr.split(".", 1)
                field_name = fname.upper()
                table_name = table_aliases.get(prefix.upper(), prefix.upper())
            else:
                field_name = norm_expr.upper()

            # Automatic alias from data dictionary if none provided
            if not alias:
                alias = self.data_dict.suggest_english_alias(table_name, field_name)

            # Check format rules
            finfo = self.data_dict.get_field_info(table_name, field_name)
            transform_rule = finfo.format_rule if finfo else None
            cast_type = finfo.spark_type if finfo else None

            fields.append(
                SelectField(
                    table=table_name,
                    field=field_name,
                    alias=alias,
                    cast_type=cast_type,
                    transform_rule=transform_rule,
                )
            )

        return fields

    def _parse_where_filters(
        self, where_part: str, table_aliases: Dict[str, str]
    ) -> List[FilterCondition]:
        """Parses WHERE clauses into FilterCondition list."""
        filters: List[FilterCondition] = []
        if not where_part:
            return filters

        # Split on AND
        conditions = re.split(r"\s+AND\s+", where_part, flags=re.IGNORECASE)
        for cond in conditions:
            cond = cond.strip()
            norm_cond = self._normalize_col(cond)

            # Match: table.field operator value
            op_pattern = r"([A-Za-z0-9_.]+)\s*(=|!=|<>|>=|<=|>|<|LIKE|IN)\s*(.*)"
            m = re.match(op_pattern, norm_cond, flags=re.IGNORECASE)
            if m:
                field_part = m.group(1).strip()
                op = m.group(2).strip().upper()
                if op == "<>":
                    op = "!="
                val = m.group(3).strip()

                table = None
                fname = field_part
                if "." in field_part:
                    t_alias, fname = field_part.split(".", 1)
                    table = table_aliases.get(t_alias.upper(), t_alias.upper())

                # Strip quotes if string literal
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]

                filters.append(
                    FilterCondition(
                        table=table,
                        field=fname.upper(),
                        operator=op,
                        value=val,
                    )
                )
            else:
                filters.append(
                    FilterCondition(
                        field="RAW",
                        raw_expr=norm_cond,
                    )
                )

        return filters
