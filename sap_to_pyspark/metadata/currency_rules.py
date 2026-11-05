"""
SAP Domain Rules: Currencies, Signs, and Dates
Handles SAP-specific technical quirks:
1. Implicit currency decimals (TCURX overrides, e.g. JPY, KWD).
2. Debit/Credit sign reversal (SHKZG 'S' vs 'H').
3. Date format and zero-date handling (DATS '00000000' -> NULL).
"""

from typing import Dict, List, Optional


class CurrencyRules:
    """Manages SAP currency decimal places (TCURX table logic)."""

    # Non-standard decimals mapping: Currency -> decimal places (Standard SAP default is 2)
    CURRENCY_DECIMALS: Dict[str, int] = {
        # 0 decimals (stored with implicit factor 100 in SAP, must divide by 100)
        "JPY": 0, "KRW": 0, "VND": 0, "CLP": 0, "IDR": 0, "HUF": 0, "PYG": 0, "RWF": 0, "UGX": 0,
        # 3 decimals (stored with implicit factor 100 in SAP, must multiply by 10)
        "KWD": 3, "BHD": 3, "OMR": 3, "TND": 3, "JOD": 3, "IQD": 3, "LYD": 3,
    }

    @classmethod
    def get_factor(cls, currency: str) -> float:
        """Returns multiplier factor to correct SAP raw stored value."""
        curr = currency.upper().strip()
        decimals = cls.CURRENCY_DECIMALS.get(curr, 2)
        # In SAP DB, values are stored as if they have 2 decimals.
        # If true decimals = 0, raw 100 means 100 Yen, but DB stores 1.00 -> factor = 100 (or raw is 10000 -> divide by 100)
        # Specifically in SAP: if CURR field stores raw number with 2 decimal places:
        # For JPY: raw amount 5000 is stored as 50.00. Factor to get actual currency is 100.0.
        # For KWD: raw amount 50.125 is stored as 501.25. Factor is 0.1.
        if decimals == 0:
            return 100.0
        elif decimals == 3:
            return 0.1
        return 1.0

    @classmethod
    def spark_currency_adjustment_expr(cls, amount_col: str, currency_col: str) -> str:
        """Generates PySpark SQL expression to correct SAP currency shift."""
        zero_decimal_list = ", ".join(f"'{c}'" for c, d in cls.CURRENCY_DECIMALS.items() if d == 0)
        three_decimal_list = ", ".join(f"'{c}'" for c, d in cls.CURRENCY_DECIMALS.items() if d == 3)

        return (
            f"F.when(F.col({currency_col}).isin({zero_decimal_list}), F.col({amount_col}) * 100.0)\n"
            f" .when(F.col({currency_col}).isin({three_decimal_list}), F.col({amount_col}) * 0.1)\n"
            f" .otherwise(F.col({amount_col}))"
        )


class SignRules:
    """Manages SAP Debit/Credit (SHKZG) indicator conversions."""

    @classmethod
    def spark_signed_amount_expr(cls, amount_col: str, sign_col: str = "SHKZG") -> str:
        """
        Generates PySpark expression where Credit ('H') is negative and Debit ('S') is positive.
        """
        return f"F.when(F.col('{sign_col}') == 'H', -F.col('{amount_col}')).otherwise(F.col('{amount_col}'))"


class DateRules:
    """Manages SAP DATS string format conversions to Spark DateType."""

    @classmethod
    def spark_date_expr(cls, date_col: str, target_alias: Optional[str] = None) -> str:
        """
        Converts SAP 'YYYYMMDD' string to DateType, mapping '00000000' or empty strings to NULL.
        """
        alias_str = f".alias('{target_alias}')" if target_alias else ""
        return (
            f"F.when((F.col('{date_col}').isNull()) | (F.col('{date_col}') == '00000000') | (F.col('{date_col}') == ''), F.lit(None))\n"
            f" .otherwise(F.to_date(F.col('{date_col}'), 'yyyyMMdd')){alias_str}"
        )
