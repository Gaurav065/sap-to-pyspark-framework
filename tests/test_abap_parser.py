"""Unit tests for ABAP OpenSQL query parser."""
import unittest
from sap_to_pyspark.engine.abap_parser import ABAPQueryParser


class TestABAPParser(unittest.TestCase):
    def setUp(self):
        self.parser = ABAPQueryParser()

    def test_clean_abap_sql(self):
        abap = "SELECT vbeln FROM vbak INTO TABLE @DATA(lt_tab) UP TO 50 ROWS."
        cleaned = self.parser.clean_abap_sql(abap)
        self.assertNotIn("@DATA", cleaned)
        self.assertNotIn("UP TO", cleaned)
        self.assertFalse(cleaned.endswith("."))

    def test_parse_sales_order_query(self):
        query = """
        SELECT vbak~vbeln, vbak~erdat, vbap~posnr, vbap~matnr, vbap~kwmeng, kna1~name1
          FROM vbak
          INNER JOIN vbap ON vbap~vbeln = vbak~vbeln
          LEFT OUTER JOIN kna1 ON kna1~kunnr = vbak~kunnr
          WHERE vbak~vbtyp = 'C' AND vbak~netwr > 500
          INTO TABLE @DATA(lt_res).
        """
        spec = self.parser.parse(query, tcode="Z_VA05_CUSTOM", target_table_name="gold_sales_custom")
        self.assertEqual(spec.base_table, "VBAK")
        self.assertEqual(len(spec.joins), 2)
        self.assertEqual(spec.joins[0].right_table, "VBAP")
        self.assertEqual(spec.joins[0].join_type, "inner")
        self.assertEqual(spec.joins[1].right_table, "KNA1")
        self.assertEqual(spec.joins[1].join_type, "left")

        # Check fields
        field_names = [f.field for f in spec.fields]
        self.assertIn("VBELN", field_names)
        self.assertIn("POSNR", field_names)
        self.assertIn("NAME1", field_names)

        # Check filter
        self.assertEqual(len(spec.filters), 2)
        self.assertEqual(spec.filters[0].field, "VBTYP")
        self.assertEqual(spec.filters[0].value, "C")

    def test_parse_fi_query(self):
        query = """
        SELECT bkpf~bukrs, bkpf~belnr, bkpf~gjahr, bseg~buzei, bseg~hkont
          FROM bkpf
          INNER JOIN bseg ON bkpf~bukrs = bseg~bukrs AND bkpf~belnr = bseg~belnr AND bkpf~gjahr = bseg~gjahr
          WHERE bkpf~gjahr = 2026
        """
        spec = self.parser.parse(query, tcode="Z_FI_DOCS")
        self.assertEqual(spec.base_table, "BKPF")
        self.assertEqual(len(spec.joins), 1)
        self.assertEqual(spec.joins[0].right_table, "BSEG")
        self.assertIn("BUKRS", spec.joins[0].join_keys)
        self.assertIn("BELNR", spec.joins[0].join_keys)


if __name__ == "__main__":
    unittest.main()
