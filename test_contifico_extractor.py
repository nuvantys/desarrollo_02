import tempfile
import unittest
from pathlib import Path

import contifico_extractor as ce


class MergeRowsTests(unittest.TestCase):
    def test_merge_rows_replaces_existing_keys(self) -> None:
        existing = [
            {"id": "1", "nombre": "anterior"},
            {"id": "2", "nombre": "persistente"},
        ]
        new = [
            {"id": "1", "nombre": "nuevo"},
            {"id": "3", "nombre": "agregado"},
        ]
        merged = ce.merge_rows(existing, new, ["id"])
        rows = {row["id"]: row["nombre"] for row in merged}
        self.assertEqual(rows["1"], "nuevo")
        self.assertEqual(rows["2"], "persistente")
        self.assertEqual(rows["3"], "agregado")


class DocumentExtractionTests(unittest.TestCase):
    def test_extract_documentos_splits_tables(self) -> None:
        records = [
            {
                "id": "doc-1",
                "persona_id": "per-1",
                "cliente": {"id": "cli-1", "razon_social": "Cliente"},
                "vendedor": {"id": "ven-1", "razon_social": "Vendedor"},
                "detalles": [
                    {"producto_id": "prod-1", "precio": "10.00"},
                    {"producto_id": None, "cuenta_id": "cta-1", "precio": "5.00"},
                ],
                "cobros": [{"forma_cobro": "TC", "monto": 15}],
            }
        ]
        tables = ce.extract_documentos(records, "run-1", "2026-03-28T00:00:00+00:00")
        self.assertEqual(len(tables["documentos"]), 1)
        self.assertEqual(len(tables["documento_detalles"]), 2)
        self.assertEqual(len(tables["documento_cobros"]), 1)
        doc = tables["documentos"][0]
        self.assertEqual(doc["cliente_id"], "cli-1")
        self.assertEqual(doc["vendedor_obj_id"], "ven-1")
        self.assertNotIn("cliente", doc)
        self.assertNotIn("vendedor", doc)
        detalle = tables["documento_detalles"][1]
        self.assertEqual(detalle["documento_id"], "doc-1")
        self.assertEqual(detalle["detalle_index"], "1")
        self.assertEqual(detalle["cuenta_id"], "cta-1")


class TicketExtractionTests(unittest.TestCase):
    def test_extract_tickets_splits_empty_and_nested_items(self) -> None:
        records = [
            {
                "id": "doc-1",
                "fecha_emision": "28/03/2026",
                "detalles": [
                    {
                        "producto_id": "prod-1",
                        "vendidos": 2,
                        "tickets": [{"numero": "A1"}, {"numero": "A2"}],
                    },
                    {
                        "producto_id": None,
                        "vendidos": 0,
                        "tickets": [],
                    },
                ],
            }
        ]
        tables = ce.extract_tickets(records, "run-1", "2026-03-28T00:00:00+00:00")
        self.assertEqual(len(tables["tickets_documentos"]), 1)
        self.assertEqual(len(tables["tickets_detalles"]), 2)
        self.assertEqual(len(tables["tickets_items"]), 2)
        empty_line = tables["tickets_detalles"][1]
        self.assertEqual(empty_line["producto_id"], "")
        self.assertEqual(empty_line["detalle_index"], "1")


class WindowResolutionTests(unittest.TestCase):
    def test_resolve_window_uses_watermark_with_overlap(self) -> None:
        spec = next(item for item in ce.RESOURCE_SPECS if item.key == "persona")
        watermarks = {
            "persona": {
                "resource": "persona",
                "last_successful_to": "2026-03-28",
            }
        }
        window = ce.resolve_window(
            spec=spec,
            mode="incremental",
            from_date=None,
            to_date=ce.parse_iso_date("2026-03-30"),
            overlap_days=2,
            watermarks=watermarks,
        )
        assert window is not None
        self.assertEqual(window[0].isoformat(), "2026-03-26")
        self.assertEqual(window[1].isoformat(), "2026-03-30")

    def test_build_window_params_uses_resource_specific_names(self) -> None:
        producto = next(item for item in ce.RESOURCE_SPECS if item.key == "producto")
        asiento = next(item for item in ce.RESOURCE_SPECS if item.key == "contabilidad/asiento")
        from_date = ce.parse_iso_date("2026-03-27")
        to_date = ce.parse_iso_date("2026-03-28")
        self.assertEqual(
            ce.build_window_params(producto, from_date, to_date),
            {"fecha_inicio": "2026-03-27", "fecha_fin": "2026-03-28"},
        )
        self.assertEqual(
            ce.build_window_params(asiento, from_date, to_date),
            {"fecha_inicial": "27/03/2026", "fecha_final": "28/03/2026"},
        )

    def test_parse_record_date_supports_iso_prefix_and_eu(self) -> None:
        self.assertEqual(
            ce.parse_record_date("2026-03-28T15:28:11", "iso_prefix"),
            ce.parse_iso_date("2026-03-28"),
        )
        self.assertEqual(
            ce.parse_record_date("28/03/2026", ce.EU_DATE),
            ce.parse_iso_date("2026-03-28"),
        )


class CsvStoreTests(unittest.TestCase):
    def test_merge_or_replace_replaces_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ce.CsvStore(Path(tmp))
            store.merge_or_replace("categorias", [{"id": "1", "nombre": "A"}], replace=True)
            count = store.merge_or_replace(
                "categorias",
                [{"id": "2", "nombre": "B"}],
                replace=True,
            )
            rows = store.load_rows("categorias")
            self.assertEqual(count, 1)
            self.assertEqual(rows[0]["id"], "2")


if __name__ == "__main__":
    unittest.main()
