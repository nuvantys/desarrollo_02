import datetime as dt
import unittest

import contifico_pg_backfill as pg


class ConversionTests(unittest.TestCase):
    def test_to_bool_supports_string_flags(self) -> None:
        self.assertTrue(pg.to_bool("True"))
        self.assertFalse(pg.to_bool("false"))
        self.assertIsNone(pg.to_bool("desconocido"))

    def test_parse_timestamp_supports_iso_and_local(self) -> None:
        iso_value = pg.parse_timestamp("2026-03-28T15:28:11")
        local_value = pg.parse_timestamp("28/03/2026 18:13:48")
        assert iso_value is not None
        assert local_value is not None
        self.assertEqual(iso_value.tzinfo, dt.timezone.utc)
        self.assertEqual(local_value.tzinfo, dt.timezone.utc)


class DocumentNormalizationTests(unittest.TestCase):
    def test_derive_document_party_ids(self) -> None:
        persona_id, cliente_id, proveedor_id, vendedor_id = pg.derive_document_party_ids(
            {
                "persona_id": "per-1",
                "tipo_registro": "CLI",
                "cliente": {"id": "cli-1"},
                "vendedor": {"id": "ven-1"},
            }
        )
        self.assertEqual(persona_id, "per-1")
        self.assertEqual(cliente_id, "cli-1")
        self.assertIsNone(proveedor_id)
        self.assertEqual(vendedor_id, "ven-1")

    def test_normalize_documento_records_preserves_nullable_product(self) -> None:
        raw_rows, core_rows = pg.normalize_documento_records(
            records=[
                {
                    "id": "doc-1",
                    "persona_id": "per-1",
                    "tipo_registro": "PRO",
                    "tipo_documento": "FAC",
                    "fecha_emision": "28/03/2026",
                    "detalles": [
                        {"producto_id": None, "cuenta_id": "cta-1", "formula": []},
                    ],
                    "cobros": [{"forma_cobro": "TRANSF", "monto": "10.00"}],
                }
            ],
            run_id="run-1",
            ingested_at=dt.datetime(2026, 3, 28, tzinfo=dt.timezone.utc),
            page_number=1,
            request_params={"page": 1},
            fetched_at=dt.datetime(2026, 3, 28, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(len(raw_rows), 3)
        self.assertEqual(core_rows["documentos"][0]["proveedor_id"], "per-1")
        self.assertIsNone(core_rows["documento_detalles"][0]["producto_id"])
        self.assertEqual(core_rows["documento_detalles"][0]["cuenta_id"], "cta-1")
        self.assertIsNotNone(core_rows["documento_detalles"][0]["formula_jsonb"])


class TicketNormalizationTests(unittest.TestCase):
    def test_normalize_ticket_records_builds_payload_items(self) -> None:
        raw_rows, core_rows = pg.normalize_ticket_records(
            records=[
                {
                    "id": "doc-1",
                    "fecha_emision": "28/03/2026",
                    "detalles": [
                        {
                            "producto_id": "prod-1",
                            "tickets": [{"numero": "A1"}],
                        }
                    ],
                }
            ],
            run_id="run-1",
            ingested_at=dt.datetime(2026, 3, 28, tzinfo=dt.timezone.utc),
            page_number=1,
            request_params={"page": 1},
            fetched_at=dt.datetime(2026, 3, 28, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(len(raw_rows), 3)
        self.assertEqual(len(core_rows["tickets_items"]), 1)
        self.assertIsNotNone(core_rows["tickets_items"][0]["payload_jsonb"])


if __name__ == "__main__":
    unittest.main()
