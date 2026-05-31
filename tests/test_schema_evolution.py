"""Unit and integration tests for Schema Evolution and Registry integration."""

import os
import time
import unittest
from http.server import HTTPServer
import threading

from common.types import LogEvent
from common.schema_registry import (
    LOG_EVENT_V1_SCHEMA,
    LOG_EVENT_V2_SCHEMA,
    validate_json_schema,
    check_backward_compatibility,
    _global_registry,
    SchemaRegistry,
    SchemaRegistryClient
)
from run import parse_and_deduplicate_event, HealthHandler


class TestSchemaEvolution(unittest.TestCase):

    def setUp(self):
        # Reset global registry between tests
        _global_registry.schemas.clear()
        _global_registry.global_ids.clear()
        _global_registry.next_id = 1
        _global_registry.register("events", LOG_EVENT_V1_SCHEMA)

    def test_schema_validation(self):
        """Test programmatic JSON schema validation."""
        valid_v1 = {
            "event_id": "ev-1",
            "event_time": 1716600000.0,
            "status": 200,
            "schema_version": 1,
            "payload": {"method": "GET"}
        }
        self.assertTrue(validate_json_schema(valid_v1, LOG_EVENT_V1_SCHEMA))

        # Missing required field 'status'
        invalid_v1 = {
            "event_id": "ev-1",
            "event_time": 1716600000.0,
            "schema_version": 1
        }
        self.assertFalse(validate_json_schema(invalid_v1, LOG_EVENT_V1_SCHEMA))

        # Bad field type (status should be integer)
        bad_type_v1 = {
            "event_id": "ev-1",
            "event_time": 1716600000.0,
            "status": "200",
            "schema_version": 1
        }
        self.assertFalse(validate_json_schema(bad_type_v1, LOG_EVENT_V1_SCHEMA))

    def test_compatibility_checking(self):
        """Test backward compatibility checking."""
        # LOG_EVENT_V2_SCHEMA is incompatible with LOG_EVENT_V1_SCHEMA because:
        # 1. status is renamed to http_status (removed required field)
        # 2. service_name is a new required field with no default
        self.assertFalse(check_backward_compatibility(LOG_EVENT_V1_SCHEMA, LOG_EVENT_V2_SCHEMA))

        # An compatible schema: just adding an optional field to V1
        compatible_schema = {
            "type": "object",
            "properties": {
                "event_id": {"type": "string"},
                "event_time": {"type": "number"},
                "status": {"type": "integer"},
                "schema_version": {"type": "integer"},
                "new_optional_field": {"type": "string"}
            },
            "required": ["event_id", "event_time", "status", "schema_version"]
        }
        self.assertTrue(check_backward_compatibility(LOG_EVENT_V1_SCHEMA, compatible_schema))

    def test_in_memory_registry(self):
        """Test in-memory SchemaRegistry operations."""
        registry = SchemaRegistry()
        
        # Register V1
        v1 = registry.register("test-subject", LOG_EVENT_V1_SCHEMA)
        self.assertEqual(v1, 1)

        # Register same schema (should return existing version)
        v1_again = registry.register("test-subject", LOG_EVENT_V1_SCHEMA)
        self.assertEqual(v1_again, 1)

        # Retrieve schemas
        retrieved_v1 = registry.get_version("test-subject", 1)
        self.assertEqual(retrieved_v1, LOG_EVENT_V1_SCHEMA)

        # Get latest
        latest = registry.get_latest("test-subject")
        self.assertEqual(latest, LOG_EVENT_V1_SCHEMA)

        # Get by global ID
        by_id = registry.get_by_id(2)  # global IDs increment; 1 was 'events', 2 was 'test-subject'
        self.assertEqual(by_id, LOG_EVENT_V1_SCHEMA)

    def test_parse_and_deduplicate_compat(self):
        """Test event parsing, backward-compatibility mapping, and seen deduplication."""
        seen_cache = set()

        # 1. V1 Event passes through unchanged (defaults added if missing)
        v1_raw = {"event_id": "ev-1", "event_time": 100.0, "status": 200, "schema_version": 1}
        ev1 = parse_and_deduplicate_event(v1_raw, seen_cache)
        self.assertIsNotNone(ev1)
        self.assertEqual(ev1["schema_version"], 1)
        self.assertEqual(ev1["status"], 200)
        self.assertEqual(ev1["http_status"], 200)  # compatibility wrapper added
        self.assertEqual(ev1["service_name"], "legacy-service")

        # 2. V2 Event mapped back to contain 'status' (required by existing workers)
        v2_raw = {"event_id": "ev-2", "event_time": 105.0, "http_status": 500, "service_name": "auth-service", "schema_version": 2}
        ev2 = parse_and_deduplicate_event(v2_raw, seen_cache)
        self.assertIsNotNone(ev2)
        self.assertEqual(ev2["schema_version"], 2)
        self.assertEqual(ev2["status"], 500)  # mapped status from http_status
        self.assertEqual(ev2["service_name"], "auth-service")

        # 3. Duplicate event ID is discarded (returns None)
        v1_dup = {"event_id": "ev-1", "event_time": 100.0, "status": 200, "schema_version": 1}
        ev_dup = parse_and_deduplicate_event(v1_dup, seen_cache)
        self.assertIsNone(ev_dup)


class TestSchemaRegistryHTTP(unittest.TestCase):
    """Test Schema Registry client-server communication over mock HTTP endpoints."""

    @classmethod
    def setUpClass(cls):
        # Start a local HTTP server hosting the health/schema handler
        cls.server_state = {"monitoring_manager": None}
        
        # Subclass HealthHandler to bind state
        class BindedHandler(HealthHandler):
            server_state = cls.server_state

        cls.httpd = HTTPServer(("127.0.0.1", 0), BindedHandler)
        cls.port = cls.httpd.server_address[1]
        cls.server_thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.server_thread.join()

    def setUp(self):
        # Reset the global in-memory registry which the HealthHandler queries
        _global_registry.schemas.clear()
        _global_registry.global_ids.clear()
        _global_registry.next_id = 1
        _global_registry.register("events", LOG_EVENT_V1_SCHEMA)

    def test_client_registration_and_query(self):
        """Test that SchemaRegistryClient registers schemas and checks compatibility over HTTP."""
        client = SchemaRegistryClient(registry_url=f"http://127.0.0.1:{self.port}")

        # Register schema via client
        v = client.register_schema("http-subject", LOG_EVENT_V1_SCHEMA)
        self.assertEqual(v, 1)  # version is 1 for new subject

        # Get schema from client
        schema = client.get_schema("http-subject", 1)
        self.assertEqual(schema, LOG_EVENT_V1_SCHEMA)

        # Check compatibility
        compatible = client.check_compatibility("http-subject", 1, LOG_EVENT_V2_SCHEMA)
        self.assertFalse(compatible)
