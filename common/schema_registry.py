"""Schema Registry and client integration for Schema Evolution."""

import json
import logging
import urllib.request
import urllib.error

logger = logging.getLogger("schema_registry")

# ---------------------------------------------------------------------------
# Core Schema Definitions
# ---------------------------------------------------------------------------

LOG_EVENT_V1_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "event_time": {"type": "number"},
        "status": {"type": "integer"},
        "schema_version": {"type": "integer"},
        "payload": {"type": "object"}
    },
    "required": ["event_id", "event_time", "status", "schema_version"]
}

# V2 contains a breaking change: status renamed to http_status, and required service_name added
LOG_EVENT_V2_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "event_time": {"type": "number"},
        "http_status": {"type": "integer"},
        "service_name": {"type": "string"},
        "schema_version": {"type": "integer"},
        "payload": {"type": "object"}
    },
    "required": ["event_id", "event_time", "http_status", "service_name", "schema_version"]
}

WINDOW_RESULT_V1_SCHEMA = {
    "type": "object",
    "properties": {
        "window_id": {"type": "string"},
        "partition_id": {"type": "integer"},
        "window_start": {"type": "number"},
        "window_end": {"type": "number"},
        "count": {"type": "integer"},
        "status_500": {"type": "integer"},
        "is_speculative": {"type": "boolean"},
        "version": {"type": "integer"},
        "schema_version": {"type": "integer"}
    },
    "required": ["window_id", "partition_id", "window_start", "window_end", "count", "status_500", "is_speculative", "schema_version"]
}

# V2 of WindowResult adds an optional target_sla field
WINDOW_RESULT_V2_SCHEMA = {
    "type": "object",
    "properties": {
        "window_id": {"type": "string"},
        "partition_id": {"type": "integer"},
        "window_start": {"type": "number"},
        "window_end": {"type": "number"},
        "count": {"type": "integer"},
        "status_500": {"type": "integer"},
        "is_speculative": {"type": "boolean"},
        "version": {"type": "integer"},
        "schema_version": {"type": "integer"},
        "target_sla": {"type": "string"}
    },
    "required": ["window_id", "partition_id", "window_start", "window_end", "count", "status_500", "is_speculative", "schema_version"]
}


# ---------------------------------------------------------------------------
# Programmatic JSON Validator
# ---------------------------------------------------------------------------

def validate_json_schema(data: dict, schema: dict) -> bool:
    """Validate JSON data against a basic schema structure without external deps."""
    if not isinstance(data, dict):
        return False
    
    # Check required fields
    for req in schema.get("required", []):
        if req not in data:
            logger.debug("Validation failed: missing required field '%s'", req)
            return False
            
    # Check field types
    properties = schema.get("properties", {})
    for key, val in data.items():
        if key in properties:
            prop_def = properties[key]
            t = prop_def.get("type")
            if t == "string" and not isinstance(val, str):
                return False
            elif t == "integer":
                if isinstance(val, bool) or not isinstance(val, int):
                    return False
            elif t == "number" and not isinstance(val, (int, float)):
                return False
            elif t == "boolean" and not isinstance(val, bool):
                return False
            elif t == "object" and not isinstance(val, dict):
                return False
            elif t == "array" and not isinstance(val, list):
                return False
    return True


# ---------------------------------------------------------------------------
# Schema Compatibility Checker
# ---------------------------------------------------------------------------

def check_backward_compatibility(old_schema: dict, new_schema: dict) -> bool:
    """Check if new_schema is backward compatible with old_schema.
    
    Backward compatibility: Consumers with old_schema can read data produced by new_schema.
    Rules:
    - New schema must NOT remove fields that are required in the old schema.
    - New schema must NOT change the type of existing fields.
    - Any new required fields in the new schema must have default values (not supported here,
      so any new required field makes it incompatible).
    """
    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})
    old_req = old_schema.get("required", [])
    new_req = new_schema.get("required", [])
    
    # 1. New schema must not remove required fields of old schema
    for req in old_req:
        if req not in new_props:
            logger.debug("Incompatible: required field '%s' removed in new schema", req)
            return False
            
    # 2. Type of existing fields must not change
    for key, old_prop in old_props.items():
        if key in new_props:
            new_prop = new_props[key]
            if old_prop.get("type") != new_prop.get("type"):
                logger.debug("Incompatible: field '%s' type changed from '%s' to '%s'",
                             key, old_prop.get("type"), new_prop.get("type"))
                return False
                
    # 3. New required fields must not be added (unless they had defaults, which we don't store)
    for req in new_req:
        if req not in old_props:
            logger.debug("Incompatible: new required field '%s' added without a fallback", req)
            return False
            
    return True


# ---------------------------------------------------------------------------
# Schema Registry Server Model (Embedded in Coordinator/Memory)
# ---------------------------------------------------------------------------

class SchemaRegistry:
    """In-memory Schema Registry for tracking and validating schemas."""

    def __init__(self):
        # subject -> list of schemas (index = version - 1)
        self.schemas: dict[str, list[dict]] = {}
        # global id -> (subject, version)
        self.global_ids: dict[int, tuple[str, int]] = {}
        self.next_id = 1

        # Register standard default schemas
        self.register("events", LOG_EVENT_V1_SCHEMA)
        # Register v2 as a separate step or forced bypass if needed

    def register(self, subject: str, schema: dict) -> int:
        """Register a schema under a subject. Increments version."""
        if subject not in self.schemas:
            self.schemas[subject] = []
            
        versions = self.schemas[subject]
        
        # Check if already registered
        for v_idx, existing in enumerate(versions):
            if existing == schema:
                return v_idx + 1
                
        # Perform backward compatibility check against latest version if any
        if versions:
            latest = versions[-1]
            if not check_backward_compatibility(latest, schema):
                logger.warning("Schema registered under '%s' is NOT backward-compatible", subject)
                
        versions.append(schema)
        version = len(versions)
        schema_id = self.next_id
        self.next_id += 1
        self.global_ids[schema_id] = (subject, version)
        return version

    def get_latest(self, subject: str) -> dict | None:
        versions = self.schemas.get(subject, [])
        return versions[-1] if versions else None

    def get_version(self, subject: str, version: int) -> dict | None:
        versions = self.schemas.get(subject, [])
        if 1 <= version <= len(versions):
            return versions[version - 1]
        return None

    def get_by_id(self, schema_id: int) -> dict | None:
        ref = self.global_ids.get(schema_id)
        if ref:
            return self.get_version(ref[0], ref[1])
        return None


# Global in-memory instance for standalone local test execution fallback
_global_registry = SchemaRegistry()


# ---------------------------------------------------------------------------
# Schema Registry HTTP Client
# ---------------------------------------------------------------------------

class SchemaRegistryClient:
    """Client for communicating with the Schema Registry HTTP endpoints."""

    def __init__(self, registry_url: str = None):
        self.registry_url = registry_url.rstrip("/") if registry_url else None
        # Cache to reduce network roundtrips
        self._cache: dict[str, dict[int, dict]] = {}

    def register_schema(self, subject: str, schema: dict) -> int:
        """Register schema and return its version integer."""
        if not self.registry_url:
            # Fallback to local in-memory registry
            return _global_registry.register(subject, schema)

        url = f"{self.registry_url}/schemas/subjects/{subject}/versions"
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps({"schema": schema}).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                return res.get("version", 1)
        except Exception as e:
            logger.warning("SchemaRegistryClient: registration failed for '%s' (%s), using local fallback", subject, e)
            return _global_registry.register(subject, schema)

    def get_schema(self, subject: str, version: int) -> dict | None:
        """Fetch a specific schema version."""
        if not self.registry_url:
            return _global_registry.get_version(subject, version)

        # Check cache
        if subject in self._cache and version in self._cache[subject]:
            return self._cache[subject][version]

        url = f"{self.registry_url}/schemas/subjects/{subject}/versions/{version}"
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                schema = res.get("schema")
                if schema:
                    if subject not in self._cache:
                        self._cache[subject] = {}
                    self._cache[subject][version] = schema
                return schema
        except Exception as e:
            logger.warning("SchemaRegistryClient: failed to fetch '%s' v%d (%s), using local fallback", subject, version, e)
            return _global_registry.get_version(subject, version)

    def check_compatibility(self, subject: str, version: int, schema: dict) -> bool:
        """Check compatibility of schema against specified version."""
        if not self.registry_url:
            old = _global_registry.get_version(subject, version)
            if not old:
                return True
            return check_backward_compatibility(old, schema)

        url = f"{self.registry_url}/compatibility/subjects/{subject}/versions/{version}"
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps({"schema": schema}).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                return res.get("compatible", False)
        except Exception as e:
            logger.warning("SchemaRegistryClient: compatibility check failed: %s, checking locally", e)
            old = _global_registry.get_version(subject, version)
            if not old:
                return True
            return check_backward_compatibility(old, schema)
