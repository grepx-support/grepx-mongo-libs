"""Database Migration utilities for MongoDB"""

import os
from datetime import datetime

from .schema import Schema
from ..core.connection import Connection
from ..core.exceptions import ProgrammingError


class Migration:
    """Database migration management for MongoDB"""

    def __init__(self, connection: Connection):
        self._conn = connection
        self._schema = Schema(connection)
        self._ensure_migrations_collection()

    def _ensure_migrations_collection(self) -> None:
        """Ensure migrations collection exists"""
        if not self._schema.collection_exists("schema_migrations"):
            self._schema.create_collection(
                "schema_migrations",
                validator={
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["version"],
                        "properties": {
                            "version": {"bsonType": "string"},
                            "applied_at": {"bsonType": "date"}
                        }
                    }
                }
            )
            # Create index on version
            self._schema.create_index(
                "schema_migrations",
                [("version", 1)],
                unique=True
            )

    def get_applied_migrations(self) -> set[str]:
        """Get list of applied migrations"""
        collection = self._conn.collection("schema_migrations")
        cursor = collection.find({}, {"version": 1})
        return {doc["version"] for doc in cursor}

    def apply_migration(self, version: str, operations: list[dict]) -> None:
        """
        Apply a migration.
        
        Args:
            version: Migration version identifier
            operations: List of MongoDB operations to execute
        """
        applied = self.get_applied_migrations()

        if version in applied:
            return  # Already applied

        try:
            # Execute migration operations
            for op in operations:
                op_type = op.get("type")
                if op_type == "create_collection":
                    self._schema.create_collection(**op.get("params", {}))
                elif op_type == "drop_collection":
                    self._schema.drop_collection(op.get("collection"))
                elif op_type == "create_index":
                    self._schema.create_index(**op.get("params", {}))
                elif op_type == "drop_index":
                    self._schema.drop_index(op.get("collection"), op.get("index"))
                elif op_type == "insert":
                    collection = self._conn.collection(op.get("collection"))
                    collection.insert_many(op.get("documents", []))
                elif op_type == "update":
                    collection = self._conn.collection(op.get("collection"))
                    collection.update_many(
                        op.get("filter", {}),
                        op.get("update", {})
                    )
                elif op_type == "delete":
                    collection = self._conn.collection(op.get("collection"))
                    collection.delete_many(op.get("filter", {}))
                else:
                    raise ProgrammingError(f"Unknown operation type: {op_type}")

            # Record migration
            collection = self._conn.collection("schema_migrations")
            collection.insert_one({
                "version": version,
                "applied_at": datetime.utcnow()
            })
        except Exception as e:
            raise ProgrammingError(f"Migration {version} failed: {e}")

    def apply_migration_file(self, filepath: str) -> None:
        """Apply a migration from a JSON file"""
        import json
        version = os.path.basename(filepath)
        with open(filepath, 'r') as f:
            data = json.load(f)

        version = data.get("version", version)
        operations = data.get("operations", [])
        self.apply_migration(version, operations)

    def rollback_migration(self, version: str) -> None:
        """Rollback a migration (requires rollback operations)"""
        applied = self.get_applied_migrations()

        if version not in applied:
            raise ProgrammingError(f"Migration {version} not applied")

        # Note: Rollback requires separate rollback operation files
        # This is a simplified version that just removes the migration record
        collection = self._conn.collection("schema_migrations")
        collection.delete_one({"version": version})

    def migrate_from_directory(self, directory: str) -> None:
        """Apply all migrations from a directory"""
        applied = self.get_applied_migrations()

        # Get all JSON files in directory
        migration_files = [
            f for f in os.listdir(directory)
            if f.endswith('.json')
        ]
        migration_files.sort()  # Apply in order

        for filename in migration_files:
            filepath = os.path.join(directory, filename)
            self.apply_migration_file(filepath)
