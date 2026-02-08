# CRUD_Python_Module.py
# Author: Mark Christof
# Purpose: Reusable CRUD class (Module Four scope: Create + Read) for the AAC MongoDB dataset.
# Notes: Follows industry best practices—clear naming, type hints, docstrings, input validation,
#        exception handling, and focused inline comments.

from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from pymongo import MongoClient, errors


class AnimalShelter:
    """
    Create/Read access layer for the AAC MongoDB collection.

    Design choices (best practices):
    - Clear, consistent naming (PascalCase class; snake_case methods/vars).
    - Input validation + explicit returns (True/False or List[...] as appropriate).
    - Exceptions handled close to the DB boundary; callers get stable, simple results.
    - Minimal, purposeful inline comments; full docstrings for methods.
    """

    def __init__(
        self,
        username: str,
        password: str,
        host: str = "localhost",
        port: int = 27017,
        db_name: str = "aac",
        collection_name: str = "animals",
        server_selection_timeout_ms: int = 5000,
    ) -> None:
        """
        Initialize the client connection and bind to the target collection.

        Parameters:
            username: MongoDB username (e.g., "aacuser").
            password: MongoDB password.
            host: MongoDB host (default "localhost").
            port: MongoDB port (default 27017).
            db_name: Target database (default "aac").
            collection_name: Target collection (default "animals").
            server_selection_timeout_ms: Fail-fast timeout for server selection.

        Implementation notes:
            - The course setup creates user 'aacuser' inside the *aac* database.
              Therefore we set authSource to 'aac'.
            - Credentials are URL-encoded so special characters don't break the URI.
        """
        # URL-encode credentials to safely place them in the URI
        u = quote_plus(username)
        p = quote_plus(password)

        # Use authSource=aac because user is stored in the 'aac' database
        uri = f"mongodb://{u}:{p}@{host}:{port}/{db_name}?authSource=aac"

        try:
            self._client = MongoClient(uri, serverSelectionTimeoutMS=server_selection_timeout_ms)
            # Fail fast if server is unreachable or auth is wrong
            self._client.admin.command("ping")

            self._db = self._client[db_name]
            self._col = self._db[collection_name]
        except errors.PyMongoError as exc:
            # Raise a clean, descriptive error for the caller
            raise RuntimeError(f"Failed to connect to MongoDB: {exc}") from exc

    # ------------------------------ CREATE ------------------------------

    def create(self, data: Dict[str, Any]) -> bool:
        """
        Insert a single document.

        Args:
            data: A non-empty dict representing the document.

        Returns:
            True if the insert succeeds; otherwise False (including validation failures).
        """
        # Defensive checks keep the interface predictable for callers
        if not isinstance(data, dict) or not data:
            return False

        try:
            result = self._col.insert_one(data)
            return bool(result.acknowledged)
        except errors.DuplicateKeyError:
            # Primary key collision (e.g., _id already exists)
            return False
        except errors.PyMongoError:
            # Any other database-related error
            return False

    # ------------------------------- READ -------------------------------

    def read(self, query: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find and return documents matching the filter (uses find(), not find_one()).

        Args:
            query: A Mongo-style filter dict. If None/invalid, defaults to {} (all docs).

        Returns:
            A list of documents. Returns an empty list on no matches or any database error.
        """
        try:
            filter_doc: Dict[str, Any] = query if isinstance(query, dict) else {}
            # Hide internal _id unless explicitly needed by caller
            cursor = self._col.find(filter_doc, {"_id": 0})
            return list(cursor)
        except errors.PyMongoError:
            return []
        
            # ------------------------------ UPDATE ------------------------------

    def update(self, query: Dict[str, Any], new_values: Dict[str, Any], many: bool = True) -> int:
        """
        Update document(s) in the collection.

        Args:
            query: Filter dict selecting which docs to update.
            new_values: Dict of field/value pairs to set.
            many: If True, update all matches; if False, only update one.

        Returns:
            The number of documents modified.
        """
        if not isinstance(query, dict) or not query:
            return 0
        if not isinstance(new_values, dict) or not new_values:
            return 0

        try:
            if many:
                result = self._col.update_many(query, {"$set": new_values})
            else:
                result = self._col.update_one(query, {"$set": new_values})
            return result.modified_count
        except errors.PyMongoError:
            return 0

    # ------------------------------ DELETE ------------------------------

    def delete(self, query: Dict[str, Any], many: bool = True) -> int:
        """
        Delete document(s) from the collection.

        Args:
            query: Filter dict selecting which docs to remove.
            many: If True, delete all matches; if False, only delete one.

        Returns:
            The number of documents deleted.
        """
        if not isinstance(query, dict) or not query:
            return 0

        try:
            if many:
                result = self._col.delete_many(query)
            else:
                result = self._col.delete_one(query)
            return result.deleted_count
        except errors.PyMongoError:
            return 0

    # ----------------------------- CLEANUP ------------------------------

    def close(self) -> None:
        """Close the MongoDB client connection (safe to call multiple times)."""
        try:
            self._client.close()
        except Exception:
            # Suppress close-time errors; nothing actionable for the caller here
            pass

    # Context manager support for with-statements
    def __enter__(self) -> "AnimalShelter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
