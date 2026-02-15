# ENHANCED_CRUD_Python_Module.py
# Author: Mark Christof
# Purpose: Reusable CRUD access layer for the AAC MongoDB dataset with database-focused enhancements.
# Notes: Follows industry best practices: clear naming, type hints, docstrings, input validation,
#        exception handling, focused inline comments, and database performance considerations.

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote_plus

from pymongo import MongoClient, errors
from pymongo.collection import Collection


SortSpec = Sequence[Tuple[str, int]]
ProjectionSpec = Optional[Dict[str, int]]


class AnimalShelter:
    """
    CRUD access layer for the AAC MongoDB collection.

    Design choices (best practices):
    - Clear, consistent naming (PascalCase class; snake_case methods/vars).
    - Input validation + explicit returns (predictable True/False, list, or int results).
    - Exceptions handled close to the DB boundary; callers get stable, simple results.
    - Database-minded enhancements: index support and query shaping (projection/limit/sort).
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
            - The course setup creates user 'aacuser' inside the aac database,
              therefore authSource is set to 'aac'.
            - Credentials are URL-encoded so special characters do not break the URI.
        """
        u = quote_plus(username)
        p = quote_plus(password)

        uri = f"mongodb://{u}:{p}@{host}:{port}/{db_name}?authSource=aac"

        try:
            self._client = MongoClient(uri, serverSelectionTimeoutMS=server_selection_timeout_ms)
            self._client.admin.command("ping")  # Fail fast if unreachable/auth is wrong

            self._db = self._client[db_name]
            self._col: Collection = self._db[collection_name]
        except errors.PyMongoError as exc:
            raise RuntimeError(f"Failed to connect to MongoDB: {exc}") from exc

    # --------------------------- HELPERS (A&D + DB) ---------------------------

    def _normalize_filter(self, query: Any) -> Dict[str, Any]:
        """
        Normalize a MongoDB filter into a predictable dictionary.

        Database note:
            - Keeps query handling consistent and prevents runtime errors when non-dicts appear.
            - Read operations allow {} (match all) by design, but destructive operations require
              non-empty filters to avoid accidental broad updates/deletes.
        """
        if query is None:
            return {}
        if isinstance(query, dict):
            return query
        return {}

    def _is_non_empty_dict(self, value: Any) -> bool:
        """Return True only if value is a non-empty dict."""
        return isinstance(value, dict) and len(value) > 0

    def _normalize_limit(self, limit: Optional[int]) -> Optional[int]:
        """
        Normalize a limit value.

        Database note:
            - Avoids accidental full-collection reads when callers pass invalid limits.
        """
        if limit is None:
            return None
        if isinstance(limit, int) and limit > 0:
            return limit
        return None

    def _normalize_sort(self, sort: Optional[SortSpec]) -> Optional[List[Tuple[str, int]]]:
        """
        Normalize sort specification into a safe list of (field, direction).

        Direction should be 1 (ascending) or -1 (descending).
        """
        if sort is None:
            return None
        if not isinstance(sort, (list, tuple)):
            return None

        normalized: List[Tuple[str, int]] = []
        for item in sort:
            if (
                isinstance(item, (list, tuple))
                and len(item) == 2
                and isinstance(item[0], str)
                and item[0].strip() != ""
                and item[1] in (1, -1)
            ):
                normalized.append((item[0], item[1]))

        return normalized or None

    # ------------------------------ DATABASE ENHANCEMENT ------------------------------

    def create_indexes(self) -> bool:
        """
        Create indexes to improve query performance for common dashboard-style filters.

        Returns:
            True if index creation succeeds (or indexes already exist), otherwise False.

        Notes:
            - Index creation is safe to call multiple times.
            - If some fields are not present in a given dataset variant, MongoDB still allows
              index creation; unused indexes simply will not be used by the query planner.
        """
        try:
            # Single-field indexes for common filter/search fields
            self._col.create_index("animal_type")
            self._col.create_index("breed")
            self._col.create_index("sex_upon_outcome")
            self._col.create_index("age_upon_outcome_in_weeks")
            self._col.create_index("outcome_type")
            self._col.create_index("name")

            # Location fields are frequently used for mapping/geo-style dashboards
            self._col.create_index("location_lat")
            self._col.create_index("location_long")

            # A small compound index can help when multiple filters are commonly combined
            self._col.create_index([("animal_type", 1), ("breed", 1)])

            return True
        except errors.PyMongoError:
            return False

    # ------------------------------ CREATE ------------------------------

    def create(self, data: Dict[str, Any]) -> bool:
        """
        Insert a single document.

        Args:
            data: A non-empty dict representing the document.

        Returns:
            True if the insert succeeds; otherwise False.
        """
        if not self._is_non_empty_dict(data):
            return False

        try:
            result = self._col.insert_one(data)
            return bool(result.acknowledged)
        except errors.DuplicateKeyError:
            return False
        except errors.PyMongoError:
            return False

    # ------------------------------- READ -------------------------------

    def read(
        self,
        query: Optional[Dict[str, Any]],
        projection: ProjectionSpec = None,
        limit: Optional[int] = None,
        sort: Optional[SortSpec] = None,
    ) -> List[Dict[str, Any]]:
        """
        Find and return documents matching the filter (uses find()).

        Args:
            query: Mongo-style filter dict. If None/invalid, defaults to {} (match all).
            projection: Optional projection dict (e.g., {"_id": 0, "animal_type": 1}).
                        If not provided, defaults to excluding "_id".
            limit: Optional positive integer limit to reduce result size.
            sort: Optional sort specification like [("age_upon_outcome_in_weeks", 1)].

        Returns:
            List of documents. Returns [] on no matches or any database error.

        Database enhancements:
            - Projection reduces payload size (faster reads).
            - Limit prevents accidental full-collection reads.
            - Sort supports stable ordering for UI or analysis.
        """
        try:
            filter_doc = self._normalize_filter(query)

            # Default projection excludes _id for easier serialization
            proj: Dict[str, int] = projection if isinstance(projection, dict) else {"_id": 0}

            normalized_limit = self._normalize_limit(limit)
            normalized_sort = self._normalize_sort(sort)

            cursor = self._col.find(filter_doc, proj)

            if normalized_sort is not None:
                cursor = cursor.sort(normalized_sort)

            if normalized_limit is not None:
                cursor = cursor.limit(normalized_limit)

            return list(cursor)
        except errors.PyMongoError:
            return []

    # ------------------------------ UPDATE ------------------------------

    def update(self, query: Dict[str, Any], new_values: Dict[str, Any], many: bool = True) -> int:
        """
        Update document(s) in the collection.

        Args:
            query: Non-empty filter dict selecting which docs to update.
            new_values: Non-empty dict of field/value pairs to set.
            many: If True, update all matches; if False, update only one.

        Returns:
            Number of documents modified. Returns 0 on validation failure or DB error.
        """
        filter_doc = self._normalize_filter(query)
        if not self._is_non_empty_dict(filter_doc):
            return 0
        if not self._is_non_empty_dict(new_values):
            return 0

        try:
            operation = self._col.update_many if many else self._col.update_one
            result = operation(filter_doc, {"$set": new_values})
            return result.modified_count
        except errors.PyMongoError:
            return 0

    # ------------------------------ DELETE ------------------------------

    def delete(self, query: Dict[str, Any], many: bool = True) -> int:
        """
        Delete document(s) from the collection.

        Args:
            query: Non-empty filter dict selecting which docs to remove.
            many: If True, delete all matches; if False, delete only one.

        Returns:
            Number of documents deleted. Returns 0 on validation failure or DB error.
        """
        filter_doc = self._normalize_filter(query)
        if not self._is_non_empty_dict(filter_doc):
            return 0

        try:
            operation = self._col.delete_many if many else self._col.delete_one
            result = operation(filter_doc)
            return result.deleted_count
        except errors.PyMongoError:
            return 0

    # ----------------------------- CLEANUP ------------------------------

    def close(self) -> None:
        """Close the MongoDB client connection (safe to call multiple times)."""
        try:
            self._client.close()
        except Exception:
            pass

    def __enter__(self) -> "AnimalShelter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
