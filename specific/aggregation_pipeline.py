"""MongoDB Aggregation Pipeline Builder"""

from ..core.connection import Connection
from ..core.cursor import Cursor


class AggregationPipeline:
    """Builder for MongoDB aggregation pipelines"""

    def __init__(self, connection: Connection, collection: str):
        self._conn = connection
        self._collection = collection
        self._pipeline: list[dict] = []

    def match(self, filter: dict) -> 'AggregationPipeline':
        """Add $match stage"""
        self._pipeline.append({"$match": filter})
        return self

    def project(self, projection: dict) -> 'AggregationPipeline':
        """Add $project stage"""
        self._pipeline.append({"$project": projection})
        return self

    def group(self, group: dict) -> 'AggregationPipeline':
        """Add $group stage"""
        self._pipeline.append({"$group": group})
        return self

    def sort(self, sort: dict) -> 'AggregationPipeline':
        """Add $sort stage"""
        self._pipeline.append({"$sort": sort})
        return self

    def limit(self, limit: int) -> 'AggregationPipeline':
        """Add $limit stage"""
        self._pipeline.append({"$limit": limit})
        return self

    def skip(self, skip: int) -> 'AggregationPipeline':
        """Add $skip stage"""
        self._pipeline.append({"$skip": skip})
        return self

    def unwind(self, path: str, **options) -> 'AggregationPipeline':
        """Add $unwind stage"""
        unwind = {"path": path}
        unwind.update(options)
        self._pipeline.append({"$unwind": unwind})
        return self

    def lookup(
            self,
            from_collection: str,
            local_field: str,
            foreign_field: str,
            as_field: str
    ) -> 'AggregationPipeline':
        """Add $lookup stage"""
        self._pipeline.append({
            "$lookup": {
                "from": from_collection,
                "localField": local_field,
                "foreignField": foreign_field,
                "as": as_field
            }
        })
        return self

    def add_fields(self, fields: dict) -> 'AggregationPipeline':
        """Add $addFields stage"""
        self._pipeline.append({"$addFields": fields})
        return self

    def replace_root(self, new_root: dict | str) -> 'AggregationPipeline':
        """Add $replaceRoot stage"""
        self._pipeline.append({"$replaceRoot": {"newRoot": new_root}})
        return self

    def facet(self, facets: dict) -> 'AggregationPipeline':
        """Add $facet stage"""
        self._pipeline.append({"$facet": facets})
        return self

    def count(self, field: str = "count") -> 'AggregationPipeline':
        """Add $count stage"""
        self._pipeline.append({"$count": field})
        return self

    def sample(self, size: int) -> 'AggregationPipeline':
        """Add $sample stage"""
        self._pipeline.append({"$sample": {"size": size}})
        return self

    def build(self) -> list[dict]:
        """Build the aggregation pipeline"""
        return self._pipeline.copy()

    def execute(self) -> Cursor:
        """Execute the aggregation pipeline"""
        cursor = self._conn.execute(
            self._collection,
            "aggregate",
            self._pipeline
        )
        cursor.execute()
        return cursor

    def fetchall(self) -> list[dict]:
        """Execute and fetch all results"""
        return list(self.execute())

    def fetchone(self) -> dict | None:
        """Execute and fetch one result"""
        cursor = self.execute()
        return cursor.fetchone()


def create_pipeline(connection: Connection, collection: str) -> AggregationPipeline:
    """Create an aggregation pipeline builder"""
    return AggregationPipeline(connection, collection)

