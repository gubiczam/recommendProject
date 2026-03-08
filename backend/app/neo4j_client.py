from __future__ import annotations

from typing import Any

from neo4j import Driver, GraphDatabase


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str) -> None:
        self._driver: Driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self._driver.close()

    def get_random_products(self, limit: int = 12) -> list[dict[str, Any]]:
        query = """
        MATCH (p:Product)
        WITH p, rand() AS randomizer
        ORDER BY randomizer
        LIMIT $limit
        RETURN p {
            .id,
            .name,
            .category,
            .price
        } AS product
        """
        with self._driver.session() as session:
            rows = session.run(query, limit=limit)
            return [row["product"] for row in rows]
