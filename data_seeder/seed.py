from __future__ import annotations

import argparse
import os
import random
from typing import Any

from faker import Faker
from neo4j import GraphDatabase


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "password"
DEFAULT_USERS = 10
DEFAULT_PRODUCTS = 50


def build_users(fake: Faker, count: int) -> list[dict[str, Any]]:
    return [{"id": f"user_{i}", "name": fake.name()} for i in range(1, count + 1)]


def build_products(fake: Faker, count: int) -> list[dict[str, Any]]:
    categories = [
        "Electronics",
        "Books",
        "Home",
        "Fashion",
        "Sports",
        "Beauty",
        "Toys",
        "Groceries",
    ]
    products: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        product_name = f"{fake.color_name()} {fake.word().title()} {fake.word().title()}"
        products.append(
            {
                "id": f"product_{i}",
                "name": product_name,
                "category": random.choice(categories),
                "price": round(random.uniform(5.0, 500.0), 2),
            }
        )
    return products


def seed_data(
    uri: str,
    username: str,
    password: str,
    user_count: int,
    product_count: int,
    reset_existing: bool,
) -> None:
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    users = build_users(fake, user_count)
    products = build_products(fake, product_count)

    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        with driver.session() as session:
            session.run(
                "CREATE CONSTRAINT user_id_unique IF NOT EXISTS "
                "FOR (u:User) REQUIRE u.id IS UNIQUE"
            )
            session.run(
                "CREATE CONSTRAINT product_id_unique IF NOT EXISTS "
                "FOR (p:Product) REQUIRE p.id IS UNIQUE"
            )

            if reset_existing:
                session.run("MATCH (u:User) DETACH DELETE u")
                session.run("MATCH (p:Product) DETACH DELETE p")

            session.run(
                """
                UNWIND $users AS user
                MERGE (u:User {id: user.id})
                SET u.name = user.name
                """,
                users=users,
            )

            session.run(
                """
                UNWIND $products AS product
                MERGE (p:Product {id: product.id})
                SET p.name = product.name,
                    p.category = product.category,
                    p.price = product.price
                """,
                products=products,
            )

            counts = session.run(
                """
                MATCH (u:User)
                WITH count(u) AS users
                MATCH (p:Product)
                RETURN users, count(p) AS products
                """
            ).single()
            if counts:
                print(
                    f"Seeding complete. Users: {counts['users']}, Products: {counts['products']}"
                )
    finally:
        driver.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed mock users/products into Neo4j.")
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", DEFAULT_URI))
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", DEFAULT_USER))
    parser.add_argument("--password", default=os.getenv("NEO4J_PASSWORD", DEFAULT_PASSWORD))
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    parser.add_argument("--products", type=int, default=DEFAULT_PRODUCTS)
    parser.add_argument(
        "--no-reset-existing",
        action="store_true",
        help="Keep existing User/Product nodes and only append/merge new rows.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    seed_data(
        uri=args.uri,
        username=args.user,
        password=args.password,
        user_count=args.users,
        product_count=args.products,
        reset_existing=not args.no_reset_existing,
    )
