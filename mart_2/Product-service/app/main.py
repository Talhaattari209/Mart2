from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import psycopg2

# Replace with your Neon Postgres connection URL
DATABASE_URL = "postgresql://<your_username>:<your_password>@<your_neon_project_host>:<your_neon_project_port>/<your_database_name>"

# ... (Rest of your code, including Kafka producer and consumer setup)

# CRUD operations directly using psycopg2
def get_products():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM products")
            products = cur.fetchall()
            return products

def get_product(product_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM products WHERE id = %s", (product_id,))
            product = cur.fetchone()
            if product is None:
                raise HTTPException(status_code=404, detail="Product not found")
            return product

def create_product(product: Product):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO products (name, description, price) VALUES (%s, %s, %s)", (product.name, product.description, product.price))
            conn.commit()

            # Publish product_added event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "product_added", "product": product.dict()})
            producer.flush()

            return product

def update_product(product_id: int, product: Product):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE products SET name = %s, description = %s, price = %s WHERE id = %s", (product.name, product.description, product.price, product_id))
            conn.commit()

            # Publish product_updated event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "product_updated", "product_id": product_id})
            producer.flush()

            return product

def delete_product(product_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM products WHERE id = %s", (product_id,))
            conn.commit()

            # Publish product_deleted event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "product_deleted", "product_id": product_id})
            producer.flush()

            return {"message": "Product deleted"}

# Kafka consumer
def consume_product_events():
    for message in consumer:
        event_data = message.value
        event_type = event_data["event"]

        if event_type == "product_added":
            # Add the new product to the database
            create_product(Product(**event_data["product"]))
        elif event_type == "product_deleted":
            # Remove the deleted product from the database
            delete_product(event_data["product_id"])