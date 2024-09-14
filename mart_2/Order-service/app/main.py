from kafka import KafkaProducer

# Replace with your Kafka broker address
KAFKA_BROKERS = "localhost:9092"

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

from kafka import KafkaConsumer

# Create a Kafka consumer
consumer = KafkaConsumer("order_events", bootstrap_servers=KAFKA_BROKERS, value_deserializer=lambda v: json.loads(v.decode('utf-8')))



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

# Product model (assuming you have a separate product-service)
class Product(BaseModel):
    id: int
    name: str
    price: float

# Order model
class Order(BaseModel):
    id: int = None  # Optional for new orders
    customer_id: int
    products: List[Product]
    shipping_address: str
    payment_method: str
    status: str = "pending"

# CRUD operations directly using psycopg2
def create_order(order: Order):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Calculate total price
            total_price = sum(product.price for product in order.products)

            # Insert order into database
            cur.execute("INSERT INTO orders (customer_id, products, shipping_address, payment_method, total_price, status) VALUES (%s, %s, %s, %s, %s, %s)", (order.customer_id, json.dumps(order.products), order.shipping_address, order.payment_method, total_price, order.status))
            order_id = cur.fetchone()[0]
            conn.commit()

            # Publish order_created event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "order_created", "order_id": order_id})
            producer.flush()

            return order_id

def get_order(order_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
            order = cur.fetchone()
            if order is None:
                raise HTTPException(status_code=404, detail="Order not found")

            # Convert products from JSON to list
            order["products"] = json.loads(order["products"])
            return order

def update_order_status(order_id: int, status: str):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE orders SET status = %s WHERE id = %s", (status, order_id))
            conn.commit()

            # Publish order_status_updated event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "order_status_updated", "order_id": order_id, "status": status})
            producer.flush()

            return {"message": "Order status updated"}

def cancel_order(order_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))
            conn.commit()

            # Publish order_canceled event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "order_canceled", "order_id": order_id})
            producer.flush()

            return {"message": "Order canceled"}

# Kafka consumer
def consume_order_events():
    for message in consumer:
        event_data = message.value
        event_type = event_data["event"]

        # Handle events (e.g., trigger email notifications, update inventory)
        if event_type == "order_created":
            pass # print message
        elif event_type == "order_status_updated":
            pass # print message
        elif event_type == "order_canceled":
            pass # print message