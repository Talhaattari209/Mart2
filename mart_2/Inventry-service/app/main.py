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

# Inventory model
class Inventory(BaseModel):
    product_id: int
    quantity: int

# CRUD operations directly using psycopg2
def get_inventory(product_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT quantity FROM inventory WHERE product_id = %s", (product_id,))
            inventory = cur.fetchone()
            if inventory is None:
                raise HTTPException(status_code=404, detail="Inventory not found")
            return inventory[0]  # Return the quantity directly

def update_inventory(product_id: int, quantity: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Validate new inventory level (e.g., ensure it's non-negative)
            if quantity < 0:
                raise HTTPException(status_code=400, detail="Inventory quantity cannot be negative")

            cur.execute("UPDATE inventory SET quantity = %s WHERE product_id = %s", (quantity, product_id))
            conn.commit()

            # Publish inventory_updated event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "inventory_updated", "product_id": product_id, "quantity": quantity})
            producer.flush()

            return {"message": "Inventory updated"}

# Kafka consumer
def consume_inventory_events():
    for message in consumer:
        event_data = message.value
        event_type = event_data["event"]

        if event_type == "inventory_updated":
            pass
            # Handle inventory updates (e.g., trigger notifications, update order status)