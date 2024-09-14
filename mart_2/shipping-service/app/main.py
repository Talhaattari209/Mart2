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

# Shipment model
class Shipment(BaseModel):
    order_id: int
    shipping_address: str
    carrier: str
    status: str = "pending"

# CRUD operations directly using psycopg2
def create_shipment(shipment: Shipment):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Calculate shipping costs (e.g., based on shipping address and weight)
            shipping_cost = calculate_shipping_cost(shipment.shipping_address)

            # Insert shipment into database
            cur.execute("INSERT INTO shipments (order_id, shipping_address, carrier, status, shipping_cost) VALUES (%s, %s, %s, %s, %s)", (shipment.order_id, shipment.shipping_address, shipment.carrier, shipment.status, shipping_cost))
            shipment_id = cur.fetchone()[0]
            conn.commit()

            # Publish shipment_created event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "shipment_created", "shipment_id": shipment_id})
            producer.flush()

            return shipment_id

def get_shipment(shipment_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shipments WHERE id = %s", (shipment_id,))
            shipment = cur.fetchone()
            if shipment is None:
                raise HTTPException(status_code=404, detail="Shipment not found")
            return shipment

def update_shipment_status(shipment_id: int, status: str):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE shipments SET status = %s WHERE id = %s", (status, shipment_id))
            conn.commit()

            # Publish shipment_status_updated event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "shipment_status_updated", "shipment_id": shipment_id, "status": status})
            producer.flush()

            return {"message": "Shipment status updated"}

# Kafka consumer
def consume_shipment_events():
    for message in consumer:
        event_data = message.value
        event_type = event_data["event"]

        if event_type == "shipment_created":
            pass
            # ... (handle shipment creation, e.g., update order status)
        elif event_type == "shipment_status_updated":
            pass
            # ... (handle shipment status updates, e.g., notify customer)