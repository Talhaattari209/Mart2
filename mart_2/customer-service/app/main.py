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

# Customer model
class Customer(BaseModel):
    id: int = None
    name: str
    email: str
    address: str

# CRUD operations directly using psycopg2
def create_customer(customer: Customer):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO customers (name, email, address) VALUES (%s, %s, %s)", (customer.name, customer.email, customer.address))
            customer_id = cur.fetchone()[0]
            conn.commit()

            # Publish customer_created event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "customer_created", "customer_id": customer_id})
            producer.flush()

            return customer_id

def get_customer(customer_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE id = %s", (customer_id,))
            customer = cur.fetchone()
            if customer is None:
                raise HTTPException(status_code=404, detail="Customer not found")
            return customer

def update_customer(customer_id: int, customer: Customer):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE customers SET name = %s, email = %s, address = %s WHERE id = %s", (customer.name, customer.email, customer.address, customer_id))
            conn.commit()

            # Publish customer_updated event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "customer_updated", "customer_id": customer_id})
            producer.flush()

            return {"message": "Customer updated"}

def delete_customer(customer_id: int):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
            conn.commit()

            # Publish customer_deleted event to Kafka
            producer.send(KAFKA_TOPIC, value={"event": "customer_deleted", "customer_id": customer_id})
            producer.flush()

            return {"message": "Customer deleted"}

# Kafka consumer
def consume_customer_events():
    for message in consumer:
        event_data = message.value
        event_type = event_data["event"]

        if event_type == "customer_created":
            pass
            # ... (handle customer creation, e.g., send welcome email)
        elif event_type == "customer_updated":
            pass
            # ... (handle customer updates, e.g., update associated orders)
        elif event_type == "customer_deleted":
            pass
            # ... (handle customer deletion, e.g., delete associated orders)