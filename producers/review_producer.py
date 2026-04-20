import json
import time
import uuid
import random
from kafka import KafkaProducer

# Configuration
TOPIC_NAME = 'raw-reviews'
KAFKA_SERVER = '127.0.0.1:9092'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    # Serialization: Dict -> JSON Bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # Key Serialization: String -> Bytes
    key_serializer=lambda v: v.encode('utf-8')
)

def generate_review():
    brands = ['Apple', 'Nike', 'Tesla', 'Bazaarvoice', 'Samsung']
    brand = random.choice(brands)
    
    payload = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "brand_name": brand,
        "user_tier": random.choice(['Premium', 'Basic']),
        "sentiment_score": round(random.uniform(1.0, 5.0), 1),
        "review_text": "The battery life on this is incredible!" if brand == 'Apple' else "Could be better.",
        "timestamp": int(time.time() * 1000)
    }
    return brand, payload

print(f"🚀 SentinelStream Producer started. Sending to {TOPIC_NAME}...")

try:
    while True:
        brand_key, data = generate_review()
        
        # We send the BRAND as the key to ensure ordering per brand
        producer.send(TOPIC_NAME, key=brand_key, value=data)
        
        print(f"Sent: {brand_key} | Score: {data['sentiment_score']} | ID: {data['event_id'][:8]}")
        time.sleep(1) # Send 1 review per second
except KeyboardInterrupt:
    print("\nStopping Producer...")
finally:
    producer.close()