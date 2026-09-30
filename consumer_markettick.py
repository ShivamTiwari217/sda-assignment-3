import json
import csv
import os
from kafka import KafkaConsumer

TOPIC_NAME = "market-ticks"
FILE_PATH = "market_data.csv"
FIELDNAMES = ["timestamp", "ticker", "price"]

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest"
)

# Check if file exists so we only write the header once
file_exists = os.path.isfile(FILE_PATH)

with open(FILE_PATH, mode="a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
    
    if not file_exists:
        writer.writeheader()
        
    print(f"Consuming from '{TOPIC_NAME}' and saving to {FILE_PATH}...")
    
    try:
        for message in consumer:
            writer.writerow(message.value)
            f.flush() # Force write to disk immediately
            print(f"Saved: {message.value['timestamp']} | {message.value['ticker']} @ {message.value['price']}")
    except KeyboardInterrupt:
        print("\nConsumer stopped.")