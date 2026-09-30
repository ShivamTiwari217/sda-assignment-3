import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC_NAME = "market-ticks"
BOOTSTRAP_SERVERS = ["localhost:9092"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TICKERS = {
    "RELIANCE": 2980.0,
    "TCS": 3950.0,
    "INFY": 1820.0
}

print(f"Starting producer streaming to topic: {TOPIC_NAME}")

try:
    while True:
        ticker = random.choice(list(TICKERS.keys()))
        current_base = TICKERS[ticker]
        
        # Simulate tick movement
        delta = round(random.uniform(-1.5, 1.5), 2)
        price = round(max(10.0, current_base + delta), 2)
        TICKERS[ticker] = price
        
        tick = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "price": price,
            "volume": random.randint(10, 500),
            "bid": round(price - random.uniform(0.05, 0.30), 2),
            "ask": round(price + random.uniform(0.05, 0.30), 2)
        }
        
        producer.send(TOPIC_NAME, value=tick)
        print(f"[SENT] {tick['timestamp']} | {tick['ticker']} | Price: {tick['price']} | Vol: {tick['volume']}")
        time.sleep(1.0)
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.flush()
    producer.close()