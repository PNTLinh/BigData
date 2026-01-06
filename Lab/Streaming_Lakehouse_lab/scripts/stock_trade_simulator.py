#!/usr/bin/env python3
"""
Stock Trade Simulator
Generates realistic stock trade events for Kafka
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "stock-trades"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX"]
EXCHANGES = ["NASDAQ", "NYSE"]

def generate_trade_event(trade_id, symbol, base_price=None):
    """Generate a realistic stock trade event"""
    if base_price is None:
        base_price = random.uniform(100, 500)
    
    # Price movement (small random change)
    price_change = random.uniform(-0.02, 0.02)
    price = round(base_price * (1 + price_change), 2)
    
    # Volume (random)
    volume = random.randint(10, 1000)
    
    # Trade type
    trade_type = random.choice(["BUY", "SELL"])
    
    return {
        "trade_id": f"TRD_{trade_id:06d}",
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "timestamp": datetime.now().isoformat(),
        "trade_type": trade_type,
        "exchange": random.choice(EXCHANGES)
    }

def generate_quote_event(quote_id, symbol, base_price=None):
    """Generate a stock quote event (bid/ask)"""
    if base_price is None:
        base_price = random.uniform(100, 500)
    
    spread = random.uniform(0.01, 0.10)
    mid_price = base_price
    bid = round(mid_price - spread/2, 2)
    ask = round(mid_price + spread/2, 2)
    
    return {
        "quote_id": f"QTE_{quote_id:06d}",
        "symbol": symbol,
        "bid": bid,
        "ask": ask,
        "timestamp": datetime.now().isoformat(),
        "exchange": random.choice(EXCHANGES)
    }

def setup_kafka_topic():
    """Create Kafka topic if it doesn't exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='stock-simulator-admin'
        )
        
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME not in existing_topics:
            topic = NewTopic(
                name=TOPIC_NAME,
                num_partitions=3,
                replication_factor=1
            )
            admin_client.create_topics([topic])
            print(f"✅ Created topic: {TOPIC_NAME}")
        else:
            print(f"ℹ️  Topic already exists: {TOPIC_NAME}")
    except Exception as e:
        print(f"⚠️  Error setting up topic: {e}")
        print("   Topic may be auto-created or already exists")

def run_simulator(events_per_second=5, duration_seconds=60):
    """Run stock trade simulator"""
    setup_kafka_topic()
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print(f"🚀 Starting stock trade simulator...")
    print(f"   Events per second: {events_per_second}")
    print(f"   Duration: {duration_seconds} seconds")
    print(f"   Topic: {TOPIC_NAME}")
    
    trade_id = 0
    quote_id = 0
    base_prices = {symbol: random.uniform(100, 500) for symbol in SYMBOLS}
    
    start_time = time.time()
    event_count = 0
    
    try:
        while time.time() - start_time < duration_seconds:
            # Generate trade event
            symbol = random.choice(SYMBOLS)
            trade = generate_trade_event(trade_id, symbol, base_prices[symbol])
            base_prices[symbol] = trade["price"]  # Update base price
            
            producer.send(TOPIC_NAME, key=symbol, value=trade)
            trade_id += 1
            event_count += 1
            
            # Occasionally generate quote event
            if random.random() > 0.7:
                quote = generate_quote_event(quote_id, symbol, base_prices[symbol])
                producer.send(TOPIC_NAME, key=symbol, value=quote)
                quote_id += 1
                event_count += 1
            
            time.sleep(1.0 / events_per_second)
            
            if event_count % 50 == 0:
                print(f"   📊 Sent {event_count} events...")
    
    except KeyboardInterrupt:
        print("\n⏹️  Simulator stopped by user")
    
    producer.flush()
    producer.close()
    
    print(f"\n✅ Simulator finished!")
    print(f"   Total events sent: {event_count}")
    print(f"   Trades: {trade_id}, Quotes: {quote_id}")

if __name__ == "__main__":
    import sys
    
    events_per_sec = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    run_simulator(events_per_second=events_per_sec, duration_seconds=duration)

