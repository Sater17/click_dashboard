import time
import json
import random
import uuid
import os
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'clickstream-raw'

# Resolve absolute path in workspace
WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_STREAM_DIR = os.path.join(WORKSPACE_DIR, "data", "raw_stream")

# Mock constants
PAGES = [
    'https://shop.example.com/home',
    'https://shop.example.com/products',
    'https://shop.example.com/products/electronics',
    'https://shop.example.com/products/apparel',
    'https://shop.example.com/cart',
    'https://shop.example.com/checkout',
    'https://shop.example.com/checkout/success'
]

REFERRERS = [
    'https://www.google.com',
    'https://www.facebook.com',
    'https://twitter.com',
    'https://yandex.ru',
    'https://shop.example.com/home',
    ''
]

DEVICES = [
    {'type': 'desktop', 'os': 'Windows', 'browsers': ['Chrome', 'Firefox', 'Edge']},
    {'type': 'desktop', 'os': 'macOS', 'browsers': ['Safari', 'Chrome', 'Firefox']},
    {'type': 'desktop', 'os': 'Linux', 'browsers': ['Chrome', 'Firefox']},
    {'type': 'mobile', 'os': 'iOS', 'browsers': ['Safari', 'Chrome']},
    {'type': 'mobile', 'os': 'Android', 'browsers': ['Chrome', 'Firefox']},
    {'type': 'tablet', 'os': 'Android', 'browsers': ['Chrome']},
    {'type': 'tablet', 'os': 'iOS', 'browsers': ['Safari']}
]

active_sessions = []

def generate_session():
    """Create a new user session state."""
    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    device = random.choice(DEVICES)
    browser = random.choice(device['browsers'])
    ip = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
    
    return {
        'user_id': user_id,
        'session_id': session_id,
        'device_type': device['type'],
        'os': device['os'],
        'browser': browser,
        'ip_address': ip,
        'current_page_idx': 0,
        'history': []
    }

def get_next_event(session):
    """Simulate user actions to progress through the e-commerce pages/funnel."""
    idx = session['current_page_idx']
    
    if idx == 0:
        event_type = 'page_view'
        page = PAGES[0]
        session['current_page_idx'] = random.choice([1, 1, 0])
    elif idx == 1:
        event_type = random.choice(['page_view', 'click_ad', 'page_view'])
        page = PAGES[1]
        session['current_page_idx'] = random.choice([2, 2, 0])
    elif idx == 2:
        event_type = random.choice(['page_view', 'add_to_cart', 'page_view'])
        page = random.choice([PAGES[2], PAGES[3]])
        if event_type == 'add_to_cart':
            session['current_page_idx'] = 4
        else:
            session['current_page_idx'] = random.choice([1, 2, 0])
    elif idx == 4:
        event_type = random.choice(['page_view', 'remove_from_cart', 'page_view'])
        page = PAGES[4]
        if event_type == 'remove_from_cart':
            session['current_page_idx'] = 1
        else:
            session['current_page_idx'] = 5
    elif idx == 5:
        event_type = 'page_view'
        page = PAGES[5]
        session['current_page_idx'] = random.choice([6, 1])
    else:
        event_type = 'purchase'
        page = PAGES[6]
        session['current_page_idx'] = 0
        
    referrer = random.choice(REFERRERS) if idx == 0 else PAGES[max(0, idx - 1)]
    
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': session['user_id'],
        'session_id': session['session_id'],
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'event_type': event_type,
        'page_url': page,
        'referrer': referrer,
        'ip_address': session['ip_address'],
        'device_type': session['device_type'],
        'os': session['os'],
        'browser': session['browser']
    }

def initialize_producer():
    """Initialize Kafka Producer with retries. Returns None if connection fails."""
    print("Attempting to connect to Kafka broker...")
    retries = 3
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=3000,
                connection_max_idle_ms=3000
            )
            print("Connected to Kafka successfully!")
            return producer
        except Exception as e:
            retries -= 1
            if retries > 0:
                print(f"Kafka connection failed: {e}. Retrying in 1 second...")
                time.sleep(1)
    print("Could not connect to Kafka broker. Operating in File-based fallback mode.")
    return None

def write_to_file(payload):
    """Write mock data as a single JSON line to data/raw_stream/."""
    os.makedirs(RAW_STREAM_DIR, exist_ok=True)
    filename = f"event_{uuid.uuid4().hex}.json"
    filepath = os.path.join(RAW_STREAM_DIR, filename)
    with open(filepath, 'w') as f:
        if isinstance(payload, bytes):
            # For testing malformed JSON payloads in file mode
            f.write(payload.decode('utf-8'))
        else:
            json.dump(payload, f)

def main():
    parser = argparse.ArgumentParser(description="Mock Clickstream Streaming Generator")
    parser.add_argument("--mode", choices=["kafka", "file", "auto"], default="auto", 
                        help="Streaming mode. 'kafka' requires running broker, 'file' dumps json files locally, 'auto' tries kafka and falls back to file.")
    args = parser.parse_args()

    producer = None
    mode = args.mode

    if mode in ["kafka", "auto"]:
        producer = initialize_producer()
        if producer:
            mode = "kafka"
        else:
            if args.mode == "kafka":
                print("Error: Kafka mode requested but connection failed. Exiting.")
                return
            mode = "file"
            
    print(f"\n--- Generator running in [{mode.upper()}] mode ---")
    if mode == "file":
        print(f"Data will be written to: {RAW_STREAM_DIR}")
    else:
        print(f"Data will be sent to Kafka topic: {KAFKA_TOPIC}")
        
    print("Press Ctrl+C to stop.\n")

    # Initialize starting sessions
    for _ in range(20):
        active_sessions.append(generate_session())
        
    msg_count = 0
    bad_msg_count = 0
    
    try:
        while True:
            # Keep active sessions between 15 and 40
            if len(active_sessions) < 15:
                active_sessions.append(generate_session())
            if len(active_sessions) > 40:
                active_sessions.pop(random.randint(0, len(active_sessions)-1))
                
            if random.random() < 0.1:
                active_sessions[random.randint(0, len(active_sessions)-1)] = generate_session()
                
            session = random.choice(active_sessions)
            event = get_next_event(session)
            
            fault_trigger = random.random()
            payload = None
            is_bad = False
            
            if fault_trigger < 0.05:
                # Fault 1: Malformed JSON
                is_bad = True
                bad_msg_count += 1
                msg_count += 1
                payload = b'{"event_id": "corrupted-json-payload", "user_id": '
                print(f"[FAULT] Generating malformed JSON payload...")
            elif fault_trigger < 0.08:
                # Fault 2: Missing IDs
                is_bad = True
                bad_msg_count += 1
                msg_count += 1
                event_type_fault = random.choice(['missing_user', 'missing_event'])
                if event_type_fault == 'missing_user':
                    event['user_id'] = None
                else:
                    event['event_id'] = None
                payload = event
                print(f"[FAULT] Generating event missing IDs: {event_type_fault}")
            elif fault_trigger < 0.10:
                # Fault 3: Future Timestamp
                is_bad = True
                bad_msg_count += 1
                msg_count += 1
                event['timestamp'] = "2099-12-31T23:59:59.999Z"
                payload = event
                print(f"[FAULT] Generating event with future timestamp...")
            else:
                # Clean Event
                msg_count += 1
                payload = event
                
            # Send payload depending on mode
            if mode == "kafka":
                if is_bad and isinstance(payload, bytes):
                    producer.send(KAFKA_TOPIC, value=payload, value_serializer=lambda x: x)
                else:
                    producer.send(KAFKA_TOPIC, value=payload)
            else:
                write_to_file(payload)
                
            if msg_count % 10 == 0:
                print(f"Total Sent: {msg_count} (Clean: {msg_count - bad_msg_count}, Bad/DLQ: {bad_msg_count})")
                
            # Sleep to match ingestion rate
            time.sleep(random.uniform(0.2, 0.9))
            
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        if producer:
            producer.close()
        print("Generator stopped. Goodbye!")

if __name__ == '__main__':
    main()
