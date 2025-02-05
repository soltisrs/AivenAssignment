import json
from confluent_kafka import Consumer, KafkaError
import psycopg2
from datetime import datetime
from collections import defaultdict
import statistics
import sys
import os
import uuid

SESSION_TIMEOUT = 15  # Define session timeout as 15 seconds

class SensorDataConsumer:
    def __init__(self, kafka_config_path, postgres_uri):
        kafka_conf = {
            'group.id': 'sensor-processor-debug',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000
        }
        
        # Read the kcat config file
        with open(kafka_config_path) as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    if key == 'bootstrap.servers':
                        kafka_conf['bootstrap.servers'] = value
                    elif key == 'security.protocol':
                        kafka_conf['security.protocol'] = value
                    elif key == 'ssl.ca.location':
                        kafka_conf['ssl.ca.location'] = value
                    elif key == 'ssl.certificate.location':
                        kafka_conf['ssl.certificate.location'] = value
                    elif key == 'ssl.key.location':
                        kafka_conf['ssl.key.location'] = value

        self.consumer = Consumer(kafka_conf)
        self.consumer.subscribe(['sensor_readings'])
        
        print(f"Testing PostgreSQL connection to: {postgres_uri}")
        self.postgres_uri = postgres_uri
        # Test database connection
        try:
            with self.get_db_connection() as conn:
                print("Successfully connected to PostgreSQL")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)
            
        self.sensor_sessions = defaultdict(list)
        self.session_last_seen = {}
        self.current_session_id = str(uuid.uuid4())  # Current active session ID
        self.session_start_time = datetime.now()     # Track when the current session started


    def get_db_connection(self):
        return psycopg2.connect(self.postgres_uri)

    def save_readings_to_postgres(self, reading):
        """Save individual reading to PostgreSQL"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    timestamp = datetime.fromtimestamp(int(reading['timestamp']))
                    cur.execute("""
                        INSERT INTO sensor_readings (sensor_id, temperature, humidity, timestamp)
                        VALUES (%s, %s, %s, %s)
                        RETURNING sensor_id;
                    """, (
                        reading['sensor_id'],
                        reading['temperature'],
                        reading['humidity'],
                        timestamp
                    ))
                    row_id = cur.fetchone()[0]
                    conn.commit()
                    print(f"Successfully saved reading to database with id {row_id}")
                    return True
        except Exception as e:
            print(f"Error saving reading to database: {e}")
            return False
    
    def process_stream(self):
        """Main processing loop"""
        try:
            print("Starting to consume sensor readings...")
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    self.check_for_expired_sessions()
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                try:
                    reading = json.loads(msg.value().decode('utf-8'))
                    sensor_id = reading['sensor_id']
                    timestamp = datetime.fromtimestamp(int(reading['timestamp']))

                    # Check if we need to start a new session
                    if self.session_last_seen:
                        latest_seen = max(self.session_last_seen.values())
                        if (timestamp - latest_seen).total_seconds() > SESSION_TIMEOUT:
                            print("Session timeout reached. Starting new session...")
                            self.finalize_all_sessions()
                            self.current_session_id = str(uuid.uuid4())
                            self.session_start_time = timestamp

                    # Add reading to current session
                    self.sensor_sessions[sensor_id].append(reading)
                    self.session_last_seen[sensor_id] = timestamp

                    self.save_readings_to_postgres(reading)
                    print(f"Saved reading from sensor {sensor_id} in session {self.current_session_id}")

                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            print("\nStopping consumer...")
            self.finalize_all_sessions()
        finally:
            self.consumer.close()

    def calculate_metrics(self, readings, session_id):
        """Calculate metrics for a sensor session"""
        temperatures = [r['temperature'] for r in readings]
        humidities = [r['humidity'] for r in readings]

        metrics = {
            'session_id': self.current_session_id,  # Use the shared session ID
            'sensor_id': readings[0]['sensor_id'],
            'avg_temperature': round(statistics.mean(temperatures), 2),
            'max_temperature': max(temperatures),
            'min_temperature': min(temperatures),
            'avg_humidity': round(statistics.mean(humidities), 2),
            'max_humidity': max(humidities),
            'min_humidity': min(humidities),
            'start_time': datetime.fromtimestamp(int(readings[0]['timestamp'])),
            'end_time': datetime.fromtimestamp(int(readings[-1]['timestamp']))
        }
        return metrics

    def finalize_session(self, sensor_id):
        """Compute and save session-level metrics, then clear session data"""
        if sensor_id in self.sensor_sessions and self.sensor_sessions[sensor_id]:
            metrics = self.calculate_metrics(self.sensor_sessions[sensor_id], self.current_session_id)
            self.save_metrics_to_postgres(metrics)
            print(f"Session metrics saved for sensor {sensor_id} in session {self.current_session_id}")

        self.sensor_sessions.pop(sensor_id, None)
        self.session_last_seen.pop(sensor_id, None)

    def check_for_expired_sessions(self):
        """Check if any sessions have expired and finalize them"""
        if not self.session_last_seen:
            return

        current_time = datetime.now()
        latest_seen = max(self.session_last_seen.values())
        
        if (current_time - latest_seen).total_seconds() > SESSION_TIMEOUT:
            print("Session timeout reached. Finalizing all sessions...")
            self.finalize_all_sessions()
            self.current_session_id = str(uuid.uuid4())
            self.session_start_time = current_time

    def save_metrics_to_postgres(self, metrics):
        """Save aggregated session metrics to PostgreSQL"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sensor_metrics (
                        session_id, sensor_id, avg_temperature, max_temperature, min_temperature,
                        avg_humidity, max_humidity, min_humidity,
                        start_time, end_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    metrics['session_id'],
                    metrics['sensor_id'],
                    metrics['avg_temperature'],
                    metrics['max_temperature'],
                    metrics['min_temperature'],
                    metrics['avg_humidity'],
                    metrics['max_humidity'],
                    metrics['min_humidity'],
                    metrics['start_time'],
                    metrics['end_time']
                ))
                conn.commit()

    def finalize_all_sessions(self):
        """Ensure all active sessions are processed before shutting down"""
        for sensor_id in list(self.sensor_sessions.keys()):
            self.finalize_session(sensor_id)


if __name__ == "__main__":
    # Create the table for session metrics if it is not created
    CREATE_METRICS_TABLE = """
    CREATE TABLE IF NOT EXISTS sensor_metrics (
        id SERIAL PRIMARY KEY,
        session_id TEXT,
        sensor_id TEXT,
        avg_temperature FLOAT,
        max_temperature FLOAT,
        min_temperature FLOAT,
        avg_humidity FLOAT,
        max_humidity INT,
        min_humidity INT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
        
    POSTGRES_URI = "postgres://avnadmin:AVNS_XCrhTdkKJ6HRIzgIpGP@postgres-clickstream-clickstream-analytics.d.aivencloud.com:19356/defaultdb?sslmode=require"
    
    print("Initializing consumer...")
    consumer = SensorDataConsumer("kcat.config", POSTGRES_URI)
    
    print("Starting message processing...")

    with consumer.get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_METRICS_TABLE)
            conn.commit()
    consumer.process_stream()