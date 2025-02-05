#!/bin/bash

# Kafka topic name
TOPIC="sensor_readings"

# Number of messages per batch
NUM_MESSAGES=10

# Function to generate random sensor readings
generate_sensor_data() {
    local SENSOR_ID="s$((RANDOM % 5 + 1))" # Generate sensor IDs like s1, s2, ..., s5
    local TEMPERATURE=$(awk -v min=15 -v max=30 'BEGIN{srand(); printf "%.1f", min+rand()*(max-min)}') # Random temperature 15-30
    local HUMIDITY=$((RANDOM % 41 + 60)) # Random humidity 60-100
    local TIMESTAMP=$(date +%s) # Current Unix timestamp
    echo "{\"sensor_id\": \"$SENSOR_ID\", \"temperature\": $TEMPERATURE, \"humidity\": $HUMIDITY, \"timestamp\": \"$TIMESTAMP\"}"
}

# Function to stream sensor data in batches
stream_data() {
    for ((i = 1; i <= NUM_MESSAGES; i++)); do
        DATA=$(generate_sensor_data)
        echo "Sending: $DATA"
        echo "$DATA" | kcat -F kcat.config -t $TOPIC -P
        sleep 1 
    done
}

# Send first batch
stream_data

# Wait 20 seconds before sending the next batch
echo "Waiting 20 seconds before sending the next batch..."
sleep 20

# Send second batch
stream_data

echo "Finished streaming messages to topic '$TOPIC'."
