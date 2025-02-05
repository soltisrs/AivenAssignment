# Challenge 1: Real-Time Data Pipeline  
Products: Aiven Kafka, Aiven PostgreSQL, Aiven OpenSearch  
Scenario: You are tasked with building a real-time data pipeline to monitor and analyze website activity.  
● Use Aiven Kafka to ingest Clickstream data from a website in real time.  
● Stream the data from Kafka into Aiven PostgreSQL, where structured information about user sessions is stored.  
Task:  
● Use Terraform to create your resources.  
● Write a script to produce sample Clickstream data into Kafka.  
● Write a script that consumes this data, processes it to calculate session-level metrics or other aggregation metrics, and then writes it to PostgreSQL.  
Bonus: Stream the data to OpenSearch for analytics and visualization.  

--------------------------------------------------------------------------------------------------------------------------
## Resources  
Aiven resources are created using Terraform in the file ```main.tf```. This creates an Aiven PostgreSQL, Aiven Kafka, Kafka Topic for Clickstream Data, and Aiven OpenSearch.  

--------------------------------------------------------------------------------------------------------------------------
## Script to produce Clickstream data into Kafka  
The file ```stream_sensor_data.sh``` generates random sensor readings in batches of 20 seconds. This is used to simulate real clickstream data. The data is being streamed into a Kafka topic named ```sensor_readings```. This file uses ```kcat.config``` (omitted for security). You will need to create this file locally to simualate on your local computer. 

--------------------------------------------------------------------------------------------------------------------------
## Script to consume data, process other metrics, and writes to PostgreSQL  
This python file ```sensor_consumer.py``` captures the stimulated Kafka clickstream data and writes it to a PostgreSQL table named ```sensor_readings```. It computers session level metrics for each sensor within the session. It identifies sessions based on periods of inactivity. It then writes the records to a PostgreSQL table named ```sensor_metrics```. 

--------------------------------------------------------------------------------------------------------------------------
## Opensearch analytics and visualization  
An Opensearch Sink connector is created on the Kafka cluster. This is to stream the data into Opensearch dashboards for visualization. The configuration for this connector can be found in ```opensearch_sink_config.json``` (sensitive information redacted)  
