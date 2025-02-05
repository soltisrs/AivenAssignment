# Challenge 2: Change Data Capture and Secure Data Sharing

Products: Aiven PostgreSQL, Aiven Kafka, Aiven OpenSearch  
Scenario: Different teams in your organization need to share data securely.  
● Use Aiven PostgreSQL as a central database for storing sensitive records.  
● Stream updates in real time to different teams using Aiven Kafka.  
● Allow teams to perform advanced searches on the shared data using Aiven OpenSearch.  
Task:  
● Use Terraform to create your resources.  
● Write a script to insert records into PostgreSQL and stream changes (CDC) to Kafka.  
● Build a Kafka consumer or use the OpenSearch connector to move the data into OpenSearch.  
● Use OpenSearch dashboards or Grafana to visualize pertinent information.  
● Query OpenSearch to show the updating information.  
Bonus: Expand the solution to consider two classifications of data, public and private. Implement a secure query mechanism for teams to access relevant OpenSearch data based on predefined roles.

--------------------------------------------------------------------------------------------------------------------------


