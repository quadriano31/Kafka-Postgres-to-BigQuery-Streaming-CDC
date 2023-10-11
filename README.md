# Kafka-Postgres-to-BigQuery-Streaming-CDC

Using Kafka to stream real time data from Postgres to BigQuery




## Folder structure
```bash
root/
├── Connector
│   └── postgres_connector
├── data/
│   └── spotify.csv/
├── kafka-consumer/
│   └── kafka-consumer.py
├── target/
│   └── plugin/
├── load_data_to_db.py
└── docker-compose.yaml
```
## PREREQUISITE 

Docker & Docker compose 

Clone the repo and run docker compose up, Ensure that all containers are healthy


Open PG Admin on localhos:8080, enter the credentials and add server to connect postgres

```bash
pgAdmin username: admin@admin.com
        password: root 
Connection
    Host name/address: postgres
    port 5432
    username: postgres
    password: postgres
```
## Create Replication user that will allow debezium read data from postgres

Open Query tool on pgAdmin and run the following 

```bash
CREATE USER debezium WITH PASSWORD 'debeziumpw';

-- Grant Privileges on the Database
GRANT ALL PRIVILEGES ON DATABASE spotify_db TO debezium;

-- Grant Specific Privileges to the User
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

```


## Setup Kafka

Navigate to the Kafka UI on http://localhost:3030/ and create a connector 

Select postgres connector and paste the connector details below and click create


```bash
name=PostgresConnector
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.user=postgres
database.dbname=spotify_db
tasks.max=1
database.server.name=postgres
database.port=5432
plugin.name=pgoutput
key.converter.schemas.enable=false
database.hostname=postgres
database.password=postgres
value.converter.schemas.enable=false
table.include.list=public.streaming_db
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter=org.apache.kafka.connect.json.JsonConverter
```


## Load data to postgres

Run the load_data_to_db.py file to load data randomly load data at intervals to postgres

```bash
python load_data_to_db.py
```
Kafka will automatically create the topic

### CREATE A GCP PROJECT 

Create a GCP project, BigQuery Dataset and Table 
Create BigQuery Dataset [click here](https://cloud.google.com/bigquery/docs/datasets)

### Permisions to give service account 

```bash
Bigquery Admin

```

### Consume data from kafka and stream to the BigQuery Table

```bash
python kafka-consumer.py
```
make sure to add the correct dataset name and table name 

## Improvement 
```bash
Add CI/CD 
Use Apache Beam to perform stream on BIgQuery
Integrate with GCP Secrets to ensure service accounts are kept safe

```

### Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, please create a GitHub issue or submit a pull request.

### License
This project is licensed under the MIT License.
