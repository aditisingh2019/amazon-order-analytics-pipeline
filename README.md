# Real-Time E-Commerce Order Analytics

A local, end-to-end pipeline that turns simulated order activity into analytics-ready data. It streams events through Kafka, processes them with PySpark, and organizes the workflow with Airflow—supporting faster visibility into sales, products, regions, and customer segments.

## What’s inside

- **Kafka + Faker** generate order, payment, return, and cancellation events.
- **Spark Structured Streaming** reads events and writes Bronze and deduplicated Silver data as Parquet.
- **PySpark batch jobs** use RDD and DataFrame workflows to produce product, sales, regional, and customer metrics.
- **Airflow** coordinates pipeline tasks and retries.
- **dbt + Snowflake** build and test analytics models; an optional **Streamlit** dashboard displays the results.

The local stack runs Kafka, Spark, Airflow, and Kafka UI with Docker Compose. Snowflake-backed orchestration, dbt models, and the dashboard need their own Snowflake configuration.

## Repository layout

```text
src/
  kafka_jobs/       # Topic setup and event producer
  streaming/        # Kafka-to-Parquet Spark consumer
  batch/            # RDD and DataFrame ETL
  airflow/dags/     # Airflow workflows
dbt_amazon/         # dbt staging, marts, and tests
dashboards/         # Streamlit analytics dashboard
data/               # Local input and generated data
```

## Setup and Run Guide

### Prerequisites

- Docker with the Docker Compose plugin
- Ports `2181`, `29092`, `7077`, `8080`, `8081`, `8082`, and `9092` available

### Startup

Create a local environment file and set a valid Airflow Fernet key for `AIRFLOW__CORE__FERNET_KEY`:

```bash
cp .env.example .env
```

Start the services and prepare the local data and Spark package directories:

```bash
docker-compose up -d --build
chmod -R 777 data
docker exec -u 0 -it spark-master bash -c \
  "mkdir -p /home/spark/.ivy2 && chmod -R 777 /home/spark/.ivy2"
```

Local interfaces:

- Airflow: [http://localhost:8082](http://localhost:8082) (local default login: `admin` / `admin`)
- Kafka UI: [http://localhost:8080](http://localhost:8080)
- Spark Master UI: [http://localhost:8081](http://localhost:8081)

Create the Kafka topic using the repository's topic helper:

```bash
docker exec -e KAFKA_SERVERS=kafka:29092 -it spark-master python3 -c \
  "from kafka_jobs.create_topics import create_topic; create_topic()"
```

Start the consumer in one terminal:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/streaming/stream_consumer.py
```

Once the consumer reports that the streaming query has started, generate events in a second terminal:

```bash
docker exec -it spark-master python3 -c \
  "from kafka_jobs.producer import stream_events; stream_events()"
```

The producer emits 2,000 sample events. The consumer runs for 20 seconds, writing raw events to `/opt/data/bronze` and deduplicated events to `/opt/data/silver`, backed by the local `data/` directory.

### Airflow and analytics

The Airflow UI includes `pipeline_dag` and `df_batch_etl_pipeline`. The pipeline DAG also uploads data to Snowflake, so configure its `my_snowflake_conn` connection and required Snowflake database objects and stages before running it. The DataFrame batch DAG expects Silver data.

The `dbt_amazon/` models and `dashboards/streamlit_app.py` are optional Snowflake-backed analytics components; configure the dbt profile and Streamlit Snowflake secrets before using them.

### Stop the stack

Stop the containers and remove their persisted volumes:

```bash
docker compose down -v
```