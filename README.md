<div align="center">

# 🏗️ End-to-End Data Engineering Pipeline

A production-inspired data pipeline built to learn the modern data engineering stack

![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat-square&logo=python)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-231F20?style=flat-square&logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5.0-E25A1C?style=flat-square&logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose_V2-2496ED?style=flat-square&logo=docker)
</div>

📖 Overview

This project is a fully containerized data pipeline that ingests data from 4 public APIs, streams it through Apache Kafka, cleans and transforms it with Apache Spark, stores it in PostgreSQL— all orchestrated with Apache Airflow.

Built as a learning project to understand how real-world data engineering pipelines work end to end.

🏛️ Architecture

<img width="644" height="715" alt="Screenshot from 2026-05-15 10-19-30" src="https://github.com/user-attachments/assets/89420eac-3962-43b5-aa4a-fd3cb800a07d" />

🛠️ Tech Stack

-Apache Kafka7.5.0 (Confluent)Message broker — buffers data between producers and Spark
-Apache Spark3.5.0Batch processing — reads Kafka, cleans and transforms data
-Apache Airflow2.xOrchestration — schedules API ingestion, handles retries
-PostgreSQL15Analytical storage — stores clean data for querying
-DockerCompose V2Containerization — runs all services in one network
-Python3.12Producer scripts and Spark jobs

🗂️ Project Structure

<img width="612" height="569" alt="Screenshot from 2026-05-15 10-29-08" src="https://github.com/user-attachments/assets/b9d964d6-3700-42cc-a55f-a1d4c8bf7d2f" />


🚀 Quick Start
Prerequisites

Docker + Docker Compose V2
Git

1. Clone the repo
cd end_to_end_data_engineering_project

3. Start all containers
bashdocker compose up -d --

4. Verify everything is running
   
  bashdocker compose ps
  NAME         STATUS
  zookeeper    Up
  kafka        Up
  producer     Up
  spark        Up
  postgres     Up

5. Start Airflow
  bashcd airflow && docker compose up -d
  Open: http://localhost:8080

6. Create Kafka topics (first time only)
  bashdocker exec -it kafka bash

  kafka-topics --create --topic github_repos \
    --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1


8. Run a Spark job manually
  bashdocker exec -it spark bash

  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /app/spark/jobs/github_spark.py


9. Check data in PostgreSQL
  bashdocker exec -it postgres psql -U admin -d economy_db

  \dt                              -- list tables

📋 Development Phases

   Phase 1 — Kafka producers (API → Kafka topics)
   
   Phase 2 — Airflow DAGs (scheduled ingestion)
   
   Phase 3 — Spark batch jobs (cleaning & transformation)
   
   Phase 4 — PostgreSQL storage (write clean data)
   
   Phase 5 — Visualization (Superset dashboards)

 
