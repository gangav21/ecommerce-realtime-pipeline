# Real-Time E-commerce Data Pipeline

## Overview
This project implements a scalable data pipeline that ingests real-time clickstream data, processes it using Spark, and stores it in a data lake for analytics.

## Tech Stack
* **Kafka**: Message Broker for high-throughput ingestion.
* **Spark Structured Streaming**: For real-time transformations and watermarking.
* **AWS S3**: Data Lake storage (Parquet format).
* **Docker**: Containerization for easy deployment.

## Key Features
* **Schema Validation**: Ensures incoming JSON matches expected types.
* **Watermarking**: Handles data that arrives late (up to 10 mins).
* **Partitioning**: Data is partitioned by 'event' type for faster querying in BigQuery/Redshift.
