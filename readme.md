# SentinelStream: Real-time Brand Reputation Engine

## 🎯 Goal
To build a production-grade, "coming-of-age" data lakehouse that monitors brand sentiment in real-time. This project demonstrates how to handle high-velocity data using the modern **Flink-Iceberg-LLM** stack.

## 🏗️ Architecture Flow
1. **Ingestion Layer:** Python Mock Producer -> **Apache Kafka** (Buffer/Broker).
2. **Stream Processing:** **Apache Flink** (Weighted Sentiment Aggregation & Watermarking).
3. **Storage Layer:** **Apache Iceberg** + **MinIO** (ACID Table Format on S3).
4. **Catalog:** **Project Nessie/Postgres** (Metadata Management).
5. **Intelligence Layer:** **Apache Spark** (Compaction) + **LLM** (Contextual Summary).

## 🚀 Technical Focus
- **State Management:** Using Flink's RocksDB state backend.
- **Table Evolution:** Leveraging Iceberg for schema and partition evolution.
- **Problem Solving:** Solving the "Small Files Problem" via Spark compaction.