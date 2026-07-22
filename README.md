# Clickstream Analytics Dashboard

A real-time clickstream analytics pipeline that ingests user events, processes them using Apache Spark Structured Streaming, stores curated data in Delta Lake, and visualizes insights through an interactive dashboard.

---

## 🚀 Overview

This project demonstrates an end-to-end streaming data pipeline for processing website click events.

The pipeline performs:

- Streaming event ingestion
- Real-time data cleansing and transformation
- Delta Lake storage
- Interactive dashboard visualization

The project follows a modern data engineering workflow and can be extended to production-scale architectures.

---

## 🏗️ Architecture

```
Clickstream Generator
          │
          ▼
      Apache Kafka
          │
          ▼
Spark Structured Streaming
          │
          ▼
     Delta Lake
          │
          ▼
      Dashboard
```

---

## 📌 Features

- Real-time clickstream processing
- Structured Streaming with Apache Spark
- Data cleansing and validation
- Delta Lake storage
- Interactive dashboard
- Scalable streaming architecture

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| Language | Python |
| Streaming | Apache Kafka |
| Processing | Apache Spark Structured Streaming |
| Storage | Delta Lake |
| Visualization | Streamlit *(or your dashboard framework)* |
| Environment | Docker |

---

## 📂 Project Structure

```
click_dashboard/
│
├── dashboard/          # Dashboard source code
├── spark/              # Spark streaming jobs
├── kafka/              # Kafka producer/configuration
├── data/               # Local data (ignored by Git)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚙️ Getting Started

### Clone repository

```bash
git clone https://github.com/Sater17/click_dashboard.git

cd click_dashboard
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Start services

```bash
docker compose up -d
```

### Run Spark Streaming

```bash
python spark/streaming.py
```

### Launch Dashboard

```bash
streamlit run dashboard/app.py
```

---

## 📊 Pipeline Flow

1. Generate clickstream events
2. Publish events to Kafka
3. Consume events with Spark Structured Streaming
4. Clean and transform data
5. Store results in Delta Lake
6. Display analytics in the dashboard

---

## 📈 Dashboard Metrics

Example metrics include:

- Total clicks
- Active users
- Page views
- Event distribution
- Top visited pages
- Click trends over time

---
## 🔮 Future Improvements

- Airflow orchestration
- dbt transformations
- Data quality checks with Great Expectations
- Cloud deployment (GCP / AWS)
- CI/CD pipeline
- Real-time alerting

---

## 👤 Author

**Minh Tran Cong**

- LinkedIn: www.linkedin.com/in/minh-trần-877a35397
- GitHub: https://github.com/Sater17
