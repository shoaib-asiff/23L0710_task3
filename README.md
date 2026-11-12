# Daily Currency Exchange Rates ETL Pipeline with Apache Airflow & MongoDB

This project implements an end-to-end automated ETL (Extract, Transform, Load) data pipeline using **Apache Airflow (via Astronomer Astro CLI)**, **MongoDB**, and **Mongo Express**. The pipeline integrates daily currency exchange rates from the public **Frankfurter API** into a persistent MongoDB database.

---

## 🏗️ Architecture Overview

```mermaid
flowchart LR
    API["Frankfurter API\n(api.frankfurter.dev)"]
    T1["Task 1: Extract\n(extract_currency_rates)"]
    X1[("Named XCom:\nraw_currency_data")]
    T2["Task 2: Transform\n(transform_exchange_rate_data)"]
    X2[("Named XCom:\ntransformed_currency_data")]
    T3["Task 3: Load\n(load_data_into_mongodb)"]
    MDB[("MongoDB 6.0\n(currency_db.exchange_rates)")]
    MEX["Mongo Express UI\n(http://localhost:8081)"]

    API -->|HTTP GET JSON| T1
    T1 -->|xcom_push| X1
    X1 -->|xcom_pull| T2
    T2 -->|xcom_push| X2
    X2 -->|xcom_pull| T3
    T3 -->|pymongo insert_many| MDB
    MDB <-->|Visual Web Admin| MEX
```

---

## 🚀 Key Features & Requirements

1. **Astro CLI Local Development**: Orchestrated cleanly using Astronomer's runtime.
2. **Frankfurter API Integration**: Fetches real-time exchange rates (`base=EUR`, quotes: `USD, GBP, JPY`).
3. **Three Core Airflow Tasks**:
   - `extract_currency_rates`: Retrieves exchange rate data from the REST API.
   - `transform_exchange_rate_data`: Transforms and validates records into the required schema.
   - `load_data_into_mongodb`: Connects and loads transformed records into MongoDB.
4. **Airflow Named XCom**: Explicitly passes data downstream using designated named keys (`raw_currency_data` and `transformed_currency_data`).
5. **MongoDB & Mongo Express via Docker**:
   - Primary database running MongoDB 6.0 with **persistent volume storage** (`task-3_mongo_data`).
   - Web-based admin GUI powered by **Mongo Express** at `http://localhost:8081`.
   - Connected on a shared Docker bridge network (`airflow`).

---

## 📂 Project Structure

```
├── dags/
│   └── currency_exchange_etl.py       # Airflow DAG definition (3 tasks with Named XCom)
├── docker-compose.override.yml        # MongoDB & Mongo Express services, networking & volumes
├── requirements.txt                   # Python dependencies (requests, pymongo)
├── Dockerfile                         # Astronomer runtime image definition
├── README.md                          # Project documentation
├── REPORT.md                          # Detailed assignment submission report
└── .astro/                            # Astro CLI project configuration
```

---

## 📋 Data Schema

Each document stored in MongoDB (`currency_db.exchange_rates`) conforms to the following standardized structure:

```json
{
  "_id": "6af5e2f91c131d9cb926d0cc",
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1123,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T16:02:00"
}
```

---

## ⚙️ Prerequisites

- **Docker Desktop** (Engine 20.10+ / Compose v2+)
- **Astronomer Astro CLI** (`brew install astronomer/tap/astro`)
- **Python 3.10+**

---

## 🛠️ Getting Started

### 1. Clone the Repository
```bash
git clone <YOUR_REPO_URL>
cd Task3
```

### 2. Start the Environment
Run the following command to spin up Airflow, PostgreSQL, MongoDB, and Mongo Express:
```bash
astro dev start
```

### 3. Access Web Interfaces
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Default login: `admin` / `admin`)
- **Mongo Express Web UI**: [http://localhost:8081](http://localhost:8081) (Login: `admin` / `pass`)
- **MongoDB Connection URI**: `mongodb://root:rootpassword@localhost:27017/?authSource=admin`

### 4. Trigger the DAG
Unpause and trigger the DAG via Airflow CLI or Web UI:
```bash
astro dev run dags unpause currency_exchange_etl_pipeline
astro dev run dags trigger currency_exchange_etl_pipeline
```

### 5. Verify Loaded Data
Open [http://localhost:8081](http://localhost:8081) in your browser, click on database `currency_db`, and select collection `exchange_rates` to view all inserted exchange rate documents.

---

## 🛑 Stopping the Pipeline
To stop all containers without losing data:
```bash
astro dev stop
```
To restart:
```bash
astro dev start
```
