# Task 3: Building Airflow ETL Pipeline
**Course**: MLOps (7th Semester)  
**Student Name**: Shoaib Asif  
**Repository**: https://github.com/shoaib-asiff/23L0710_task3  
**Date**: November 12, 2026  

---

## 1. Astro CLI Environment & Container Status
Started the local Airflow environment using Astronomer Astro CLI (`astro dev start`). All 8 required containers are running on the same Docker bridge network (`airflow`), including the Airflow components, PostgreSQL, MongoDB 6.0, and Mongo Express.

```bash
astro dev ps
```
![Astro CLI Services Status](screenshots/report_terminal_astro_ps.png)
*Figure 1: Astro CLI status showing all containers running.*

---

## 2. Airflow DAG Execution & Task States (Run 1)
Triggered the DAG `currency_exchange_etl_pipeline` and checked the task states via Astro CLI. All three sequential tasks (`extract_currency_rates`, `transform_exchange_rate_data`, and `load_data_into_mongodb`) finished with state `success`.

```bash
astro dev run tasks states-for-dag-run currency_exchange_etl_pipeline <RUN_ID>
```
![Airflow Task States Run 1](screenshots/report_terminal_dag_states_1.png)
*Figure 2: Terminal output showing all three tasks succeeded.*

---

## 3. Airflow DAG Repeat Execution (Run 2)
Triggered the pipeline a second time to verify reliability and repeat execution. All tasks completed with status `success`.

![Airflow Task States Run 2](screenshots/report_terminal_dag_states_2.png)
*Figure 3: Second DAG run completing with all tasks successful.*

---

## 4. Airflow Web UI - Grid View
Opened the Airflow web interface at `http://localhost:8080`. The Grid view displays consecutive green boxes confirming successful execution across multiple runs.

![Airflow Grid View](screenshots/report_chrome_airflow_1.png)
*Figure 4: Airflow Grid view showing successful execution history.*

---

## 5. Airflow Web UI - Graph View & Task Dependencies
Graph view in the Airflow UI showing the sequential task dependencies:
`extract_currency_rates` ➔ `transform_exchange_rate_data` ➔ `load_data_into_mongodb`.

![Airflow Graph View](screenshots/report_chrome_airflow_2.png)
*Figure 5: DAG graph view showing the sequential flow of tasks.*

---

## 6. Airflow Web UI - DAG Run Details
Inspected the details and duration of the DAG run in the Airflow UI. The pipeline completed all three tasks in approximately 5.6 seconds.

![Airflow Run Details](screenshots/report_chrome_airflow_3.png)
*Figure 6: Run details and timing in Airflow UI.*

---

## 7. Airflow Named XCom Table
Navigated to **Admin > XComs** in the Airflow UI to verify data transfer between tasks.
- `raw_currency_data`: Contains the raw JSON response fetched by `extract_currency_rates` from the Frankfurter API.
- `transformed_currency_data`: Contains the standardized exchange rate documents created by `transform_exchange_rate_data`.

![Airflow Named XCom Table](screenshots/report_airflow_xcom.png)
*Figure 7: Airflow XCom records showing the custom named keys used.*

---

## 8. Mongo Express Web UI - Database Overview
Opened the Mongo Express administration interface at `http://localhost:8081` (login: `admin` / `pass`). The primary database `currency_db` and the collection `exchange_rates` were automatically initialized and populated by the Airflow load task.

![Mongo Express Database View](screenshots/report_mongo_express_1.png)
*Figure 8: Mongo Express dashboard showing currency_db database.*

---

## 9. Mongo Express - Stored Documents View
Viewing the loaded records in Mongo Express. Each document matches the required schema:
- `base_currency`: "EUR"
- `target_currency`: "USD" / "GBP" / "JPY"
- `exchange_rate`: float value
- `rate_date`: "2026-11-12"
- `source`: "Frankfurter API"
- `retrieved_at`: ISO timestamp

![Mongo Express Document Viewer](screenshots/report_mongo_express_2.png)
*Figure 9: Inserted documents displayed in Mongo Express.*

---

## 10. MongoDB Terminal Verification (`mongosh`)
Connected to the MongoDB container and ran an interactive query using `mongosh` to verify the stored records directly from the database terminal.

```bash
docker exec -it mongodb mongosh "mongodb://root:rootpassword@localhost:27017/currency_db?authSource=admin" --eval "db.exchange_rates.find().sort({_id: -1}).limit(3)"
```
![MongoDB Terminal Query](screenshots/report_terminal_mongodb_query.png)
*Figure 10: Querying MongoDB from the terminal using mongosh.*
