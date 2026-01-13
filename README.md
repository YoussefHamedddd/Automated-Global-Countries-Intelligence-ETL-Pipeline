Automated Global Countries Data Intelligence Pipeline

An advanced, high-performance ETL pipeline designed to automate the extraction of global country metrics, featuring a hybrid processing architecture with **DuckDB** and **Pandas**, orchestrated by **Apache Airflow**.

---

##  System Architecture
This project follows a professional data engineering workflow, as shown in the diagram below:

![Project Architecture](https://github.com/YoussefHamedddd/Automated-Global-Countries-Intelligence-ETL-Pipeline/blob/main/docs%20of%20project/Project%20Design.jpeg?raw=true) 


### The Tech Stack
* **Orchestration**: Apache Airflow (Workflow management).
* **Environment**: Docker & Docker Compose (Containerization).
* **Extraction**: Python & **Pandas** (API Ingestion).
* **Transformation**: **DuckDB** (In-process SQL analytical engine).
* **Storage**: **PostgreSQL** (Relational Database).


---

## Pipeline Workflow (DAG Runs)
The pipeline is fully automated and monitored via the Airflow Web UI. Below is the successful execution of the DAG:

![Airflow DAG Runs](https://github.com/YoussefHamedddd/Automated-Global-Countries-Intelligence-ETL-Pipeline/blob/main/docs%20of%20project/Airflow%20DAGS.jpeg?raw=true)
*(Note: Replace 'dag_runs_image.png' with your screenshot showing the green tasks)*

### Task Breakdown:
1. **`cleanup_before_run`**: Removes old staging files to maintain environment health.
2. **`extract_data`**: Fetches JSON data from the API and uses **Pandas** for initial structural mapping.
3. **`transform_data`**: Uses **DuckDB** to perform high-speed SQL transformations on DataFrames.
4. **`load_data`**: Ingests the finalized analytical records into PostgreSQL.

---

## Data Insights (SQL Query)
Once the data is loaded into PostgreSQL, we can run complex analytical queries to derive business value. 

**Query: Analyzing Population Density by Region**
```sql
SELECT 
    region, 
    COUNT(*) AS total_countries,
    SUM(population) AS total_population,
    ROUND(AVG(density::numeric), 2) AS avg_density
FROM 
    public.country_metrics
GROUP BY 
    region
ORDER BY 
    total_population DESC;
