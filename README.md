# ETL Pipeline: API to PostgreSQL

This project implements a simple but production-minded **ETL (Extract, Transform, Load)** pipeline using Python. It extracts data from a public API, transforms it using business logic, and loads it into a PostgreSQL database with **idempotent upserts** to prevent duplicate records.

---

## Project Structure

```
etl_project/
├── etl_pipeline.py        # Main ETL script
├── .env                   # Environment variables for PostgreSQL
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

---

## Requirements

- Python 3.7+
- PostgreSQL (running locally or remotely)

---

## Setup

### 1. Clone the Project

```bash
git clone https://github.com/your-username/etl-project.git
cd etl-project
```

### 2. Create and Activate Virtual Environment

```bash
python -m venv venv
source venv/bin/activate       # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Configure Database Connection

Create a `.env` file in the root directory:

```ini
DB_NAME=<>
DB_USER=<>
DB_PASSWORD=<>
DB_HOST=localhost
DB_PORT=5432
DB_SCHEMA=<>
```

Ensure PostgreSQL is running, and that the schema (`etl_lab`) exists:

```sql
CREATE SCHEMA etl_lab;
```

Also create the database if it doesn’t exist:

```sql
CREATE DATABASE ETL_TRIAL;
```

---

## Running the Pipeline

```bash
python etl_pipeline.py
```

This will:

1. Fetch data from the following API:  
   `https://raw.githubusercontent.com/thesimonk/Resources/refs/heads/master/etl_files/sample.json`

2. Transform the data:
   - Add an `age_group` field based on `age`

3. Load it into the `etl_lab.customers` table using idempotent `UPSERT` logic based on the `id` column.

---

## Idempotency

This pipeline uses a **staging table + upsert** approach with PostgreSQL:

- Inserts new records
- Updates existing records (same `id`)
- Avoids duplicates when re-running the pipeline

---

## Sample Output

```text
Data Extracted Successfully
Data Transformed Successfully
   id     name  age           city age_group
0   1    Alice   25       New York     Youth
1   2      Bob   30    Los Angeles     Adult
...
Data upserted into PostgreSQL table 'etl_lab.customers'
```

---

## Tech Stack

- Python
- pandas
- SQLAlchemy
- psycopg2
- dotenv
- PostgreSQL

---

## Notes

- Make sure PostgreSQL is running and accessible
- The `id` field must be unique per source record (required for upsert)
- You can adapt this pipeline to other sources like CSV, S3, or web scraping

---

## Future Enhancements

- Logging (replace `print()` with Python's `logging`)
- Automated scheduling (e.g., cron, Apache Airflow)
- Unit tests for transformation logic
- Dockerize the whole pipeline

---

## Author

Simon K  
[GitHub Profile](https://github.com/thesimonk)
