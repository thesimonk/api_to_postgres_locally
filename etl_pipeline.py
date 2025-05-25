import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ---------------------------
# Extract data from API
# ---------------------------
def data_extraction(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("Data Extracted Successfully")
        return response.json()
    except requests.RequestException as e:
        print(f'Error during extraction: {e}')
        return None

# ---------------------------
# Transform data
# ---------------------------
def age_group(age):
    if age < 30:
        return 'Youth'
    elif age < 40:
        return 'Adult'
    else:
        return 'Senior'

def transform_data(data):
    if data:
        df = pd.json_normalize(data)
        df['age_group'] = df['age'].apply(age_group)
        print('Data Transformed Successfully')
        print(df.head())
        return df
    else:
        print('No data to transform')
        return None

# ---------------------------
# Load to PostgreSQL with upsert based on `id`
# ---------------------------
def load_to_postgres(df, table_name):
    try:
        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_schema = os.getenv("DB_SCHEMA", "public")

        engine = create_engine(
            f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        )

        with engine.begin() as conn:
            temp_table = f"{table_name}_temp"

            # Step 1: Load data to a temporary table
            df.to_sql(
                name=temp_table,
                con=conn,
                schema=db_schema,
                if_exists="replace",
                index=False
            )

            # Step 2: Create the main table if not exists
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    age INTEGER,
                    age_group TEXT
                );
            """))

            # Step 3: Upsert from temp table using ON CONFLICT on `id`
            conn.execute(text(f"""
                INSERT INTO {db_schema}.{table_name} (id, name, age, age_group)
                SELECT id, name, age, age_group FROM {db_schema}.{temp_table}
                ON CONFLICT (id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    age = EXCLUDED.age,
                    age_group = EXCLUDED.age_group;
            """))

            # Step 4: Drop the temporary table
            conn.execute(text(f"DROP TABLE {db_schema}.{temp_table};"))

        print(f"Data upserted into PostgreSQL table '{db_schema}.{table_name}'")

    except Exception as e:
        print(f"Error loading data: {e}")

# ---------------------------
# Main ETL Function
# ---------------------------
def run_etl(url):
    data = data_extraction(url)
    df_transformed = transform_data(data)
    if df_transformed is not None:
        load_to_postgres(df_transformed, table_name="customers")

# ---------------------------
# Run the ETL Pipeline
# ---------------------------
if __name__ == "__main__":
    url = 'https://raw.githubusercontent.com/thesimonk/Resources/refs/heads/master/etl_files/sample.json'
    run_etl(url)
