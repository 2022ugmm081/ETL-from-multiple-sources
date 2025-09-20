import os
import json
import pandas as pd
import xml.etree.ElementTree as ET
import configparser
import logging
import mysql.connector

# ----------------- CONFIG -----------------
config = configparser.ConfigParser()
config.read("config.ini")

DB_CONFIG = {
    "host": config["database"]["host"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "database": config["database"]["database"],
    "port": int(config["database"]["port"])
}

DATA_FOLDER = config["paths"]["data_folder"]
LOG_FILE = config["paths"]["log_file"]
DEST_TABLE = config["settings"]["destination_table"]

# ----------------- LOGGING -----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------- HELPERS -----------------
def read_csv(file_path):
    return pd.read_csv(file_path)

def read_tsv(file_path):
    return pd.read_csv(file_path, sep="\t")

def read_json(file_path):
    return pd.read_json(file_path, orient="records")

def read_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    all_records = []
    for row in root.findall("record"):
        record = {child.tag: child.text for child in row}
        all_records.append(record)
    return pd.DataFrame(all_records)

def extract_data(folder):
    all_data = []
    for file in os.listdir(folder):
        file_path = os.path.join(folder, file)
        if file.endswith(".csv"):
            df = read_csv(file_path)
        elif file.endswith(".tsv"):
            df = read_tsv(file_path)
        elif file.endswith(".json"):
            df = read_json(file_path)
        elif file.endswith(".xml"):
            df = read_xml(file_path)
        else:
            continue
        logging.info(f"Extracted {len(df)} rows from {file}")
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)

def transform_data(df):
    logging.info(f"Initial shape: {df.shape}")

    # Drop duplicates
    df = df.drop_duplicates()

    # Handle missing values
    df = df.fillna({
        "EmployeeID": -1,
        "FirstName": "Unknown",
        "LastName": "Unknown",
        "Department": "Unknown",
        "StartDate": "1970-01-01",
        "Salary": 0
    })

    # Ensure Salary is numeric
    df["Salary"] = pd.to_numeric(df["Salary"], errors="coerce").fillna(0)

    # Ensure EmployeeID is int
    df["EmployeeID"] = df["EmployeeID"].astype(int)

    logging.info("Missing values handled and data cleaned")
    logging.info(f"Final shape: {df.shape}")

    # Basic statistics
    logging.info(f"Salary Statistics:\n{df['Salary'].describe()}")

    return df

def load_data(df):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            EmployeeID INT,
            FirstName VARCHAR(100),
            LastName VARCHAR(100),
            Department VARCHAR(100),
            StartDate DATE,
            Salary FLOAT
        )
        """)

        # Insert data
        for _, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO {DEST_TABLE} (EmployeeID, FirstName, LastName, Department, StartDate, Salary)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

        conn.commit()
        logging.info(f"Inserted {len(df)} rows into {DEST_TABLE}")
    except Exception as e:
        logging.error(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# ----------------- MAIN PIPELINE -----------------
if __name__ == "__main__":
    logging.info("=== Starting ETL Pipeline ===")

    # Extract
    df = extract_data(DATA_FOLDER)

    # Transform
    df = transform_data(df)

    # Load
    load_data(df)

    logging.info("=== ETL Pipeline Finished ===")
