# -*- coding: utf-8 -*-

import pandas as pd
import psycopg2
import hashlib
import time
from datetime import datetime


# Establish database connection -- setup to be provided

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Function to calculate hash
def calculate_hash(row):
    row_string = '|'.join(map(str, row))
    return hashlib.sha256(row_string.encode()).hexdigest()

# Function to read metadata and determine ingestion type
def fetch_metadata():
    query = "SELECT * FROM ingestion_metadata"
    metadata = pd.read_sql(query, conn)
    return metadata

# Function for date-based ingestion
def date_based_ingestion(file_path, table_name, timestamp_column):
    start_time = time.time()

    # Read new data
    new_data = pd.read_csv(file_path)
    new_data[timestamp_column] = pd.to_datetime(new_data[timestamp_column])

    # Fetch max timestamp from the database
    query = f"SELECT MAX({timestamp_column}) FROM {table_name}"
    cursor.execute(query)
    max_timestamp = cursor.fetchone()[0]
    if max_timestamp is None:
        max_timestamp = datetime(1970, 1, 1)

    # Filter data based on the timestamp
    filtered_data = new_data[new_data[timestamp_column] > max_timestamp]

    # Insert filtered data into the database
    for _, row in filtered_data.iterrows():
        columns = ', '.join(filtered_data.columns)
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    elapsed_time = time.time() - start_time
    print(f"Date-based ingestion for {file_path} completed in {elapsed_time:.2f} seconds.")

# Function for hash-based ingestion
def hash_based_ingestion(file_path, table_name):
    start_time = time.time()

    # Read new data
    new_data = pd.read_csv(file_path)
    new_data['hash'] = new_data.apply(calculate_hash, axis=1)

    # Fetch existing hashes from the database
    query = f"SELECT hash FROM {table_name}"
    existing_hashes = pd.read_sql(query, conn)['hash'].tolist()

    # Filter new data based on hash
    filtered_data = new_data[~new_data['hash'].isin(existing_hashes)]

    # Insert filtered data into the database
    for _, row in filtered_data.iterrows():
        columns = ', '.join(filtered_data.columns)
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    elapsed_time = time.time() - start_time
    print(f"Hash-based ingestion for {file_path} completed in {elapsed_time:.2f} seconds.")

# Function for full ingestion
def full_ingestion(file_path, table_name):
    start_time = time.time()

    # Read new data
    new_data = pd.read_csv(file_path)

    # Truncate the table before full ingestion
    truncate_query = f"TRUNCATE TABLE {table_name}"
    cursor.execute(truncate_query)

    # Insert all data into the table
    for _, row in new_data.iterrows():
        columns = ', '.join(new_data.columns)
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    elapsed_time = time.time() - start_time
    print(f"Full ingestion for {file_path} completed in {elapsed_time:.2f} seconds.")

def hybrid_ingestion(file_path, table_name, timestamp_column):
    start_time = time.time()

    # Read the new dataset
    new_data = pd.read_csv(file_path)
    new_data[timestamp_column] = pd.to_datetime(new_data[timestamp_column], errors="coerce") if timestamp_column else None
    new_data['hash'] = new_data.apply(calculate_hash, axis=1)

    # Split the dataset into two halves
    half_index = len(new_data) // 2
    full_data = new_data.iloc[:half_index]  # First 50% for full ingestion
    incremental_data = new_data.iloc[half_index:]  # Remaining 50% for incremental ingestion

    # Perform full ingestion on the first half
    print(f"Performing full ingestion for the first 50% of the dataset.")
    truncate_query = f"TRUNCATE TABLE {table_name}"
    cursor.execute(truncate_query)

    for _, row in full_data.iterrows():
        columns = ', '.join(full_data.columns)
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    # Perform incremental ingestion on the second half
    if timestamp_column:
        print(f"Performing date-based ingestion for the remaining 50% of the dataset.")
        # Fetch max timestamp from the database
        query_max_timestamp = f"SELECT MAX({timestamp_column}) FROM {table_name}"
        cursor.execute(query_max_timestamp)
        max_timestamp = cursor.fetchone()[0]
        if max_timestamp is None:
            max_timestamp = datetime(1970, 1, 1)

        # Filter incremental data based on timestamp
        filtered_incremental_data = incremental_data[incremental_data[timestamp_column] > max_timestamp]
    else:
        print(f"Performing hash-based ingestion for the remaining 50% of the dataset.")
        # Fetch existing hashes from the database
        query_hashes = f"SELECT hash FROM {table_name}"
        existing_hashes = pd.read_sql(query_hashes, conn)['hash'].tolist()

        # Filter incremental data based on hash
        filtered_incremental_data = incremental_data[~incremental_data['hash'].isin(existing_hashes)]

    # Insert filtered incremental data into the database
    for _, row in filtered_incremental_data.iterrows():
        columns = ', '.join(filtered_incremental_data.columns)
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    elapsed_time = time.time() - start_time
    print(f"Hybrid ingestion for {file_path} completed in {elapsed_time:.2f} seconds.")


# Main ingestion function
def perform_ingestion(file_path):
    metadata = fetch_metadata()

    # Assume metadata contains columns: table_name, ingestion_type, schema, refresh_rate, timestamp_column
    for _, meta in metadata.iterrows():
        table_name = meta['table_name']
        ingestion_type = meta['ingestion_type']
        schema = meta['schema']  # Not used here but can validate dataset columns
        timestamp_column = meta['timestamp_column']

        print(f"Starting {ingestion_type} ingestion for table: {table_name}")

        if ingestion_type == 'full':
            full_ingestion(file_path, table_name)
        elif ingestion_type == 'incremental' and pd.notna(timestamp_column):
            date_based_ingestion(file_path, table_name, timestamp_column)
        elif ingestion_type == 'incremental':
            hash_based_ingestion(file_path, table_name)
        elif ingestion_type == 'hybrid':
            hybrid_ingestion(file_path, table_name, timestamp_column)
        else:
            print(f"Unknown ingestion type: {ingestion_type}")

# Example usage
file_path = "path/to/your/dataset.csv"
perform_ingestion(file_path)

# Close the connection
cursor.close()
conn.close()

