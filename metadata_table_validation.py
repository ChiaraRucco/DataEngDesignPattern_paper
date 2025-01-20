

import pandas as pd
import difflib
from datetime import datetime
import pandas as pd

dtype_dict = {
    'table_name': str,
    'data_source_name': str,
    'primary_key': str,
    'refresh_schedule': str,
    'schema': str,
    'ingestion_type': str,
    'error_log': str,
    'incremental_ingestion_type': str,
    'data_quality_rules': str,
    'transformation_rules': str,
    'batch_size': 'Int64',
    'concurrency_level': 'Int64',
    'retry_policy': str,
    'status': str,
    'data_validation_script': str,
    'data_lineage': str,
    'source_file_format': str,
    'compression_type': str,
    'column_level_encryption': str,
    'schema_evolution': str,
    'historical_data_retention_policy': str,
    'data_validation_rules': str,
    'audit_logs': str,
    'notification_rules': str,
    'source_record_count': 'Int64',
    'target_system': str,
    'data_sync_type': str,
    'data_validation_frequency': str,
    'historical_data_handling': str
}

# Specify the columns to parse as dates
date_columns = ['watermark', 'last_ingestion_date', 'last_error_timestamp']

# Read the CSV and apply the dtype dictionary and parse_dates
metadata = pd.read_csv('MetadataTable.csv', dtype=dtype_dict, parse_dates=date_columns)

# Normalize column names (lowercase and replace spaces with underscores)
metadata.columns = metadata.columns.str.lower().str.replace(' ', '_')

# Function to fix typos in field values using fuzzy matching
def fix_typos(field_value, valid_values):
    matches = difflib.get_close_matches(field_value, valid_values, n=1, cutoff=0.8)
    if matches:
        return matches[0]  # Return the closest match
    return field_value  # If no close match, leave the value as is

# Function to auto-correct standard values
def standardize_values(metadata, field_name, valid_values, default_value=None):
    # Normalize field_name by replacing underscores with spaces and lowering the case
    normalized_field_name = field_name.lower().replace(' ', '_')

    for col in metadata.columns:
        if col.lower().replace(' ', '_') == normalized_field_name:
            metadata[col] = metadata[col].apply(lambda x: fix_typos(str(x), valid_values) if pd.notna(x) else default_value)
    return metadata

# Function to convert string to datetime if possible
def convert_to_datetime(value, format='%Y-%m-%d %H:%M:%S'):
    try:
        return datetime.strptime(str(value), format)
    except ValueError:
        return None  # Return None if it cannot be converted

# Function to standardize column names (lowercase and replace spaces with underscores)
def standardize_column_names(metadata):
    metadata.columns = metadata.columns.str.lower().str.replace(' ', '_')
    return metadata

# Function to check mandatory fields
def check_mandatory_fields(metadata):
    mandatory_fields = ['table_name', 'data_source_name', 'primary_key', 'watermark', 'refresh_schedule', 'schema', 'ingestion_type']
    missing_fields = []
    for field in mandatory_fields:
        if field not in metadata.columns:
            missing_fields.append(field)
    return missing_fields

# Function to correct and validate fields
def correct_metadata(metadata):
    # Fix typos and standardize values
    valid_ingestion_types = ['incremental', 'full']
    valid_refresh_schedules = ['daily', 'weekly', 'monthly']
    valid_status_values = ['active', 'paused', 'failed']
    valid_file_formats = ['csv', 'json', 'parquet']
    valid_compression_types = ['gzip', 'snappy']

    # Standardizing and correcting fields if they exist
    metadata = standardize_values(metadata, 'ingestion_type', valid_ingestion_types)
    metadata = standardize_values(metadata, 'refresh_schedule', valid_refresh_schedules)
    metadata = standardize_values(metadata, 'status', valid_status_values)
    metadata = standardize_values(metadata, 'source_file_format', valid_file_formats)
    metadata = standardize_values(metadata, 'compression_type', valid_compression_types)

    # Correct datetime fields (e.g., last_ingestion_date)
    if 'last_ingestion_date' in metadata.columns:
        metadata['last_ingestion_date'] = metadata['last_ingestion_date'].apply(lambda x: convert_to_datetime(x) if pd.notna(x) else None)

    # Additional corrections for known mistakes
    if 'watermark' in metadata.columns:
        metadata['watermark'] = metadata['watermark'].apply(lambda x: convert_to_datetime(x) if pd.notna(x) else None)

    return metadata

# Function to standardize values in a column
def standardize_values(metadata, column_name, valid_values):
    if column_name in metadata.columns:
        metadata[column_name] = metadata[column_name].str.lower().apply(lambda x: x if x in valid_values else None)
    return metadata

# Function to fix missing values with defaults for optional fields
def fill_optional_fields_with_defaults(metadata, optional_fields, default_values):
    for field in optional_fields:
        normalized_field_name = field.lower().replace(' ', '_')
        for col in metadata.columns:
            if col.lower().replace(' ', '_') == normalized_field_name:
                # Instead of inplace=True, directly assign the filled column back to the DataFrame
                metadata[col] = metadata[col].fillna(default_values.get(field))
    return metadata


# Function to check data types for each field
def check_data_types(metadata):
    expected_data_types = {
        'table_name': str,
        'data_source_name': str,
        'primary_key': str,
        'watermark': pd.Timestamp,
        'refresh_schedule': str,
        'schema': str,
        'ingestion_type': str,
        'last_ingestion_date': pd.Timestamp,
        'error_log': str,
        'incremental_ingestion_type': str,
        'data_quality_rules': str,
        'transformation_rules': str,
        'batch_size': int,
        'concurrency_level': int,
        'retry_policy': str,
        'last_error_timestamp': pd.Timestamp,
        'status': str,
        'data_validation_script': str,
        'data_lineage': str,
        'source_file_format': str,
        'compression_type': str,
        'column_level_encryption': str,
        'schema_evolution': str,
        'historical_data_retention_policy': str,
        'data_validation_rules': str,
        'audit_logs': str,
        'notification_rules': str,
        'source_record_count': int,
        'target_system': str,
        'data_sync_type': str,
        'data_validation_frequency': str,
        'historical_data_handling': str
    }

    mismatched_types = {}

    for field, expected_type in expected_data_types.items():
        if field in metadata.columns:
            actual_type = metadata[field].dtype
            if actual_type != expected_type:
                mismatched_types[field] = (actual_type, expected_type)

    return mismatched_types

# List of optional fields and their default values
optional_fields = ['last_ingestion_date', 'incremental_ingestion_type', 'error_log', 'data_quality_rules',
                   'transformation_rules', 'batch_size', 'concurrency_level', 'retry_policy', 'last_error_timestamp',
                   'data_validation_script', 'data_lineage', 'compression_type', 'column_level_encryption', 'schema_evolution',
                   'historical_data_retention_policy', 'data_validation_rules', 'audit_logs', 'notification_rules',
                   'source_record_count', 'data_validation_frequency', 'historical_data_handling']

default_values = {
    'last_ingestion_date': datetime(2025, 1, 20, 12, 0),
    'incremental_ingestion_type': 'date-based',
    'error_log': 'No errors',
    'data_quality_rules': 'None',
    'transformation_rules': 'No transformations',
    'batch_size': 1000,
    'concurrency_level': 1,
    'retry_policy': '3 retries',
    'last_error_timestamp': '2025-01-01 00:00:00',
    'data_validation_script': 'validate_data.py',
    'data_lineage': 'initial_pipeline',
    'compression_type': 'gzip',
    'column_level_encryption': 'AES256',
    'schema_evolution': 'allow',
    'historical_data_retention_policy': '30 days',
    'data_validation_rules': 'None',
    'audit_logs': 'enabled',
    'notification_rules': 'email',
    'source_record_count': 0,
    'data_validation_frequency': 'on ingestion',
    'historical_data_handling': 'merge'
}

# Assuming 'metadata' is the dataframe already loaded
metadata = standardize_column_names(metadata)

# Check for missing mandatory fields and report if any
missing_fields = check_mandatory_fields(metadata)

if missing_fields:
    print(f"Missing mandatory columns: {', '.join(missing_fields)}")
else:
    print("All mandatory columns exist.")

# Check for mismatched data types
mismatched_types = check_data_types(metadata)
if mismatched_types:
    print(f"Data type mismatches found: {mismatched_types}")
else:
    print("No data type mismatches found.")

# Correct the metadata
corrected_metadata = correct_metadata(metadata)

# Fill missing values for optional fields with defaults if they exist
corrected_metadata = fill_optional_fields_with_defaults(corrected_metadata, optional_fields, default_values)

# Check the ingestion type and summarize
incremental_tables = corrected_metadata[corrected_metadata['ingestion_type'] == 'incremental']
full_tables = corrected_metadata[corrected_metadata['ingestion_type'] == 'full']
unique_data_sources = corrected_metadata['data_source_name'].nunique()

# Summarize the metadata status
if missing_fields or mismatched_types:
    print("Please check consistency. There are still adjustments needed.")
else:
    print("Ready for ingestion.")

# Print summary
print("\nSummary:")
print(f"Total tables: {len(corrected_metadata)}")
print(f"Incremental ingestion tables: {len(incremental_tables)}")
print(f"Full ingestion tables: {len(full_tables)}")
print(f"Unique data sources: {unique_data_sources}")

## Consisency Chekcs

def check_ingestion_type_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if row['ingestion_type'] == 'incremental' and pd.isna(row['incremental_ingestion_type']):
            metadata.at[index, 'incremental_ingestion_type'] = 'hash-based'
            alerts.append(f"Row {index}: Incremental ingestion type missing, set to default (hash-based).")
    return alerts


def check_watermark_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if row['ingestion_type'] == 'date-based' and pd.isna(row['watermark']):
            metadata.at[index, 'watermark'] = '1970-01-01'  # Default date
            alerts.append(f"Row {index}: Watermark missing for date-based ingestion, set to default (1970-01-01).")
    return alerts

def check_transformation_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if pd.notna(row['transformation_rules']) and pd.isna(row['data_transformation_script']):
            metadata.at[index, 'data_transformation_script'] = 'no_transformation.py'  # Default script
            alerts.append(f"Row {index}: Data transformation script missing, set to default (no_transformation.py).")
    return alerts

def check_data_validation_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if pd.notna(row['data_validation_rules']) and pd.isna(row['data_validation_script']):
            metadata.at[index, 'data_validation_script'] = 'validate_data.py'  # Default validation script
            alerts.append(f"Row {index}: Data validation script missing, set to default (validate_data.py).")
    return alerts

def check_batch_and_concurrency_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if pd.notna(row['batch_size']) and (pd.isna(row['concurrency_level']) or row['concurrency_level'] <= 0):
            metadata.at[index, 'concurrency_level'] = 1  # Default concurrency level
            alerts.append(f"Row {index}: Invalid concurrency level, set to default (1).")
    return alerts


def check_source_file_format_consistency(metadata):
    valid_compression_types = ['gzip', 'snappy']
    alerts = []
    for index, row in metadata.iterrows():
        if row['source_file_format'] == 'CSV' and row['compression_type'] not in valid_compression_types:
            metadata.at[index, 'compression_type'] = 'gzip'  # Default compression type
            alerts.append(f"Row {index}: Invalid compression type for CSV, set to default (gzip).")
    return alerts


def check_status_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if row['status'] == 'paused' and pd.notna(row['watermark']):
            alerts.append(f"Row {index}: Watermark should not be filled when status is 'paused'.")
    return alerts


def check_encryption_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if pd.notna(row['column_level_encryption']) and row['target_system'] != 'encrypted':
            alerts.append(f"Row {index}: Column-level encryption is set, but target system is not 'encrypted'.")
    return alerts


def check_schema_evolution_consistency(metadata):
    alerts = []
    for index, row in metadata.iterrows():
        if row['schema_evolution'] == 'allow' and pd.isna(row['schema']):
            metadata.at[index, 'schema'] = 'default_schema'  # Default schema value
            alerts.append(f"Row {index}: Schema missing for schema evolution, set to default (default_schema).")
    return alerts



def validate_consistency(metadata):
    all_alerts = []

    alerts = check_ingestion_type_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_watermark_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_transformation_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_data_validation_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_batch_and_concurrency_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_source_file_format_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_status_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_encryption_consistency(metadata)
    all_alerts.extend(alerts)

    alerts = check_schema_evolution_consistency(metadata)
    all_alerts.extend(alerts)

    return all_alerts



validate_consistency(metadata)

