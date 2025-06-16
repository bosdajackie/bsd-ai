import requests
import yaml
from pathlib import Path
from typing import List, Dict
import time
from tqdm import tqdm

def get_tables() -> List[str]:
    """Get all tables from the database"""
    resp = requests.get("http://localhost:8001/tables")
    tables = resp.json().get("tables", [])
    # Filter out system tables
    return [table for table in tables if not table.startswith("MSys")]

def get_columns(table_name: str) -> List[Dict]:
    """Get columns for a specific table"""
    resp = requests.get(f"http://localhost:8001/columns/{table_name}")
    return resp.json().get("columns", [])

def get_row_count(table_name: str) -> int:
    """Get the row count for a specific table"""
    resp = requests.get(f"http://localhost:8001/query/", params={"q": f"SELECT COUNT(*) FROM {table_name}"})
    return resp.json().get("result", [0])[0][0]

def generate_schema_yaml():
    """Generate the complete schema YAML file"""
    print("Fetching tables...")
    tables = get_tables()
    
    schema = {
        "database": {
            "name": "Bosda-BizLibrary",
            "version": "20250519",
            "total_tables": len(tables)
        },
        "tables": {},
        "few_shot_examples": [
            {
                "question": "How many car models fit the part with item_id 513001?",
                "tables_needed": ["ProductApplication_ACES"],
                "query": "SELECT COUNT(DISTINCT [model]) FROM [ProductApplication_ACES] WHERE [item_id] = '513001'",
                "explanation": "Counts distinct car models for the specified part ID"
            },
            {
                "question": "What are the car models that fit part 513001?",
                "tables_needed": ["ProductApplication_ACES"],
                "query": "SELECT DISTINCT [model] FROM [ProductApplication_ACES] WHERE [item_id] = '513001'",
                "explanation": "Lists all car models that can use this part"
            }
        ]
    }
    
    print(f"Processing {len(tables)} tables...")
    for table in tqdm(tables, total=len(tables)):
        try:
            row_count = get_row_count(table)
            if row_count > 1:
                columns = get_columns(table)

                schema["tables"][table] = {
                    "description": "",  # You can add descriptions manually later
                    "columns": columns
                }
        except Exception as e:
            print(f"Error processing table {table}: {str(e)}")
            continue
    
    output_path = Path("db_schema.yaml")
    with open(output_path, "w") as f:
        yaml.dump(schema, f, default_flow_style=False, sort_keys=False)
    
    print(f"\nSchema saved to {output_path}")
    print(f"Total tables processed: {len(schema['tables'])}")

if __name__ == "__main__":
    generate_schema_yaml() 