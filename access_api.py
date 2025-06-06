from fastapi import FastAPI, Query
import pyodbc
from fastapi.responses import StreamingResponse
import logging

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACCESS_DB_PATH = r"Z:\Bosda-BizLibrary-20250519.accdb"
CONN_STR = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    fr"DBQ={ACCESS_DB_PATH};"
)

stream_chunk_size = 100

@app.get("/tables")
def get_tables():
    logger.info("Getting tables")
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.tables()
        tables = [table.table_name for table in cursor.fetchall()]
        cursor.close()
        conn.close()
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Tables error: {str(e)}")
        return {"error": str(e)}
    

@app.get("/query")
def run_query(q: str = Query(...)):
    logger.info(f"Received query: {q}")    
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.execute(q)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"result": [tuple(row) for row in rows]}
    except Exception as e:
        logger.error(f"Query error: {str(e)}")
        return {"error": str(e)}

@app.get("/schema/{table_name}")
def get_schema(table_name: str):
    logger.info(f"Getting schema for table: {table_name}")
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.columns(table_name)
        columns = [(column.column_name, column.type_name) for column in cursor.fetchall()]
        cursor.close()
        conn.close()
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Schema error: {str(e)}")
        return {"error": str(e)}
    

def stream_query(query: str):
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Yield column headers first
        column_names = [desc[0] for desc in cursor.description]
        yield ",".join(column_names) + "\n"

        while True:
            rows = cursor.fetchmany(stream_chunk_size)
            if not rows:
                break
            for row in rows:
                # Convert row tuple to comma-separated string
                yield ",".join(map(str, row)) + "\n"

        cursor.close()
        conn.close()
    except Exception as e:
        yield f"ERROR: {str(e)}\n"


@app.get("/query_stream")
def run_streaming_query(q: str = Query(...)):
    return StreamingResponse(stream_query(q), media_type="text/plain")

