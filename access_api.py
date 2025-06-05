from fastapi import FastAPI, Query
import pyodbc

app = FastAPI()

ACCESS_DB_PATH = r"Z:\Bosda-BizLibrary-20250519.accdb"
CONN_STR = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    fr"DBQ={ACCESS_DB_PATH};"
)

@app.get("/query")
def run_query(q: str = Query(...)):
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.execute(q)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"result": [tuple(row) for row in rows]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/schema/{table_name}")
def get_schema(table_name: str):
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.columns(table_name)
        columns = [(column.column_name, column.type_name) for column in cursor.fetchall()]
        cursor.close()
        conn.close()
        return {"columns": columns}
    except Exception as e:
        return {"error": str(e)}
