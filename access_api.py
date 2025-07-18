from fastapi import FastAPI, Query, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import pyodbc
from fastapi.responses import StreamingResponse
import logging
import os

app = FastAPI()

# Mount static files for plots
if os.path.exists("plots"):
    app.mount("/plots", StaticFiles(directory="plots"), name="plots")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACCESS_DB_PATH = r"Z:\Bosda-BizLibrary-20250519.accdb"
CONN_STR = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    fr"DBQ={ACCESS_DB_PATH};"
)

stream_chunk_size = 100

def decode_sketchy_utf16(raw_bytes):
    """Handle problematic UTF-16-LE encoded strings from MS Access."""
    s = raw_bytes.decode("utf-16le", "ignore")
    try:
        n = s.index('\u0000')
        s = s[:n]  # respect null terminator
    except ValueError:
        pass
    return s

@app.post("/save_plot")
async def save_plot(file: UploadFile = File(...)):
    plots_dir = "plots"
    if not os.path.exists(plots_dir):
        os.makedirs(plots_dir)
    filename = file.filename
    file_path = os.path.join(plots_dir, filename)
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    return JSONResponse({"filename": filename})

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

@app.get("/columns/{table_name}")
def get_columns(table_name: str):
    logger.info(f"Getting columns for table: {table_name}")
    try:
        conn = pyodbc.connect(CONN_STR)
        
        # Set up the UTF-16 converter for this operation
        prev_converter = conn.get_output_converter(pyodbc.SQL_WVARCHAR)
        conn.add_output_converter(pyodbc.SQL_WVARCHAR, decode_sketchy_utf16)
        
        cursor = conn.cursor()
        cursor.columns(table_name)
        
        # Fetch detailed column information
        columns = []
        for column in cursor.fetchall():
            try:
                column_info = {
                "name": column.column_name,
                "type": column.type_name,
                "size": column.column_size,
                "nullable": bool(column.nullable),
                    "description": column.remarks if column.remarks else None
                }
                columns.append(column_info)
            except Exception as e:
                logger.error(f"Error processing column: {str(e)}")
                continue
        
        # Restore the previous converter
        conn.add_output_converter(pyodbc.SQL_WVARCHAR, prev_converter)
        
        cursor.close()
        conn.close()
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Columns error: {str(e)}")
        return {"error": str(e)}

@app.get("/plots/{filename}")
def get_plot(filename: str):
    """Serve plot images"""
    plot_path = os.path.join("plots", filename)
    if os.path.exists(plot_path):
        logger.info(f"Saving plot to {plot_path}")
        return FileResponse(plot_path, media_type="image/png")
    else:
        return {"error": "Plot not found"}

