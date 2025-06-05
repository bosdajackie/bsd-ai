import pyodbc

# Replace with your actual path
access_file_path = r'Z:\Bosda-BizLibrary-20250519.accdb'

# For .accdb files (Access 2007+)
conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    fr'DBQ={access_file_path};'
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()