import pyodbc
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel
import logging
import aiohttp
import asyncio

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        ACCESS_PATH: str
        DB_TABLES: List[str]

    def __init__(self):
        self.name = "02 Database Query - Access"
        self.conn = None
        self.cur = None

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "ACCESS_PATH": os.getenv("ACCESS_PATH", r'Z:\Bosda-BizLibrary-20250519.accdb'),
                "DB_TABLES": ["ProductApplication_ACES"],
            }
        )

    def init_db_connection(self):
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={self.valves.ACCESS_PATH};'
        )

        try:
            self.conn = pyodbc.connect(conn_str)
            print("Connection to Access DB established successfully")
        except Exception as e:
            print(f"Error connecting to Access DB: {e}")

        # Create a cursor object
        self.cur = self.conn.cursor()

        # Query to get the list of tables
        self.cur.tables()
        tables = [table.table_name for table in self.cur.fetchall() if table.table_type == 'TABLE']
        print("Tables in the database:")
        for table in tables:
            print(f"{table}")

    async def on_startup(self):
        print(f"on_startup:{__name__}")
        self.init_db_connection()

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        print(f"received message from user: {user_message}")
        
        try:
            # Create a new connection for each query to ensure clean state
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                fr'DBQ={self.valves.ACCESS_PATH};'
            )
            conn = pyodbc.connect(conn_str)
            conn.autocommit = True
            cursor = conn.cursor()
            
            sql = user_message
            cursor.execute(sql)
            result = cursor.fetchall()
            
            conn.close()
            return str(result)
            
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return f"Error: {str(e)}"

