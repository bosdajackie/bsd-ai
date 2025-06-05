import pyodbc
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel
from llama_index.llms.openai_like import OpenAILike
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.core import SQLDatabase
from llama_index.core.prompts import PromptTemplate
from sqlalchemy import create_engine, inspect
import aiohttp
import asyncio
import logging
#from llama_index.llms.ollama import Ollama

# inside the pipelines container, run: pip install llama-index==0.10.1 llama-index-core==0.10.1 llama-index-llms-openai-like==0.1.3
# pip install -U --force-reinstall nltk==3.8.1
# pip install llama-index-llms-openai-like==0.1.3

# good SQL query to list all column names in a table: 
# SELECT json_object_keys(to_json(json_populate_record(NULL::public.movies, '{}'::JSON)))
# response for the movies db: [('Release Year',), ('title',), ('Origin/Ethnicity',), ('director',), ('Cast',), ('genre',), ('Wiki Page',), ('plot',)]

class Pipeline:
    class Valves(BaseModel):
        pass
    
    def __init__(self):
        self.name = "TESTING DB connection"
        access_file_path = r'Z:\Bosda-BizLibrary-20250519.accdb'

        # For .accdb files (Access 2007+)
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={access_file_path};'
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

    def init_db_connection(self):
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={self.valves.ACCESS_FILE_PATH};'
        )

        try:
            self.conn = pyodbc.connect(conn_str)
            print("Connection to Microsoft Access established successfully")
        except Exception as e:
            print(f"Error connecting to Microsoft Access: {e}")

        # Create a cursor object
        self.cur = self.conn.cursor()

        # Query to get the list of tables
        self.cur.tables()
        tables = [table.table_name for table in self.cur.fetchall() if table.table_type == 'TABLE']
        print("Tables in the database:")
        for table in tables:
            print(table)
        
        # Query to get the column names for the first table
        if tables:
            self.cur.columns(tables[0])
            columns = [(column.column_name,) for column in self.cur.fetchall()]
            print(f"Columns in table {tables[0]}:")
            print(f"{columns}")

        self.cur.close()
        self.conn.close()
        

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        self.cur.close()
        self.conn.close()

    async def make_request_with_retry(self, url, params, retries=3, timeout=10):
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientResponseError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt + 1 == retries:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def extract_sql_query(self, response_object):
        for key, value in response_object.items():
            if isinstance(value, dict) and 'sql_query' in value:
                return value['sql_query']
            elif key == 'sql_query':
                return value
        return None

    def handle_streaming_response(self, response_gen):
        final_response = ""
        for chunk in response_gen:
            final_response += chunk
        return final_response

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # Use the established pyodbc connection to create a SQLAlchemy engine
        self.engine = create_engine(f"sqlite+pyodbc://{self.valves.ACCESS_FILE_PATH}")
        sql_database = SQLDatabase(self.engine, include_tables=self.valves.DB_TABLES)

        
        
        llm = OpenAILike(
            model="deepseek-r1",
            api_base=self.valves.OLLAMA_HOST,
            api_key="abc-123",
            max_tokens=100,
            stopping_ids=[128009, 128001, 128000]
        )
        
        

        text_to_sql_prompt = """
        <|begin_of_text|><|start_header_id|>system<|end_header_id|>    
        You are a helpful AI Assistant providing Microsoft Access SQL commands to users.
        Make sure to always use the stop token you were trained on at the end of a response: <|eot_id|>
        
        Given an input question, create a syntactically correct Microsoft Access SQL query to run.
        You can order the results by a relevant column to return the most interesting examples in the database.
        Unless the user specifies in the question a specific number of examples to obtain, query for at most 5 results using TOP 5.
        Never query for all the columns from a specific table, only ask for a few relevant columns given the question.
        You should use DISTINCT statements and avoid returning duplicates wherever possible.
        Pay attention to use only the column names that you can see in the schema description. Be careful to not query for columns that do not exist. Pay attention to which column is in which table.
        
        Note that Microsoft Access SQL has some differences from standard SQL:
        - Use TOP n instead of LIMIT n
        - Use * for wildcards in LIKE patterns
        - Date literals use # instead of quotes, e.g., #2024-05-19#
        - String concatenation uses & instead of ||
        
        You are required to use the following format, each taking one line:
        <|start_header_id|>user<|end_header_id|>
        
        Only use tables listed below.
        ProductApplication_ACES
        
        Question: {query_str}<|eot_id|><|start_header_id|>assistant<|end_header_id|>
        """
        
        
        
        synthesis_prompt = """
        <|begin_of_text|><|start_header_id|>system<|end_header_id|>    
        You are a helpful AI Assistant synthesizing the response from a Microsoft Access SQL query.
        Make sure to always use the stop token you were trained on at the end of a response: <|eot_id|>
        
        You are required to use the following format, each taking one line:
        <|start_header_id|>user<|end_header_id|>
        
        SQLResponse: {response}
        
        Only use tables listed below.
        movies
        
        Only use columns listed below.
        [('Release Year',), ('title',), ('Origin/Ethnicity',), ('director',), ('Cast',), ('genre',), ('Wiki Page',), ('plot',)]
        
        Question: {query_str}<|eot_id|><|start_header_id|>assistant<|end_header_id|>
        """

        text_to_sql_template = PromptTemplate(text_to_sql_prompt)

        query_engine = NLSQLTableQueryEngine(
            sql_database=sql_database,
            tables=self.valves.DB_TABLES,
            llm=llm,
            embed_model="local",
            text_to_sql_prompt=text_to_sql_template,
            synthesize_response=False,
            #response_synthesis_prompt=synthesis_prompt,
            streaming=True
        )

        try:
            response = query_engine.query(user_message)
            sql_query = self.extract_sql_query(response.metadata)
            if hasattr(response, 'response_gen'):
                final_response = self.handle_streaming_response(response.response_gen)
                result = f"Generated SQL Query:\n```sql\n{sql_query}\n```\nResponse:\n{final_response}"
                self.engine.dispose()
                return result
            else:
                final_response = response.response
                result = f"Generated SQL Query:\n```sql\n{sql_query}\n```\nResponse:\n{final_response}"
                self.engine.dispose()
                return result
        except aiohttp.ClientResponseError as e:
            logging.error(f"ClientResponseError: {e}")
            self.engine.dispose()
            return f"ClientResponseError: {e}"
        except aiohttp.ClientPayloadError as e:
            logging.error(f"ClientPayloadError: {e}")
            self.engine.dispose()
            return f"ClientPayloadError: {e}"
        except aiohttp.ClientConnectionError as e:
            logging.error(f"ClientConnectionError: {e}")
            self.engine.dispose()
            return f"ClientConnectionError: {e}"
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            self.engine.dispose()
            return f"Unexpected error: {e}"
