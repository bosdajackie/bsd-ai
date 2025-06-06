"""
name: MS Access Query via API with LLM
description: A pipeline to query Microsoft Access .accdb files using natural language, converted to SQL via LLM.
mode: python
requirements: aiohttp==3.9.3
"""

from typing import List, Union, Generator, Iterator, Optional, Dict
import aiohttp
import asyncio
import logging
import json

class Pipeline:        
    def __init__(self):
        self.ollama_host = "http://10.10.12.30:11435/api/chat"
        self.access_api_url = "http://host.docker.internal:8001"
        self.model_name = "gemma3:4b"
        self.name = "Text to SQL"
        self.available_tables: List[str] = []
        self.table_schemas: Dict[str, List[tuple]] = {}

    async def on_startup(self):
        print(f"on_startup:{__name__}")
        await self.fetch_tables()
        print(f"Available tables: {self.available_tables}")

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")

    async def fetch_tables(self) -> bool:
        """Fetch all available tables from the Access database."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/tables") as resp:
                    data = await resp.json()
                    if "tables" in data:
                        self.available_tables = data["tables"]
                        return True
                    else:
                        logging.error(f"Tables error: {data.get('error', 'Unknown error')}")
                        return False
        except Exception as e:
            logging.error(f"Failed to fetch tables: {e}")
            return False

    async def fetch_schema(self, table_name: str) -> bool:
        """Fetch schema for a specific table."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/schema/{table_name}") as resp:
                    data = await resp.json()
                    if "columns" in data:
                        self.table_schemas[table_name] = data["columns"]
                        return True
                    else:
                        logging.error(f"Schema error: {data.get('error', 'Unknown error')}")
                        return False
        except Exception as e:
            logging.error(f"Failed to fetch schema: {e}")
            return False

    async def chat_completion(self, prompt: str) -> Optional[str]:
        """Make a chat completion request to Ollama."""
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": prompt}
                    ],
                    "stream": False
                }
                async with session.post(self.ollama_host, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("message", {}).get("content", "").strip()
                    else:
                        error_text = await resp.text()
                        logging.error(f"Ollama API error: {error_text}")
                        return None
        except Exception as e:
            logging.error(f"Chat completion error: {e}")
            return None

    async def select_relevant_table(self, user_question: str) -> Optional[str]:
        """Use LLM to select the most relevant table for the query."""
        if not self.available_tables:
            await self.fetch_tables()
            
        tables_str = "\n".join([f"- {table}" for table in self.available_tables])
        
        prompt = f"""You are a helpful AI Assistant that analyzes questions and selects the most relevant table from a database.
Given a question and a list of available tables, select the single most relevant table that would contain the information needed.
Only return the exact table name in plain text, no emojis, nothing else. Make sure to match the exact case of the table name from the list.

Available tables:
{tables_str}

Question: {user_question}"""
        
        try:
            selected_table = await self.chat_completion(prompt)
            print(f"I think the selected table is: {selected_table}")
            print(f"Available tables are here: {self.available_tables}")
            print(f"Evaluating {selected_table} in {self.available_tables}: {selected_table in self.available_tables}")
            if selected_table.strip() in self.available_tables:
                return selected_table
            else:
                # logging.error(f"Selected table {selected_table} not in available tables")
                # logging.error(f"Available tables: {self.available_tables}")
                return None
        except Exception as e:
            logging.error(f"Error selecting table: {e}")
            return None

    async def generate_sql_query(self, table_name: str, user_question: str) -> Optional[str]:
        """Generate SQL query based on schema and user question."""
        if table_name not in self.table_schemas:
            success = await self.fetch_schema(table_name)
            if not success:
                return None

        schema_str = "\n".join([f"- {col[0]} ({col[1]})" for col in self.table_schemas[table_name]])
        
        prompt = f"""You are a helpful AI Assistant providing Microsoft Access SQL commands to users.

Given an input question, create a syntactically correct Microsoft Access SQL query to run.
You can order the results by a relevant column to return the most interesting examples in the database.
Unless the user specifies in the question a specific number of examples to obtain, query for at most 5 results using TOP 5.
Never query for all the columns from a specific table, only ask for a few relevant columns given the question.
You should use DISTINCT statements and avoid returning duplicates wherever possible.

Note that Microsoft Access SQL has some differences from standard SQL:
- Use TOP n instead of LIMIT n
- Use * for wildcards in LIKE patterns
- Date literals use # instead of quotes, e.g., #2024-05-19#
- String concatenation uses & instead of ||
- Column names with spaces need square brackets, e.g., [Product Name]

Table name: {table_name}
Available columns:
{schema_str}

Question: {user_question}

Just return the SQL query without any additional text or formatting."""
        
        try:
            sql_query = await self.chat_completion(prompt)
            return sql_query
        except Exception as e:
            logging.error(f"Error generating SQL: {e}")
            return None

    async def fetch_query_result(self, query: str) -> str:
        """Execute SQL query and fetch results."""
        params = {"q": query}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/query", params=params) as resp:
                    data = await resp.json()
                    if "result" in data:
                        return f"✅ Results:\n{data['result']}"
                    else:
                        return f"❌ Error:\n{data.get('error', 'Unknown error')}"
        except Exception as e:
            return f"❌ Request failed: {str(e)}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        async def process():
            try:
                print(f"Processing question: {user_message}")
                
                # Select relevant table
                selected_table = await self.select_relevant_table(user_message)
                if not selected_table:
                    return "Failed to identify relevant table for your question."
                print(f"Selected table: {selected_table}")
                
                # Generate SQL query
                sql_query = await self.generate_sql_query(selected_table, user_message)
                if not sql_query:
                    return "Failed to generate SQL query from your question."
                
                # Execute query and get results
                result = await self.fetch_query_result(sql_query)
                return f"Selected table: {selected_table}\n\nGenerated SQL Query:\n```sql\n{sql_query}\n```\n\n{result}"
            
            except Exception as e:
                logging.error(f"Error in pipeline: {str(e)}")
                return f"Error: {str(e)}"

        return asyncio.run(process())