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
from urllib.parse import quote
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self):
        self.ollama_host = "http://10.10.12.30:11435/api/chat"
        self.access_api_url = "http://host.docker.internal:8001"
        self.model_name = "gemma3:4b"
        self.name = "Text to SQL"
        self.available_tables: List[str] = []
        self.table_schemas: Dict[str, List[tuple]] = {}
        logger.info("Pipeline initialized")

    async def on_startup(self):
        logger.info(f"Starting up pipeline: {__name__}")
        await self.fetch_tables()
        logger.info(f"Available tables: {self.available_tables}")

    async def on_shutdown(self):
        logger.info(f"Shutting down pipeline: {__name__}")

    async def fetch_tables(self) -> bool:
        """Fetch all available tables from the Access database."""
        logger.info("Fetching tables from database")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/tables") as resp:
                    data = await resp.json()
                    if "tables" in data:
                        self.available_tables = data["tables"]
                        logger.info(f"Successfully fetched {len(self.available_tables)} tables")
                        return True
                    else:
                        logger.error(f"Tables error: {data.get('error', 'Unknown error')}")
                        return False
        except Exception as e:
            logger.error(f"Failed to fetch tables: {e}")
            return False

    def find_matching_table(self, table_name: str) -> Optional[str]:
        """Find exact table name match (case-insensitive)."""
        if not table_name:
            return None
        table_name_lower = table_name.lower().strip()
        for available_table in self.available_tables:
            if available_table.lower() == table_name_lower:
                return available_table
        return None

    async def fetch_schema(self, table_name: str) -> bool:
        """Fetch schema for a specific table."""
        logger.info(f"Fetching schema for table: {table_name}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/schema/{table_name}") as resp:
                    data = await resp.json()
                    if "columns" in data:
                        self.table_schemas[table_name] = data["columns"]
                        logger.info(f"Successfully fetched schema with {len(data['columns'])} columns")
                        return True
                    else:
                        logger.error(f"Schema error: {data.get('error', 'Unknown error')}")
                        return False
        except Exception as e:
            logger.error(f"Failed to fetch schema!!: {e}")
            return False

    async def chat_completion(self, prompt: str) -> Optional[str]:
        """Make a chat completion request to Ollama."""
        logger.info("Making chat completion request")
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": prompt}
                    ],
                    "stream": False
                }
                logger.debug(f"Sending payload to Ollama: {payload}")
                async with session.post(self.ollama_host, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("message", {}).get("content", "").strip()
                        logger.info(f"Received response from Ollama: {response}")
                        return response
                    else:
                        error_text = await resp.text()
                        logger.error(f"Ollama API error: {error_text}")
                        return None
        except Exception as e:
            logger.error(f"Chat completion error: {e}")
            return None

    async def select_relevant_table(self, user_question: str) -> Optional[str]:
        """Use LLM to select the most relevant table for the query."""
        logger.info(f"Selecting relevant table for question: {user_question}")

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
            if selected_table and selected_table.strip() in self.available_tables:
                return selected_table
            return None
        except Exception as e:
            logger.error(f"Error selecting table: {e}")
            return None

    async def generate_sql_query(self, table_name: str, user_question: str) -> Optional[str]:
        """Generate SQL query based on schema and user question."""
        logger.info(f"Generating SQL query for table {table_name}")
        if table_name not in self.table_schemas:
            success = await self.fetch_schema(table_name)
            if not success:
                logger.error(f"Failed to fetch schema for table {table_name}")
                return None

        schema_str = "\n".join([f"- {col[0]} ({col[1]})" for col in self.table_schemas[table_name]])
        
        prompt = f"""You are a helpful AI Assistant providing Microsoft Access SQL commands to users.

Given an input question, create a syntactically correct Microsoft Access SQL query to run.
You can order the results by a relevant column to return the most interesting examples in the database.

CRITICAL MS ACCESS SQL REQUIREMENTS:
1. Use TOP n instead of LIMIT n (e.g., 'SELECT TOP 5' not 'SELECT ... LIMIT 5')
2. Use square brackets [ ] for ALL table and column names
3. Put TOP immediately after SELECT (e.g., 'SELECT TOP 5 [Column]' not 'SELECT [Column] TOP 5')
4. Use * for wildcards in LIKE patterns
5. Date literals use # instead of quotes, e.g., #2024-05-19#
6. String concatenation uses & instead of ||

Unless the user specifies a number of results, always use TOP 5 in your queries.
Never query for all columns - only select specific columns relevant to the question.
Use DISTINCT when appropriate to avoid duplicates.

Table name: {table_name}
Available columns:
{schema_str}

Question: {user_question}
"""
        
        try:
            sql_query = await self.chat_completion(prompt)
            if sql_query:
                logger.info(f"Generated SQL query: {sql_query}")
                # Clean up the query - remove markdown formatting and extra newlines
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                # Normalize newlines to single spaces
                sql_query = " ".join(line.strip() for line in sql_query.splitlines() if line.strip())
                logger.info(f"Cleaned SQL query: {sql_query}")
            return sql_query
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return None

    async def fetch_query_result(self, query: str) -> str:
        """Execute SQL query and fetch results."""
        # clean up the query - remove any markdown and normalize whitespace
        query = query.replace("```sql", "").replace("```", "").strip()
        query = " ".join(line.strip() for line in query.splitlines() if line.strip())
        logger.info(f"Executing query: {query}")
        params = {"q": query}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/query", params=params) as resp:
                    data = await resp.json()
                    if "result" in data:
                        logger.info("Query executed successfully")
                        return f"✅ Results:\n{data['result']}"
                    else:
                        error_msg = data.get('error', 'Unknown error')
                        logger.error(f"Query error: {error_msg}")
                        return f"❌ Error:\n{error_msg}"
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return f"❌ Request failed: {str(e)}"

    async def summarize_results(self, question: str, sql_query: str, query_result: str) -> Optional[str]:
        """Summarize the query results in natural language."""
        logger.info("Generating summary of query results")
        
        # Remove the emoji prefixes from query_result if present
        query_result = query_result.replace("✅ Results:\n", "").replace("❌ Error:\n", "")
        
        prompt = f"""You are a helpful AI Assistant that explains database query results in natural language.
Given a user's question, the SQL query used, and the query results, provide a clear and concise summary.

Original Question: {question}

SQL Query Used:
{sql_query}

Query Results:
{query_result}

If the query result contains an error, explain the error in natural language.

Please provide a natural language summary of the results that directly answers the original question.
Keep your response concise and focused on the data. If there was an error, explain what might have gone wrong.
"""

        try:
            summary = await self.chat_completion(prompt)
            if summary:
                logger.info("Successfully generated summary")
                return summary.strip()
            return None
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return None

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        async def process():
            try:
                logger.info(f"Processing question: {user_message}")
                
                # Select relevant table
                selected_table = await self.select_relevant_table(user_message)
                if not selected_table:
                    return "Failed to identify relevant table for your question."
                logger.info(f"Selected table: {selected_table}")
                
                # Generate SQL query
                sql_query = await self.generate_sql_query(selected_table, user_message)
                if not sql_query:
                    return "Failed to generate SQL query from your question."
                
                # Execute query and get results
                result = await self.fetch_query_result(sql_query)
                
                # Generate summary
                summary = await self.summarize_results(user_message, sql_query, result)
                summary_text = f"\nSummary:\n{summary}" if summary else ""
                
                return f"Selected table: {selected_table}\n\nGenerated SQL Query:\n```sql\n{sql_query}\n```\n\n{result}{summary_text}"
            
            except Exception as e:
                logger.error(f"Error in pipeline: {e}")
                return f"Error: {str(e)}"

        return asyncio.run(process())