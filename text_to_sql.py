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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self):
        self.ollama_host = "http://10.10.12.30:11435/api/chat"
        self.access_api_url = "http://host.docker.internal:8001"
        self.classifier_model_name = "gemma3:4b"
        self.query_generation_model_name = "qwen2.5-coder:3b"
        self.output_model_name = "deepseek-r1"
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

    async def chat_completion(self, prompt: str, model_usage: str) -> Optional[str]:
        """Make a chat completion request to Ollama"""
        logger.info("Making chat completion request")
        if model_usage == "classifier":
            model_name = self.classifier_model_name
        elif model_usage == "query_generation":
            model_name = self.query_generation_model_name
        elif model_usage == "output":
            model_name = self.output_model_name
        else:
            raise ValueError(f"Invalid model usage: {model_usage}")
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": model_name,
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
            logger.error(f"Failed to fetch schema: {e}")
            return False

    async def select_relevant_tables(self, user_question: str) -> List[str]:
        """Use LLM to select the relevant tables for the query."""
        logger.info(f"Selecting relevant tables for question: {user_question}")

        if not self.available_tables:
            await self.fetch_tables()
            
        tables_str = "\n".join([f"- {table}" for table in self.available_tables])
        
        prompt = f"""You are a helpful AI Assistant that analyzes questions and selects the relevant tables from a database.
Given a question and a list of available tables, select ALL tables that would be needed to answer the question completely.
Consider relationships between tables and whether joins might be needed.

Return ONLY a comma-separated list of the exact table names, no other text. Make sure to match the exact case of the table names from the list.
Example outputs:
TableA, TableB
SingleTable
Table1, Table2, Table3

Available tables:
{tables_str}

Question: {user_question}"""
        
        try:
            selected_tables_str = await self.chat_completion(prompt, "classifier")
            if selected_tables_str:
                # Split and clean the table names
                selected_tables = [t.strip() for t in selected_tables_str.split(',')]
                # Validate that all tables exist
                valid_tables = [t for t in selected_tables if t in self.available_tables]
                if valid_tables:
                    return valid_tables
            return []
        except Exception as e:
            logger.error(f"Error selecting tables: {e}")
            return []

    async def generate_sql_query(self, tables: List[str], user_question: str) -> Optional[str]:
        """Generate SQL query based on schemas and user question."""
        logger.info(f"Generating SQL query for tables: {tables}")
        
        # Fetch schemas for all tables if not already cached
        schemas = {}
        for table in tables:
            if table not in self.table_schemas:
                success = await self.fetch_schema(table)
                if not success:
                    logger.error(f"Failed to fetch schema for table {table}")
                    return None
            schemas[table] = self.table_schemas[table]

        # Build schema string for all tables
        schema_str = ""
        for table in tables:
            schema_str += f"\nTable: {table}\nColumns:\n"
            schema_str += "\n".join([f"- {col[0]} ({col[1]})" for col in schemas[table]])
            schema_str += "\n"
        
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
7. When joining tables, always qualify column names with table names (e.g., [Table1].[Column1])
8. For INNER JOIN, use this syntax: [Table1] INNER JOIN [Table2] ON [Table1].[Column1] = [Table2].[Column2]

Unless the user specifies a number of results, always use TOP 5 in your queries.
Never query for all columns - only select specific columns relevant to the question.
Use DISTINCT when appropriate to avoid duplicates.
If multiple tables are provided, determine if joins are needed and use appropriate join conditions.

Available tables and their schemas:{schema_str}

Question: {user_question}

IMPORTANT:
DO NOT return anything OTHER than the SQL query. Do not include any other text or formatting. Do not explain the query.
"""
        
        try:
            response = await self.chat_completion(prompt, "query_generation")
            if response:
                logger.info(f"Generated SQL response: {response}")
                # Extract code from first markdown code cell
                start = response.find("```sql")
                if start == -1:
                    start = response.find("```")
                if start != -1:
                    end = response.find("```", start + 3)
                    if end != -1:
                        sql_query = response[start:end].split("\n", 1)[1].strip()
                    else:
                        sql_query = response[start:].split("\n", 1)[1].strip()
                else:
                    sql_query = response.strip()
                # Normalize newlines to single spaces
                sql_query = " ".join(line.strip() for line in sql_query.splitlines() if line.strip())
                logger.info(f"Cleaned SQL query: {sql_query}")
            return sql_query
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return None

    async def fetch_query_result(self, query: str) -> str:
        """Execute SQL query and fetch results."""
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

    async def summarize_results(self, question: str, tables: List[str], sql_query: str, query_result: str) -> Optional[str]:
        """Summarize the query results in natural language."""
        logger.info("Generating summary of query results")
        
        tables_str = ", ".join(tables)
        prompt = f"""You are a helpful AI Assistant that explains database query results in natural language.
Given a user's question, the tables queried, the SQL query used, and the query results, provide a clear and concise explanation of the results.

Original Question: {question}

Tables Queried: {tables_str}

SQL Query Used:
{sql_query}

Query Results:
{query_result}

If the query result contains an error, explain the error in natural language and suggest if it might be related to table relationships or join conditions.

Your response should:
1. Answer the original question using the data from the query results
2. If multiple tables were involved, explain how the data was combined
3. Keep the response concise and focused on the data
4. If there was an error, explain what might have gone wrong, especially regarding table relationships
"""

        try:
            summary = await self.chat_completion(prompt, "output")
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
                
                # Select relevant tables
                selected_tables = await self.select_relevant_tables(user_message)
                if not selected_tables:
                    return "Failed to identify relevant tables for your question."
                logger.info(f"Selected tables: {selected_tables}")
                
                # Generate SQL query
                sql_query = await self.generate_sql_query(selected_tables, user_message)
                if not sql_query:
                    return "Failed to generate SQL query from your question."
                
                # Execute query and get results
                result = await self.fetch_query_result(sql_query)
                
                # Generate summary
                summary = await self.summarize_results(user_message, selected_tables, sql_query, result)
                summary_text = f"\nSummary:\n{summary}" if summary else ""
                
                return f"Selected tables: {', '.join(selected_tables)}\n\nGenerated SQL Query:\n```sql\n{sql_query}\n```\n\n{result}{summary_text}"
            
            except Exception as e:
                logger.error(f"Error in pipeline: {e}")
                return f"Error: {str(e)}"

        return asyncio.run(process())