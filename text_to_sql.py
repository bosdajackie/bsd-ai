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
        
        prompt = f"""You are an expert in database systems. Your task is to analyze a natural language question and choose the most relevant tables from the following list.

Rules:
- Return ALL tables needed to answer the question completely.
- If the question clearly implies a relationship between entities, include all relevant tables and assume a JOIN may be needed.
- Output should be a comma-separated list of exact table names from the list. No extra words.

Available tables:
{tables_str}

Question:
{user_question}

Respond with only the table names, comma-separated.
"""
        
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
        
        prompt = f"""You are a helpful assistant that generates Microsoft Access SQL queries from user questions.

Given a question and a list of tables with their columns, generate a syntactically correct SQL query for MS Access. Follow these strict rules:

MS Access SQL Rules:
1. Use `SELECT TOP n` instead of `LIMIT n`.
2. Use square brackets around ALL table and column names.
3. Put `TOP` immediately after `SELECT` (e.g. `SELECT TOP 5 [Name]`)
4. Use `*` only in LIKE clauses (e.g., `[Column] LIKE '*value*'`)
5. Use `#` for date literals (e.g., `#2024-05-19#`)
6. Use `&` for string concatenation instead of `||`
7. For JOINs, use:
   `[Table1] INNER JOIN [Table2] ON [Table1].[Key] = [Table2].[Key]`
8. Qualify columns in multi-table queries (e.g., `[Customers].[Name]`)

Other Rules:
- If no number of results is mentioned, return `TOP 5`.
- Do NOT select all columns (`SELECT *`). Select only those relevant to the question.
- Use `DISTINCT` where needed to remove duplicates.

Schemas:
{schema_str}

User Question:
{user_question}

Return only the SQL query. Do NOT include any explanation or markdown formatting.

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
        prompt = f"""You are a database assistant summarizing SQL query results into natural language.

Given a user's question, the SQL used, and the query results, write a short explanation of what the results mean.

Rules:
- Keep your explanation under 4 lines.
- Focus on the key patterns or values returned.
- If multiple tables were joined, explain what was joined and why.
- If there's an error, clearly explain what likely went wrong (e.g., invalid column, join failure).

Question:
{question}

Tables Queried:
{tables}

SQL Used:
{sql_query}

Results:
{query_result}

Write your summary below:

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

    async def _try_execute_query(self, user_question: str, selected_tables: List[str], attempt: int = 1, max_attempts: int = 3, previous_error: str = None) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Try to generate and execute a SQL query with retry logic.
        Returns a tuple of (sql_query, query_result, error_message).
        """
        logger.info(f"Attempting to generate and execute query (attempt {attempt}/{max_attempts})")
        
        # If we have a previous error, modify the query generation prompt
        error_context = f"\nPrevious attempt failed with error: {previous_error}\nPlease fix the query accordingly." if previous_error else ""
        
        # Generate SQL query
        sql_query = await self.generate_sql_query(selected_tables, user_question + error_context)
        if not sql_query:
            return None, None, "Failed to generate SQL query"
        
        # Execute query and get results
        result = await self.fetch_query_result(sql_query)
        
        # Check if there was an error in the result
        if result.startswith("❌"):
            error_message = result.split("\n")[1] if "\n" in result else result[2:].strip()
            
            if attempt < max_attempts:
                logger.info(f"Query failed, attempting retry {attempt + 1}/{max_attempts}")
                return await self._try_execute_query(
                    user_question,
                    selected_tables,
                    attempt + 1,
                    max_attempts,
                    error_message
                )
            else:
                return sql_query, result, f"Failed after {max_attempts} attempts. Last error: {error_message}"
        
        return sql_query, result, None

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        async def process():
            try:
                logger.info(f"Processing question: {user_message}")
                
                # Select relevant tables
                selected_tables = await self.select_relevant_tables(user_message)
                if not selected_tables:
                    return "Failed to identify relevant tables for your question."
                logger.info(f"Selected tables: {selected_tables}")
                
                # Try to execute query with retries
                sql_query, result, error = await self._try_execute_query(user_message, selected_tables)
                
                if error and not result:  # Complete failure
                    return f"Failed to execute query: {error}"
                
                # Generate summary if we have results
                summary = await self.summarize_results(user_message, selected_tables, sql_query, result)
                summary_text = f"\nSummary:\n{summary}" if summary else ""
                
                # If we had errors but eventually succeeded, include retry information
                retry_info = f"\nNote: Query succeeded after retries. Original error: {error}\n" if error else ""
                
                return f"Selected tables: {', '.join(selected_tables)}\n{retry_info}\nGenerated SQL Query:\n```sql\n{sql_query}\n```\n\n{result}{summary_text}"
            
            except Exception as e:
                logger.error(f"Error in pipeline: {e}")
                return f"Error: {str(e)}"

        return asyncio.run(process())