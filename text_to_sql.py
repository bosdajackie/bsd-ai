"""
name: MS Access Query via API with LLM
description: A pipeline to query Microsoft Access .accdb files using natural language, converted to SQL via LLM.
mode: python
requirements: aiohttp, llama-index-llms-openai-like, llama-index-core
"""

from typing import List, Union, Generator, Iterator
import aiohttp
import asyncio
import logging
from llama_index.llms.openai_like import OpenAILike
from llama_index.core.prompts import PromptTemplate

logging.basicConfig(level=logging.DEBUG)

class Pipeline:        
    def __init__(self):
        self.ollama_host = "http://10.10.12.30:11435/api/chat"
        self.access_api_url = "http://host.docker.internal:8001"
        self.model_name = "deepseek-r1"
        self.table_name = "ProductApplication_ACES"

        self.name = "Text to SQL"
        self.llm = OpenAILike(
            model=self.model_name,
            api_base=self.ollama_host,
            max_tokens=100
        )
        self.table_schema = None

    async def on_startup(self):
        print(f"on_startup:{__name__}")
        # Initialize schema on startup
        await self.fetch_schema(self.table_name)

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")

    async def fetch_schema(self, table_name: str):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.access_api_url}/schema/{table_name}") as resp:
                    data = await resp.json()
                    if "columns" in data:
                        self.table_schema = data["columns"]
                        return True
                    else:
                        logging.error(f"Schema error: {data.get('error', 'Unknown error')}")
                        return False
        except Exception as e:
            logging.error(f"Failed to fetch schema: {e}")
            return False

    async def fetch_query_result(self, query: str) -> str:
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

    async def generate_sql_query(self, user_question: str) -> str:
        if not self.table_schema:
            success = await self.fetch_schema(self.table_name)
            if not success:
                return None

        schema_str = "\n".join([f"- {col[0]} ({col[1]})" for col in self.table_schema])
        
        prompt = f"""
        <|begin_of_text|><|start_header_id|>system<|end_header_id|>    
        You are a helpful AI Assistant providing Microsoft Access SQL commands to users.
        Make sure to always use the stop token you were trained on at the end of a response: <|eot_id|>
        
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
        
        Available columns in {self.table_name} table:
        {schema_str}
        
        Question: {user_question}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

        Just return the SQL query without any additional text or formatting.
        """
        
        try:
            response = await self.llm.acomplete(prompt)
            sql_query = response.text.strip()
            return sql_query
        except Exception as e:
            logging.error(f"Error generating SQL: {e}")
            return None

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        async def process():
            try:
                print(f"Processing question: {user_message}")
                sql_query = await self.generate_sql_query(user_message)
                if not sql_query:
                    return "Failed to generate SQL query from your question."
                
                result = await self.fetch_query_result(sql_query)
                return f"Generated SQL Query:\n```sql\n{sql_query}\n```\n\n{result}"
            except Exception as e:
                logging.error(f"Error in pipeline: {str(e)}")
                return f"Error: {str(e)}"

        return asyncio.run(process())