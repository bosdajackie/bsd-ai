"""
name: MS Access Agent (LangChain)
description: LangChain agent to query Microsoft Access .accdb files using natural language, LLMs, and your FastAPI backend.
mode: python
requirements: langchain, langchain-community, langchain-ollama, aiohttp
"""

from typing import List, Union, Generator, Iterator
from langchain_community.chat_models import ChatOllama
from langchain.agents import initialize_agent, Tool
from langchain.agents.agent_types import AgentType
import asyncio
import aiohttp
import logging

# --- CONFIGURATION ---
OLLAMA_BASE_URL = "http://10.10.12.30:11435"
ACCESS_API_URL = "http://host.docker.internal:8001"
LLM_MODEL = "deepseek-r1"
TEMPERATURE = 0.3

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- TOOL DEFINITIONS ---

async def query_access_db(query: str) -> str:
    """Query Microsoft Access DB through REST API with a SQL string."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{ACCESS_API_URL}/query", params={"q": query}) as resp:
                data = await resp.json()
                return str(data.get("result", data.get("error", "Unknown error")))
    except Exception as e:
        return f"❌ API error: {str(e)}"

async def list_tables() -> str:
    """List all table names in the Access database."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{ACCESS_API_URL}/tables") as resp:
                data = await resp.json()
                return ", ".join(data.get("tables", []))
    except Exception as e:
        return f"❌ Error fetching tables: {str(e)}"

async def get_schema(table_name: str) -> str:
    """Get schema of a specific table from Access."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{ACCESS_API_URL}/schema/{table_name}") as resp:
                data = await resp.json()
                columns = data.get("columns", [])
                return "\n".join(f"{col[0]} ({col[1]})" for col in columns)
    except Exception as e:
        return f"❌ Error fetching schema: {str(e)}"

# --- LLM SETUP ---
llm = ChatOllama(
    model=LLM_MODEL,
    base_url=OLLAMA_BASE_URL,
    temperature=TEMPERATURE,
    streaming=False,
)

# --- AGENT SETUP ---
tools = [
    Tool.from_function(query_access_db, name="query_access_db", description="Run SQL query on MS Access database"),
    Tool.from_function(list_tables, name="list_tables", description="List all tables in the database"),
    Tool.from_function(get_schema, name="get_schema", description="Get schema of a given table"),
]

agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True,
)

# --- OPENWEBUI PIPELINE CLASS ---

class Pipeline:
    def __init__(self):
        self.name = "LangChain Agent for MS Access"

    async def on_startup(self):
        logger.info(f"Pipeline started: {__name__}")

    async def on_shutdown(self):
        logger.info(f"Pipeline shutting down: {__name__}")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        async def run_agent():
            try:
                logger.info(f"Agent received question: {user_message}")
                response = await agent.arun(user_message)
                return response
            except Exception as e:
                logger.error(f"Agent error: {e}")
                return f"❌ Error: {str(e)}"
        return asyncio.run(run_agent())
