from langchain_ollama import ChatOllama
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
import asyncio
import requests

class db_query_spec(BaseModel):
    query: str = Field(..., description="The syntatically correct Microsoft Access Database query to run on the database")

def db_query(query: str) -> str:
    '''Run a query on the database''' 
    resp = requests.get(f"http://localhost:8001/query", params={"q": query})
    return resp.json()

db_query_tool = StructuredTool.from_function(
    name="db_query",
    description="Run a query on the database. The query must be syntatically correct and return a result.",
    func=db_query,
    args_schema=db_query_spec
)

def db_get_tables() -> str:
    '''Get the tables in the database'''
    resp = requests.get(f"http://localhost:8001/tables")
    return resp.json()

db_get_tables_tool = StructuredTool.from_function(
    name="db_get_tables",
    description="Get the tables in the database",
    func=db_get_tables,
)

chat = ChatOllama(
    model="llama3.1",
    temperature=0,
    seed=42,
)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant that can query a Microsoft Access Database. You will be given a natual language question and you will need to convert it to a syntatically correct Microsoft Access Databse sql query and run it on the database. Use the provided tools when needed."),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

agent = create_openai_tools_agent(chat, [db_query_tool, db_get_tables_tool], prompt)
agent_executor = AgentExecutor(agent=agent, tools=[db_query_tool, db_get_tables_tool])

async def main():
    result = await agent_executor.ainvoke({"input": "what tables are in the database?"})
    print(result["output"])

if __name__ == "__main__":
    asyncio.run(main())