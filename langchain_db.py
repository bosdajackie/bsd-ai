from langchain_ollama import ChatOllama
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
import asyncio
import requests
import json

class db_query_spec(BaseModel):
    query: str = Field(..., description="The syntatically correct Microsoft Access Database query to run on the database")

class db_get_columns_spec(BaseModel):
    table_name: str = Field(..., description="The name of the table to get the columns for")

def db_query(query: str) -> str:
    '''Run a query on the database''' 
    print(f"\n🔍 Executing query: {query}")
    resp = requests.get(f"http://localhost:8001/query", params={"q": query})
    result = resp.json()
    print(f"📊 Query result: {result}")
    return result

def db_get_tables() -> str:
    '''Get the tables in the database'''
    print("\n📋 Fetching tables...")
    resp = requests.get(f"http://localhost:8001/tables")
    result = resp.json()
    print(f"📑 The tables in the database are: {result}")
    return result

def db_get_columns(table_name: str) -> str:
    '''Get the columns in a table'''
    print(f"\n📋 Fetching columns for table: {table_name}")
    resp = requests.get(f"http://localhost:8001/columns/{table_name}")
    result = resp.json()
    print(f"📑 The columns in the table {table_name} are: {result}")
    return result

db_query_tool = StructuredTool.from_function(
    name="db_query",
    description="Run a query on the database. The query must be syntatically correct and return a result.",
    func=db_query,
    args_schema=db_query_spec
)

db_get_tables_tool = StructuredTool.from_function(
    name="db_get_tables",
    description="Get the tables in the database",
    func=db_get_tables,
)

db_get_columns_tool = StructuredTool.from_function(
    name="db_get_columns",
    description="Get the columns in a table. The table name must be provided.",
    func=db_get_columns,
    args_schema=db_get_columns_spec
)


chat = ChatOllama(
    model="llama3.1",
    temperature=0,
    seed=42,
)

system_prompt = """
You are a helpful assistant that answer the user's inquries. At your disposal is a Microsoft Access Database, which likely contain the answers to many of the user's questions. 

Not all questions need to be answered by querying the database. If this is the case, just return the answer to the user.

To query the database, you can use the following tools:
- db_query: Run a query on the database. You must provide a syntatically correct query for Microsoft Access Database, which is slightly different from standard SQL. The query must be syntatically correct and return a result.
- db_get_tables: Get the tables in the database
- db_get_columns: Get the columns in a table. The table name must be provided.

If you decide you need to query the database, here is how you should proceed:

1. Retrieve all the tables in the database using the db_get_tables tool.
2. Determine which tables are relevant to the user's question.
3. For each relevant table, retrieve the columns using the db_get_columns tool. This will also give you the data types of the columns. You will need to know the data types to write a valid query.
4. Determine which columns are relevant to the user's question.
5. Write a query on the columns and tables that will answer the user's question. Make sure to use real column names and table names from the db_get_columns and db_get_tables tools.
6. Run the query using the db_query tool.
7. Return the result to the user.

Steps 1 and 3 are very important to prevent you from writing an invalid query. Please perform these tool calls.

If your query does not return a result, you should edit the query based on the error message and try again. Feel free to make multiple tool calls and attempts.

Do not hallucinate and make up information.
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

# Create the agent with callbacks
class VerboseAgent(AgentExecutor):
    async def _acall(self, inputs: dict, run_manager=None) -> dict:
        print("\n🤖 Agent received input:", json.dumps(inputs, indent=2))
        try:
            result = await super()._acall(inputs, run_manager)
            print("\n✅ Agent execution completed")
            print("📤 Final output:", json.dumps(result, indent=2))
            return result
        except Exception as e:
            print(f"\n❌ Agent execution failed: {str(e)}")
            raise

agent = create_openai_tools_agent(chat, [db_query_tool, db_get_tables_tool, db_get_columns_tool], prompt)

# Create the executor with verbose output
agent_executor = VerboseAgent(
    agent=agent,
    tools=[db_query_tool, db_get_tables_tool, db_get_columns_tool],
    verbose=True,
    handle_parsing_errors=True,
    max_iterations=10,  # Limit the number of iterations
    early_stopping_method="force",  # Force stop after max_iterations
)

async def main():
    print("\n🚀 Starting agent execution...")
    print("📋 Available tools:", [tool.name for tool in agent_executor.tools])
    
    query = "according to the ProductApplication_ACES table, how many car models fit the part with item_id 513001? and what are the car models?"
    response = await agent_executor.ainvoke({"input": query})
    print(f"\n🔍 Response: {response}")

if __name__ == "__main__":
    asyncio.run(main())