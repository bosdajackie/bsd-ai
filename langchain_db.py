from langchain_ollama import ChatOllama
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
import asyncio
import requests
import json
import yaml

class db_query_spec(BaseModel):
    query: str = Field(..., description="The syntatically correct Microsoft Access Database query to run on the database")

def db_query(query: str) -> str:
    '''Run a query on the database''' 
    print(f"\n🔍 Executing query: {query}")
    resp = requests.get(f"http://localhost:8001/query", params={"q": query})
    result = resp.json()
    print(f"📊 Query result: {result}")
    return result

db_query_tool = StructuredTool.from_function(
    name="db_query",
    description="Run a query on the database. The query must be syntatically correct and return a result.",
    func=db_query,
    args_schema=db_query_spec
)

chat = ChatOllama(
    model="qwen3",
    temperature=0,
    seed=42,
)



system_prompt = """
You are a helpful assistant that answer the user's inquries. At your disposal is a Microsoft Access Database, which likely contain the answers to many of the user's questions. 

Not all questions need to be answered by querying the database. If this is the case, just return the answer to the user.

To query the database, you can use the following tools:
- db_query: Run a query on the database. You must provide a syntatically correct query for Microsoft Access Database, which is slightly different from standard SQL. The query must be syntatically correct and return a result.

If you decide you need to query the database, here is how you should proceed:

1. Write a query on the columns and tables that will answer the user's question. Make sure to use real column names and table names from the db_query tool.
2. Run the query using the db_query tool.
3. Return the result to the user.

Here is the schema of the database, including all tables and columns in each table:

{schema}

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

agent = create_openai_tools_agent(chat, [db_query_tool], prompt)

# Create the executor with verbose output
agent_executor = VerboseAgent(
    agent=agent,
    tools=[db_query_tool],
    verbose=True,
    handle_parsing_errors=True,
    max_iterations=10,  # Limit the number of iterations
    early_stopping_method="force",  # Force stop after max_iterations
)

async def main():
    print("\n🚀 Starting agent execution...")
    print("📋 Available tools:", [tool.name for tool in agent_executor.tools])

    schema = yaml.safe_load(open("db_schema.yaml")).get("tables", {})
    
    query = "according to the ProductApplication_ACES table, how many car models fit the part with item_id 513001? and what are the car models?"
    response = await agent_executor.ainvoke({"input": query, "schema": schema})
    print(f"\n🔍 Response: {response}")

if __name__ == "__main__":
    asyncio.run(main())