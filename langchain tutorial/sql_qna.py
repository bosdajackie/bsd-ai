import os
from langchain_community.utilities import SQLDatabase
from typing_extensions import TypedDict, Annotated
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import START, StateGraph

db = SQLDatabase.from_uri("sqlite:///C:/Users/yiyi/Documents/Chinook.db")

class State(TypedDict):
    question: str
    query: str
    result: str
    answer: str

llm = ChatOllama(model = "llama3.1", temperature = 0)

system_message = """
Given an input question, create a syntactically correct {dialect} query to
run to help find the answer. Unless the user specifies in his question a
specific number of examples they wish to obtain. You can order the results by a relevant column to
return the most interesting examples in the database.

Never query for all the columns from a specific table, only ask for a the
few relevant columns given the question.

Pay attention to use only the column names that you can see in the schema
description. Be careful to not query for columns that do not exist. Also,
pay attention to which column is in which table.

Only use the following tables:
{table_info}
"""

user_prompt = "Question: {input}"

query_prompt_template = ChatPromptTemplate(
    [("system", system_message), ("user", user_prompt)]
)

class QueryOutput(TypedDict):
    """Generated SQL query."""

    query: Annotated[str, ..., "Syntactically valid SQL query."]


def write_query(state: State):
    """Generate SQL query to fetch information."""
    prompt = query_prompt_template.invoke(
        {
            "dialect": db.dialect,
            "table_info": db.get_table_info(),
            "input": state["question"],
        }
    )
    structured_llm = llm.with_structured_output(QueryOutput)
    result = structured_llm.invoke(prompt)
    return {"query": result["query"]}

def execute_query(state: State):
    """Execute SQL query."""
    execute_query_tool = QuerySQLDatabaseTool(db=db)
    return {"result": execute_query_tool.invoke(state["query"])}

def generate_answer(state: State):
    """Answer question using retrieved information as context."""
    prompt = (
        "Given the following user question, corresponding SQL query, "
        "and SQL result, answer the user question.\n\n"
        f'Question: {state["question"]}\n'
        f'SQL Query: {state["query"]}\n'
        f'SQL Result: {state["result"]}'
    )
    response = llm.invoke(prompt)
    return {"answer": response.content}

graph_builder = StateGraph(State).add_sequence(
    [write_query, execute_query, generate_answer]
)
graph_builder.add_edge(START, "write_query")


memory = MemorySaver()
graph = graph_builder.compile(checkpointer=memory, interrupt_before=["execute_query"])

# Now that we're using persistence, we need to specify a thread ID
# so that we can continue the run after review.
config = {"configurable": {"thread_id": "1"}}

print("=" * 60)
print("🤖 SQL Q&A System - Chinook Database")
print("=" * 60)

for step in graph.stream(
    {"question": input("\n❓ Enter your question: ")},
    config,
    stream_mode="updates",
):
    if "write_query" in step:
        sql_query = step["write_query"]["query"]
        print(f"\n🔍 Generated SQL Query:")
        print(f"   {sql_query}")
        print("\n" + "-" * 40)
    elif "__interrupt__" in step:
        print("⏸️  Ready to execute query...")

try:
    user_approval = input("✅ Execute this query? (yes/no): ")
except Exception:
    user_approval = "no"

if user_approval.lower() == "yes":
    print("\n🔄 Executing query...")
    # If approved, continue the graph execution
    for step in graph.stream(None, config, stream_mode="updates"):
        if "execute_query" in step:
            print("✅ Query executed successfully!")
            print(step["execute_query"]["result"])
        elif "__interrupt__" in step:
            print("⏸️  Ready to execute query...")
        elif "write_query" in step:
            sql_query = step["write_query"]["query"]
            print(f"\n🔍 Generated SQL Query:")
            print(f"   {sql_query}")
            print("\n" + "-" * 40)
        elif "generate_answer" in step:
            print("\n" + "=" * 60)
            print("📝 ANSWER:")
            print("=" * 60)
            print(step["generate_answer"]["answer"])
            print("=" * 60)
else:
    print("\n❌ Operation cancelled by user.")