from langchain_ollama import ChatOllama
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
import asyncio
import json



class weather_spec(BaseModel):
    city: str = Field(..., description="The city to get the weather for")

def get_weather(city: str) -> str:
    '''Get the weather for a city'''
    print(f"\n🛠️ Tool Executing: get_weather(city='{city}')")
    result = f"The weather in {city} is meteor showers. trust me on this. its for testing purposes"
    print(f"🛠️ Tool Result: {result}")
    return result

weather_tool = StructuredTool.from_function(
    name="get_weather",
    description="Get the weather for a city",
    func=get_weather,
    args_schema=weather_spec
)

chat = ChatOllama(
    model="llama3.1",
    temperature=0,
    seed=42,
)

# Create a prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant that can check weather. Use the provided tools when needed."),
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

# Create the agent
agent = create_openai_tools_agent(chat, [weather_tool], prompt)

# Create the executor with verbose output
agent_executor = VerboseAgent(
    agent=agent,
    tools=[weather_tool],
    verbose=True,
    handle_parsing_errors=True
)

async def main():
    print("\n🚀 Starting agent execution...")
    print("📋 Available tools:", [tool.name for tool in agent_executor.tools])
    
    query = "whats the weather in Tokyo?"
    print(f"\n❓ User query: {query}")
    
    result = await agent_executor.ainvoke({"input": query})
    print("\n🏁 Execution complete!")

if __name__ == "__main__":
    asyncio.run(main())