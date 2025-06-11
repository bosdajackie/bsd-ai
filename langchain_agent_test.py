from langchain_ollama import ChatOllama
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
import asyncio

class weather_spec(BaseModel):
    city: str = Field(..., description="The city to get the weather for")

def get_weather(city: str) -> str:
    '''Get the weather for a city'''
    return f"The weather in {city} is 90 degrees fahrenheit with a 10% chance of rain"

def get_joke() -> str:
    '''Get a joke'''
    return "Why did the chicken cross the road? To get to the other side."

weather_tool = StructuredTool.from_function(
    name="get_weather",
    description="Get the weather for a city",
    func=get_weather,
    args_schema=weather_spec
)

joke_tool = StructuredTool.from_function(
    name="get_joke",
    description="Gets the funniest joke you can think of",
    func=get_joke,
)

chat = ChatOllama(
    model="llama3.1",
    temperature=0,
    seed=42,
)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant that can check weather and tell jokes. Use the provided tools when needed."),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

agent = create_openai_tools_agent(chat, [weather_tool, joke_tool], prompt)
agent_executor = AgentExecutor(agent=agent, tools=[weather_tool, joke_tool])

async def main():
    result = await agent_executor.ainvoke({"input": "whats the weather in wakanda's capital?"})
    print(result["output"])

if __name__ == "__main__":
    asyncio.run(main())