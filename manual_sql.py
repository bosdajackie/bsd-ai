"""
name: MS Access Query via API
description: A pipeline to query Microsoft Access .accdb files using a FastAPI server and HTTP requests.
mode: python
requirements: aiohttp
"""

from typing import List, Union, Generator, Iterator
import aiohttp
import asyncio

class Pipeline:
    def __init__(self):
        self.name = "Access API Proxy"

    async def on_startup(self):
        print(f"on_startup:{__name__}")

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")

    async def fetch_query_result(self, query: str) -> str:
        url = "http://host.docker.internal:8001/query"
        params = {"q": query}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=15) as resp:
                    data = await resp.json()
                    if "result" in data:
                        return f"✅ Results:\n{data['result']}"
                    else:
                        return f"❌ Error:\n{data.get('error', 'Unknown error')}"
        except Exception as e:
            return f"❌ Request failed: {str(e)}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        try:
            result = asyncio.run(self.fetch_query_result(user_message))
            return str(result)
        except Exception as e:
            return f"Error: {str(e)}"

