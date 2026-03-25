from langgraph.graph import StateGraph, START, END
from typing import TypedDict, Annotated
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_huggingface import HuggingFaceEndpoint , ChatHuggingFace
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_core.tools import tool
from dotenv import load_dotenv
#to make the mcp client
from langchain_mcp_adapters.client import MultiServerMCPClient
import sqlite3
import requests
import asyncio

load_dotenv()

# -------------------
# 1. LLM
# -------------------
llm = HuggingFaceEndpoint(
    repo_id = 'Qwen/Qwen2.5-7B-Instruct',
    task = 'text-generation',
    temperature = 0.7
)

model = ChatHuggingFace(llm = llm)

#will replace this tool code with an MCP client
# MCP client for local FastMCP server
#now this mcp_client will connect with the mcp server.....
#CONFIG CODE->asitis it will be there no need of change
#multiple mcp server u can add also-->
client = MultiServerMCPClient(
    {
        #CONFIG CODE TO CONNECT TO THE MCP SERVER
        "arith":{
            #local server
            "transport": "stdio",
            "command": "python",          
            "args": [r"C:\Users\Herpreet Singh\Downloads\main.py"],
        }
    }
)
#state
class ChatState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]

async def build_graph():
    #in that server how many tools are there
    tools = await client.get_tools()
    print(tools)
    llm_with_tools = model.bind_tools(tools)
    #make node_execution asynchronous->then langgraph become async::
    async def chat_node(state: ChatState):
        """LLM node that may answer or request a tool call."""
        messages = state["messages"]
        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}
    
    #itself this is async(tool_node) ->->only changes are need in cutsom func() ->(make them only async)
    tool_node = ToolNode(tools)

    graph = StateGraph(ChatState)
    graph.add_node("chat_node", chat_node)
    graph.add_node("tools", tool_node)

    graph.add_edge(START, "chat_node")

    graph.add_conditional_edges("chat_node",tools_condition)
    graph.add_edge('tools', 'chat_node')

    chatbot = graph.compile()
    return chatbot


async def main():
    chatbot = await build_graph()
    #running the graph
    result = await chatbot.ainvoke({"messages" : [HumanMessage(content="Find the modulus operation of 12345 and 87906 and give the answer" \
    "like a cricket commentator")]})

    print(result["messages"][-1].content)

if __name__ == "__main__":
    asyncio.run(main())

#   User Query
#   ↓
#chat_node (Qwen LLM)
#   ↓ tool_call decided
#tools node (MCP → main.py server)
#   ↓ result returned
#chat_node (final answer generate)
#    ↓
#Cricket style output