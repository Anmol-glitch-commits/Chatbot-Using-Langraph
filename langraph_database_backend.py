from langgraph.graph import StateGraph, START, END
from typing import TypedDict, Annotated
from langchain_core.messages import BaseMessage , HumanMessage
from langchain_huggingface import HuggingFaceEndpoint , ChatHuggingFace
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph.message import add_messages
from dotenv import load_dotenv
import sqlite3

load_dotenv()

llm = HuggingFaceEndpoint(
    repo_id = "Qwen/Qwen2.5-7B-Instruct",
    task = "text-generation"
)

model = ChatHuggingFace(llm = llm)

class ChatState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]

def chat_node(state: ChatState):
    messages = state['messages']
    response = model.invoke(messages)
    return {"messages": [response]}

#sqlite database
conn = sqlite3.connect(database="Chatbot.db" , check_same_thread=False)
#sqlite checkpointer
checkpointer = SqliteSaver(conn = conn)
checkpointer.setup()

graph = StateGraph(ChatState)
graph.add_node("chat_node", chat_node)
graph.add_edge(START, "chat_node")
graph.add_edge("chat_node", END)

chatbot = graph.compile(checkpointer=checkpointer)

#i need all checkpoints
def get_threads():
    all_threads = set()
    for checkpoint in checkpointer.list(None):
        #fetching the thread-id corresponding to each checkpoint
        all_threads.add(checkpoint.config['configurable']['thread_id'])

    #will get unique thread
    return list(all_threads)

