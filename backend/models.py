"""
Shared LLM, embeddings, and chat model instances.
This module has NO project-level imports — it is the root of the dependency tree.
"""

from dotenv import load_dotenv
from langchain_huggingface import (
    HuggingFaceEndpoint,
    HuggingFaceEmbeddings,
    ChatHuggingFace,
)

load_dotenv()


llm = HuggingFaceEndpoint(
    repo_id="Qwen/Qwen2.5-7B-Instruct",
    task="text-generation",
    temperature=0.7,
)


embeddings = HuggingFaceEmbeddings(model="sentence-transformers/all-MiniLM-L6-v2")

model = ChatHuggingFace(llm=llm)
