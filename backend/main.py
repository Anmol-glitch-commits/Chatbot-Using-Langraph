import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import delete_thread
from database import retrieve_all_threads, thread_document_metadata, save_timestamp, get_timestamps
from graph import get_chat_bot
from pdf_ingestion import ingest_pdf, _THREAD_METADATA

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

chatbot = get_chat_bot()


class ChatMessage(BaseModel):
    message: str
    thread_id: str | None = None


@app.post("/api/chat")
async def chat(message: ChatMessage):
    if not message.thread_id:
        message.thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": message.thread_id}}

    # Build a system message so the LLM knows the thread_id and PDF context
    pdf_meta = _THREAD_METADATA.get(message.thread_id)
    if pdf_meta:
        system_content = (
            f"Current thread_id: {message.thread_id}\n"
            f"A PDF named '{pdf_meta['filename']}' has been uploaded and indexed for this thread. "
            f"When the user asks questions about the PDF or its content, "
            f"call the rag_tool with the user's query and thread_id='{message.thread_id}' "
            f"to retrieve relevant context, then answer based on that context."
        )
    else:
        system_content = (
            f"Current thread_id: {message.thread_id}\n"
            f"No PDF has been uploaded for this thread."
        )

    messages = [
        {"role": "system", "content": system_content},
        {"role": "user", "content": message.message},
    ]
    user_timestamp = datetime.now(timezone.utc).isoformat()
    response = chatbot.invoke({"messages": messages}, config=config)
    ai_timestamp = datetime.now(timezone.utc).isoformat()

    # Persist timestamps to SQLite
    save_timestamp(message.thread_id, "user", user_timestamp)
    save_timestamp(message.thread_id, "assistant", ai_timestamp)

    return {
        "response": response["messages"][-1].content,
        "thread_id": message.thread_id,
        "user_timestamp": user_timestamp,
        "ai_timestamp": ai_timestamp,
    }


@app.post("/api/threads")
async def create_thread():
    thread_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    return {"thread_id": thread_id, "create_at": timestamp}


@app.get("/api/threads")
async def get_all_threads():
    return {"threads": retrieve_all_threads()}


@app.post("/api/upload_pdf")
async def upload_pdf(thread_id: str = Form(...), file: UploadFile = File(...)):
    file_bytes = await file.read()

    result = ingest_pdf(file_bytes, thread_id, file.filename)

    return {
        "thread_id": thread_id,
        "filename": result["filename"],
        "documents": result["documents"],
        "chunks": result["chunks"],
    }


@app.get("/api/threads/{thread_id}/documents")
async def get_document_metadata(thread_id: str):
    return thread_document_metadata(thread_id)


@app.get("/api/threads/{thread_id}/messages")
async def get_thread_messages(thread_id: str):
    config = {"configurable": {"thread_id": thread_id}}
    state = chatbot.get_state(config=config)

    # Thread may have no LangGraph state yet (e.g. created for PDF upload only)
    if not state.values or "messages" not in state.values:
        return {"messages": []}

    all_messages = state.values["messages"]
    stored_timestamps = get_timestamps(thread_id)

    list_of_messages = []
    ts_index = 0  # Track position in stored timestamps

    for message in all_messages:
        if message.type == "tool":
            continue

        # Skip system messages
        if message.type == "system":
            continue

        # Match timestamps by role order
        timestamp = None
        if message.type == "human":
            role = "user"
        elif message.type == "ai":
            # Skip AI messages that are tool calls (no actual text content)
            if message.tool_calls or not message.content or message.content == "":
                continue
            role = "assistant"
        else:
            continue

        # Find the next matching timestamp for this role
        while ts_index < len(stored_timestamps):
            if stored_timestamps[ts_index]["role"] == role:
                timestamp = stored_timestamps[ts_index]["timestamp"]
                ts_index += 1
                break
            ts_index += 1

        list_of_messages.append(
            {"role": role, "content": message.content, "timestamp": timestamp}
        )

    return {"messages": list_of_messages}


@app.delete("/api/threads/{thread_id}")
async def remove_thread(thread_id: str):
    delete_thread(thread_id)

    return {"message": "Thread deleted successfully"}
