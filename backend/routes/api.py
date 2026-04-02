import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Form, UploadFile, File, Depends, HTTPException
from pydantic import BaseModel
from typing import Annotated
from database import delete_thread
from database import (
    get_all_threads_for_user,
    thread_document_metadata,
    save_timestamp,
    get_timestamps,
    create_thread_for_user,
    get_thread_owner,
)
from auth import get_current_user
from graph import get_chat_bot
from pdf_ingestion import ingest_pdf, _THREAD_METADATA

router = APIRouter(prefix="/api", tags=["Main API"])

chatbot = get_chat_bot()


class ChatMessage(BaseModel):
    message: str
    thread_id: str | None = None


@router.post("/chat")
async def chat(
    message: ChatMessage, current_user: Annotated[dict, Depends(get_current_user)]
):
    user_id = current_user["id"]
    if not message.thread_id:
        message.thread_id = str(uuid.uuid4())
        create_thread_for_user(message.thread_id, user_id)
    else:
        owner = get_thread_owner(message.thread_id)

        if owner is None:
            raise HTTPException(status_code=404, detail="thread not found")

        if owner != user_id:
            raise HTTPException(status_code=403, detail="Not allowed")

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


@router.post("/threads")
async def create_thread(current_user: Annotated[dict, Depends(get_current_user)]):
    thread_id = str(uuid.uuid4())
    user_id = current_user["id"]
    create_thread_for_user(thread_id, user_id)
    timestamp = datetime.now(timezone.utc).isoformat()
    return {"thread_id": thread_id, "created_at": timestamp, "user_id": user_id}


@router.get("/threads")
async def get_all_threads(current_user: Annotated[dict, Depends(get_current_user)]):
    user_id = current_user["id"]
    return {"threads": get_all_threads_for_user(user_id)}


@router.post("/upload_pdf")
async def upload_pdf(
    current_user: Annotated[dict, Depends(get_current_user)],
    thread_id: str = Form(...),
    file: UploadFile = File(...),
):
    owner = get_thread_owner(thread_id)
    user_id = current_user["id"]

    if owner is None:
        raise HTTPException(status_code=404, detail="thread not found")

    if owner != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    file_bytes = await file.read()

    result = ingest_pdf(file_bytes, thread_id, file.filename)

    return {
        "thread_id": thread_id,
        "filename": result["filename"],
        "documents": result["documents"],
        "chunks": result["chunks"],
    }


@router.get("/threads/{thread_id}/documents")
async def get_document_metadata(
    thread_id: str, current_user: Annotated[dict, Depends(get_current_user)]
):
    owner = get_thread_owner(thread_id)
    user_id = current_user["id"]

    if owner is None:
        raise HTTPException(status_code=404, detail="thread not found")

    if owner != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    return thread_document_metadata(thread_id)


@router.get("/threads/{thread_id}/messages")
async def get_thread_messages(
    thread_id: str, current_user: Annotated[dict, Depends(get_current_user)]
):
    owner = get_thread_owner(thread_id)
    user_id = current_user["id"]

    if owner is None:
        raise HTTPException(status_code=404, detail="thread not found")

    if owner != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

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


@router.delete("/threads/{thread_id}")
async def remove_thread(
    thread_id: str, current_user: Annotated[dict, Depends(get_current_user)]
):
    owner = get_thread_owner(thread_id)
    user_id = current_user["id"]

    if owner is None:
        raise HTTPException(status_code=404, detail="thread not found")

    if owner != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    delete_thread(thread_id)

    return {"message": "Thread deleted successfully"}
