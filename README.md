# 🤖 Chatbot Using LangGraph

A full-stack AI chatbot application built with **LangGraph**, **FastAPI**, and **React**. It features multi-turn conversational threads, tool-calling capabilities (web search, calculator, stock prices), and PDF-based Retrieval-Augmented Generation (RAG) — all powered by open-source HuggingFace models.

---

## ✨ Features

- **Multi-turn conversations** — Persistent chat threads with SQLite-backed checkpointing via LangGraph.
- **Tool calling** — The LLM can autonomously invoke tools:
  - 🔍 **Web Search** — Real-time search via DuckDuckGo.
  - 🧮 **Calculator** — Basic arithmetic operations.
  - 📈 **Stock Prices** — Live quotes from Alpha Vantage.
  - 📄 **RAG (PDF Q&A)** — Upload a PDF and ask questions about its content.
- **PDF ingestion** — Upload PDFs through the UI; documents are chunked, embedded, and indexed with FAISS for per-thread retrieval.
- **Thread management** — Create, switch between, and delete conversation threads from a sidebar.
- **Timestamped messages** — Every user and AI message is timestamped for reference.
- **Responsive UI** — Mobile-friendly layout with a collapsible sidebar and hamburger menu.
- **Markdown rendering** — AI responses are rendered with full Markdown and GFM table support.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     React Frontend                       │
│              (Vite + React 19 + Axios)                   │
│  ┌──────────┐ ┌──────────┐ ┌─────────┐ ┌─────────────┐  │
│  │ Sidebar  │ │ Navbar   │ │MainChat │ │  InputBox   │  │
│  └──────────┘ └──────────┘ └─────────┘ └─────────────┘  │
└────────────────────────┬─────────────────────────────────┘
                         │  HTTP (REST API)
┌────────────────────────▼─────────────────────────────────┐
│                   FastAPI Backend                         │
│  ┌──────────────────────────────────────────────────┐    │
│  │              LangGraph Agent                     │    │
│  │  ┌───────────┐    ┌─────────────────────────┐    │    │
│  │  │ Chat Node │◄──►│      Tool Node          │    │    │
│  │  │ (Qwen 2.5)│    │ (Search/Calc/Stock/RAG) │    │    │
│  │  └───────────┘    └─────────────────────────┘    │    │
│  └──────────────────────────────────────────────────┘    │
│  ┌────────────────┐  ┌──────────────────────────────┐    │
│  │ SQLite (state) │  │ FAISS (vector store / RAG)   │    │
│  └────────────────┘  └──────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
Chatbot-Using-Langraph/
├── backend/
│   ├── main.py              # FastAPI app & API endpoints
│   ├── graph.py             # LangGraph state graph assembly
│   ├── tools.py             # Tool definitions (search, calc, stock, RAG)
│   ├── models.py            # LLM & embedding model configuration
│   ├── database.py          # SQLite checkpointer & thread helpers
│   ├── pdf_ingestion.py     # PDF loading, chunking & FAISS indexing
│   ├── requirements.txt     # Python dependencies
│   └── .env                 # Environment variables (not committed)
├── frontend/
│   ├── src/
│   │   ├── App.jsx          # Root component with layout & state
│   │   ├── main.jsx         # Vite entry point
│   │   ├── index.css        # Global styles
│   │   ├── api/             # Axios API client
│   │   └── components/
│   │       ├── Sidebar.jsx  # Thread list & management
│   │       ├── Navbar.jsx   # Top bar with mobile toggle
│   │       ├── MainChat.jsx # Message display area
│   │       └── InputBox.jsx # Chat input & PDF upload
│   ├── package.json
│   └── vite.config.js
└── model/                   # Legacy Streamlit prototypes
```

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.11+
- **Node.js** 18+
- A [HuggingFace API token](https://huggingface.co/settings/tokens)
- *(Optional)* A [LangSmith API key](https://smith.langchain.com/) for tracing

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/Chatbot-Using-Langraph.git
cd Chatbot-Using-Langraph
```

### 2. Backend setup

```bash
cd backend

# Create and activate a virtual environment
python -m venv venvBackend
# Windows
venvBackend\Scripts\activate
# macOS / Linux
source venvBackend/bin/activate

# Install dependencies
pip install -r requirements.txt
```

Create a `.env` file in the `backend/` directory:

```env
HUGGINGFACEHUB_API_TOKEN="hf_your_token_here"

# Optional — LangSmith tracing
LANGCHAIN_TRACING_V2=true
LANGCHAIN_ENDPOINT="https://api.smith.langchain.com"
LANGCHAIN_API_KEY="lsv2_pt_your_key_here"
LANGCHAIN_PROJECT="Chatbot Project"
```

Start the backend server:

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`.

### 3. Frontend setup

```bash
cd frontend

# Install dependencies
npm install

# Start the dev server
npm run dev
```

The UI will be available at `http://localhost:5173`.

---

## 🔌 API Endpoints

| Method   | Endpoint                            | Description                         |
|----------|-------------------------------------|-------------------------------------|
| `POST`   | `/api/chat`                         | Send a message & get an AI response |
| `POST`   | `/api/threads`                      | Create a new conversation thread    |
| `GET`    | `/api/threads`                      | List all conversation threads       |
| `GET`    | `/api/threads/{thread_id}/messages` | Retrieve messages for a thread      |
| `GET`    | `/api/threads/{thread_id}/documents`| Get PDF metadata for a thread       |
| `DELETE` | `/api/threads/{thread_id}`          | Delete a thread                     |
| `POST`   | `/api/upload_pdf`                   | Upload & ingest a PDF for RAG       |

---

## 🛠️ Tech Stack

| Layer      | Technology                                                       |
|------------|------------------------------------------------------------------|
| LLM        | [Qwen 2.5-7B-Instruct](https://huggingface.co/Qwen/Qwen2.5-7B-Instruct) via HuggingFace Inference API |
| Embeddings | [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) (sentence-transformers) |
| Agent      | [LangGraph](https://github.com/langchain-ai/langgraph) (stateful, tool-calling graph) |
| Backend    | [FastAPI](https://fastapi.tiangolo.com/) + [Uvicorn](https://www.uvicorn.org/) |
| Vector DB  | [FAISS](https://github.com/facebookresearch/faiss) (in-memory, per-thread) |
| State DB   | SQLite via `langgraph-checkpoint-sqlite`                         |
| Frontend   | [React 19](https://react.dev/) + [Vite](https://vite.dev/)      |
| HTTP       | [Axios](https://axios-http.com/)                                 |
| Markdown   | [react-markdown](https://github.com/remarkjs/react-markdown) + remark-gfm |

---

## 📋 Environment Variables

| Variable                   | Required | Description                           |
|----------------------------|----------|---------------------------------------|
| `HUGGINGFACEHUB_API_TOKEN` | ✅       | HuggingFace Inference API token       |
| `LANGCHAIN_TRACING_V2`     | ❌       | Enable LangSmith tracing (`true`)     |
| `LANGCHAIN_API_KEY`        | ❌       | LangSmith API key                     |
| `LANGCHAIN_PROJECT`        | ❌       | LangSmith project name                |

---

## 📝 License

This project is for educational / personal use.
