import streamlit as st
from langraph_database_backend import chatbot , get_threads
from langchain_core.messages import HumanMessage , AIMessage
import uuid

#utility function->dynamic thread id:
def generate_thread_id():
    thread_id = uuid.uuid4()
    return str(thread_id)

def reset_chat():
    thread_id = generate_thread_id()
    st.session_state["thread_id"] = thread_id
    #add_thread(st.session_state["thread_id"])
    if thread_id not in st.session_state["chat_threads"]:
        st.session_state["chat_threads"].append(thread_id)
    st.session_state["message_history"] = []

#generate_title:
def generate_title(message):
    words = message.split()
    return " ".join(words[:1000])

#add thread in a list:
def add_thread(thread_id):
    if thread_id not in st.session_state["chat_threads"]:
        st.session_state["chat_threads"].append(thread_id)

#def load_conv(thread_id):
    #i can extract any message from a thread
    #return chatbot.get_state(config = {"configurable" : {"thread_id":thread_id}}).values['messages']

def load_conv(thread_id):
    state = chatbot.get_state(
        config={"configurable": {"thread_id": thread_id}}
    )

    #FIX: safe access
    if not state:
        return []

    #LangGraph state object case
    if hasattr(state, "values"):
        return state.values.get("messages", [])

    #dict case
    return state.get("messages", [])

#after load_conv
def get_title(thread_id):
    messages = load_conv(thread_id)

    # agar empty hai
    if not messages:
        return "New Chat"

    # first human message = title
    for msg in messages:
        if isinstance(msg, HumanMessage):
            return msg.content[:30]   # short & clean

    return "Chat"

#session_setup
if 'message_history' not in st.session_state:
    st.session_state['message_history'] = []

if 'thread_id' not in st.session_state:
    st.session_state['thread_id'] = generate_thread_id()

if "chat_titles" not in st.session_state:
    st.session_state["chat_titles"] = {}

#you have to check how many thread already exist in your database:
if "chat_threads" not in st.session_state:
    st.session_state["chat_threads"] = get_threads()

add_thread(st.session_state['thread_id'])

#sidebar_ui
st.sidebar.title('LANGRAPH CHATBOT')

if st.sidebar.button('New Chat'):
    reset_chat()

st.sidebar.header('Conversaton History')

#st.sidebar.text(st.session_state["thread_id"])
#for tid, title in list(st.session_state["chat_titles"].items())[:-1]:
#for tid, title in reversed(list(st.session_state["chat_titles"].items())):
for tid in st.session_state["chat_threads"]:
    title = st.session_state["chat_titles"].get(tid)

    #if not title then get it:
    if not title:
        title = get_title(tid)

    if st.sidebar.button(title , key=tid):
        st.session_state["thread_id"] = tid
        messages = load_conv(tid)

        #message should be compatible:
        temp_messages = []
        for msg in messages:
            #if my curr_message is an instance of human_message
            if isinstance(msg , HumanMessage):
                role = "user"
            else:
                role = "assistant"
            temp_messages.append({"role":role , "content":msg.content})
        
        st.session_state["message_history"] = temp_messages         

#loading the conversation:
for message in st.session_state['message_history']:
    with st.chat_message(message["role"]):
        st.text(message['content'])

user_input = st.chat_input("Type Here")

if user_input:

    thread_id = st.session_state["thread_id"]

    #across a new thread_id add->chat_title:
    if thread_id not in st.session_state["chat_titles"]:
        st.session_state["chat_titles"][thread_id] = generate_title(user_input)

    #pls add the messages to the message_history
    st.session_state['message_history'].append({'role':'user' , 'content':user_input})
    with st.chat_message("user"):
        st.text(user_input)

    #CONFIG = {"configurable" : {"thread_id":st.session_state["thread_id"]}}
    #add streaming_feature

    #use_this ->chat will organise according to the thread:
    CONFIG = {
        "configurable": {"thread_id": st.session_state["thread_id"]},
        "metadata": {
            "thread_id": st.session_state["thread_id"]
        },
        #each_trace->1_turn of conversation->user-> <-AI for better readability:
        "run_name": "chat_turn",
    }

    with st.chat_message("assistant"):
        ai_message = st.write_stream(
            message_chunk.content for message_chunk , metadata in chatbot.stream(
                {'messages' : [HumanMessage(content = user_input)]},
                config = CONFIG,
                stream_mode = "messages"
            )
        )

    st.session_state['message_history'].append({"role":"assistant" , "content":ai_message})

#tracing is also implemented in .env ->can observed anything..->observability(is there)
#also i have changed the CONFIG ->so, every different conversation ->is coming in different thread
