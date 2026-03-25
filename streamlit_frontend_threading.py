import streamlit as st
from langraph_tool_backend import chatbot
from langchain_core.messages import HumanMessage , AIMessage
import uuid

#utility function->dynamic thread id:
def generate_thread_id():
    thread_id = uuid.uuid4()
    return str(thread_id)

def reset_chat():
    thread_id = generate_thread_id()
    st.session_state["thread_id"] = thread_id
    add_thread(st.session_state["thread_id"])
    st.session_state["message_history"] = []

#generate_title:
def generate_title(message):
    words = message.split()
    return " ".join(words[:1000])

#add thread in a list:
def add_thread(thread_id):
    if thread_id not in st.session_state["chat_threads"]:
        st.session_state["chat_threads"].append(thread_id)

def load_conv(thread_id):
    #i can extract any message from a thread
    return chatbot.get_state(config = {"configurable" : {"thread_id":thread_id}}).values['messages']

#session_setup
if 'message_history' not in st.session_state:
    st.session_state['message_history'] = []

if 'thread_id' not in st.session_state:
    st.session_state['thread_id'] = generate_thread_id()

if "chat_titles" not in st.session_state:
    st.session_state["chat_titles"] = {}

if "chat_threads" not in st.session_state:
    st.session_state["chat_threads"] = []

add_thread(st.session_state['thread_id'])

#sidebar_ui
st.sidebar.title('LANGRAPH CHATBOT')

if st.sidebar.button('New Chat'):
    reset_chat()

st.sidebar.header('Conversaton History')

#st.sidebar.text(st.session_state["thread_id"])
#for tid, title in list(st.session_state["chat_titles"].items())[:-1]:
for tid, title in reversed(list(st.session_state["chat_titles"].items())):
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

    CONFIG = {"configurable" : {"thread_id":st.session_state["thread_id"]}}
    #add streaming_feautre
    #with st.chat_message("assistant"):
        #ai_message = st.write_stream(
            #message_chunk.content for message_chunk , metadata in chatbot.stream(
                #here tool message is also getting stream ->i want AI message only
                #{'messages' : [HumanMessage(content = user_input)]},
                #config = CONFIG,
                #stream_mode = "messages"
            #)
        #)

    #now i will see AI message steamed only like not the whole backend:
    with st.chat_message("assistant"):
        def ai_only_stream():
            for message_chunk, metadata in chatbot.stream(
                {"messages": [HumanMessage(content=user_input)]},
                config=CONFIG,
                stream_mode="messages"
            ):
                if isinstance(message_chunk, AIMessage):
                    # yield only assistant tokens
                    yield message_chunk.content

        ai_message = st.write_stream(ai_only_stream())

    st.session_state['message_history'].append({"role":"assistant" , "content":ai_message})