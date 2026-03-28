import streamlit as st
from langraph_backend import chatbot
from langchain_core.messages import HumanMessage , AIMessage

CONFIG = {"configurable":{"thread_id":"thread-1"}}
#store the conversation history:
#use->st.session_state->dict->enter->(no reset)->asits
#if no message_history is there->
if 'message_history' not in st.session_state:
    st.session_state['message_history'] = []

#loading the conversation:
for message in st.session_state['message_history']:
    with st.chat_message(message["role"]):
        st.text(message['content'])

#{'role':'User' , 'content':'Hi'}
#{'role':'Assistant' , 'content':'Hello'}

user_input = st.chat_input("Type Here")

if user_input:
    #pls add the messages to the message_history
    st.session_state['message_history'].append({'role':'user' , 'content':user_input})
    with st.chat_message("user"):
        st.text(user_input)

    #add streaming_feautre
    with st.chat_message("assistant"):
        ai_message = st.write_stream(
            message_chunk.content for message_chunk , metadata in chatbot.stream(
                #initial_state:
                {'messages' : [HumanMessage(content = user_input)]},
                config = {"configurable" : {"thread_id":"thread-1"}},
                #stream_mode = update , value , cutsom , messages
                stream_mode = "messages"
            )
        )

    st.session_state['message_history'].append({"role":"assistant" , "content":ai_message})