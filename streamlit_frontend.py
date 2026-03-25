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

#list of dict:
#[{'role':'User' , 'content':'Hi'}
#{'role':'Assistant' , 'content':'Hello'}]

user_input = st.chat_input("Type Here")

if user_input:
    #pls add the messages to the message_history
    st.session_state['message_history'].append({'role':'User' , 'content':user_input})
    with st.chat_message("User"):
        st.text(user_input)

    #sending human message to llm:
    response = chatbot.invoke({'messages' : [HumanMessage(content = user_input)]} , config=CONFIG)
    ai_message = response['messages'][-1].content
    st.session_state['message_history'].append({'role':'Assistant' , 'content':ai_message})
    with st.chat_message("Assistant"):
        st.text(ai_message)
