import streamlit as st
from dotenv import load_dotenv
import re
import os
from streamlit_extras.switch_page_button import switch_page
import time
import html
import openai 
from neo4j import GraphDatabase
import neo4j
import json
from streamlit_float import float_init, float_parent, float_css_helper
import requests

def process_query(user_input, patient_id):
    url = "http://localhost:8011/process_query"
    payload = {
        "query": user_input,
        "patientid": patient_id
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        api_response = response.json()
        main_answer = api_response.get("data", "No specific answer found in API response")
        follow_up_questions = api_response.get("follow_up_question", {}).get("choices", [])
        questions = []
        if follow_up_questions:
            question_content = follow_up_questions[0].get("message", {}).get("content", "")
            questions = [re.sub(r'^[a-c]\s*-\s*', '', q.strip()) for q in question_content.split("\n") if q.strip()]
        return main_answer, questions
    return f"Error: API call failed with status code {response.status_code}", []

# Initialize conversation history
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []

def interact_with_chatbot(user_input):
    # Add the user input to conversation history
    st.session_state.conversation_history.append({"role": "user", "content": user_input})
    # Process the query using the API
    response, follow_up_questions = process_query(user_input, st.session_state.patient_id)
    # Add the API response to conversation history
    st.session_state.conversation_history.append({"role": "assistant", "content": response})
    # Store follow-up questions in session state
    st.session_state.follow_up_questions = follow_up_questions

def send_message():
    if st.session_state.user_input:  # Check if there is input
        interact_with_chatbot(st.session_state.user_input)  # Send the input to the chatbot
        st.session_state.user_input = ""

def chat_page():
    st.title("Medical Chat Assistant")

    st.markdown(
        """
        <style>
        .chat-container {
            height: auto; /* Allow height to adjust based on content */
            overflow-y: auto;
            padding: 10px;
            background-color: #f9f9f9;
            border: 1px solid #ddd;
            border-radius: 5px;
            margin-bottom: 80px; /* Space for input and suggested questions */
        }
        .fixed-input-box {
            position: fixed;
            bottom: 80px;
            left: 0;
            width: 100%;
            background-color: #fff;
            padding: 10px;
            z-index: 9998;
            display: flex;
            align-items: center;
        }

        .fixed-input-box .stTextInput {
            flex-grow: 1;
            margin-right: 10px;
        }

        .fixed-input-box .stButton button {
            height: 38px;
            padding: 0 15px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .suggested-questions {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background-color: #f0f0f0;
        padding: 10px;
        z-index: 9997;
        display: flex;
        justify-content: space-around;
        }
       .suggested-question-button {
        background-color: #E3F2FD;
        color: black;
        border: none;
        padding: 10px 15px;
        margin: 5px;
        border-radius: 15px;
        cursor: pointer;
        font-size: 14px;
        width: 100%;
        height: 60px;
        display: flex;
        align-items: center;
        justify-content: center;
        text-align: center;
        }
       .suggested-question-button:hover {
        background-color: #C5E1F9;
        }
        /* Adjust the margin-bottom of the chat container to accommodate the follow-up questions */
        .chat-container {
            margin-bottom: 160px;
        }
        /* Adjust the bottom position of the fixed input box */
        .fixed-input-box {
            bottom: 80px;
        }
        .user-message, .assistant-message {
            display: flex;
            align-items: flex-start;
            margin-bottom: 10px; /* Space between messages */
        }
        .user-message {
            justify-content: flex-end; /* Align user messages to the right */
        }
        .message-content {
            max-width: 70%;
            padding: 10px;
            border-radius: 15px;
        }
        .user-message .message-content {
            background-color: #DCF8C6; /* User message color */
        }
        .assistant-message .message-content {
            background-color: #E3F2FD; /* Assistant message color */
        }
        .message-icon {
            width: 30px;
            height: 30px;
            margin: 0 10px; /* Space between icon and message */
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if not st.session_state.conversation_history:
        initial_message = "Hi, how can I help you today?"
        st.session_state.conversation_history.append({"role": "assistant", "content": initial_message})

    chat_container = st.container()
    with chat_container:
        for message in st.session_state.conversation_history:
            if message["role"] == "user":
                st.markdown(
                    f"""
                    <div class="user-message">
                        <div class="message-content">{message['content']}</div>
                        <img src="https://img.icons8.com/color/48/000000/user-male-circle--v1.png" class="message-icon">
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            else:
                st.markdown(
                    f"""
                    <div class="assistant-message">
                        <img src="https://img.icons8.com/color/48/000000/bot.png" class="message-icon">
                        <div class="message-content">{message['content']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        st.markdown('</div>', unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="fixed-input-box">', unsafe_allow_html=True)
        col1, col2 = st.columns([0.9, 0.1])
        with col1:
            user_input = st.text_input("Type your question here...", key="user_input", label_visibility="collapsed")
        with col2:
            if st.button("➡️"):
                if user_input:
                    interact_with_chatbot(user_input)  # Process input immediately
                    # Clear the input field by rerunning the app
                    st.rerun() 
                    # st.session_state.user_input = ""
        st.markdown('</div>', unsafe_allow_html=True)

    # Initial questions (add this part)
    if len(st.session_state.conversation_history) == 1:
        st.markdown('<div class="suggested-questions">', unsafe_allow_html=True)
        initial_questions = [
            "I want to access my medical history from the last provider visit",
            "What medicines can I take for a headache?",
            "I want information about the last claim I raised"
        ]
        cols = st.columns(len(initial_questions))
        for i, question in enumerate(initial_questions):
            with cols[i]:
                if st.button(question, key=f"initial_q_{i}", use_container_width=True):
                    interact_with_chatbot(question)
                    st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # Display follow-up questions
    if hasattr(st.session_state, 'follow_up_questions') and st.session_state.follow_up_questions:
        cols = st.columns(len(st.session_state.follow_up_questions))
        for i, question in enumerate(st.session_state.follow_up_questions):
            with cols[i]:
                if st.button(question, key=f"follow_up_{i}", use_container_width=True):
                    interact_with_chatbot(question)
                    st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

# chat page
chat_page()