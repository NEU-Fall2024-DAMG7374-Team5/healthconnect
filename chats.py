import streamlit as st
# from openai import OpenAI
from dotenv import load_dotenv
import re
import os
from writerai import Writer
import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import time
import html
import openai 
from neo4j import GraphDatabase
import neo4j
import json
from streamlit_float import float_init, float_parent, float_css_helper

# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
client = Writer(api_key=os.environ.get("WRITER_API_KEY"))

# Initialize conversation history
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []

def get_gpt_response(user_input):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-1106",  
            messages=[
                {"role": "user", "content": user_input}
            ]
        )
        
        # Extract the assistant's response from the OpenAI API response
        gpt_response = response['choices'][0]['message']['content']
        return gpt_response
    except Exception as e:
        return f"Error: {str(e)}"

def interact_with_chatbot(user_input):
    # Add the user input to conversation history
    st.session_state.conversation_history.append({"role": "user", "content": user_input})

    # Call the Palmyra medical model to get a response
    chat = client.chat.chat(
        messages=st.session_state.conversation_history,
        model="palmyra-med"
    )

    gpt_response = get_gpt_response(user_input)

    palmyra_med_response = chat.choices[0].message.content
    # Add responses to conversation history
    st.session_state.conversation_history.append({"role": "assistant", "content": palmyra_med_response})
    st.session_state.conversation_history.append({"role": "assistant", "content": gpt_response})


# Generate follow-up questions dynamically
def generate_suggested_questions():
    if len(st.session_state.conversation_history) > 1:
        context = "\n".join([message['content'] for message in st.session_state.conversation_history])
        prompt = f"Based on the conversation context:\n{context}\n\nGenerate 3 follow-up questions."
        chat = client.chat.chat(
            messages=[{"role": "system", "content": "You are a helpful assistant."},
                      {"role": "user", "content": prompt}],
            model="palmyra-med"
        )
        return re.findall(r"\d*\.\s*([^0-9\n]+)", chat.choices[0].message.content.strip())
    return []

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
        display: flex;
        justify-content: space-around;
        padding: 10px;
        background-color: #f0f0f0; /* Background for suggested questions container */
        }
        .suggested-question-button {
        background-color: #E3F2FD; /* Same as assistant message color */
        color: black;
        border: none;
        padding: 10px 15px;
        margin: 5px;
        border-radius: 15px;
        cursor: pointer;
        font-size: 14px;
        width: 100%; /* Full width of column */
        height: 60px; /* Fixed height for consistency */
        display: flex;
        align-items: center;
        justify-content: center;
        text-align: center;
        }
        .suggested-question-button:hover {
        background-color: #C5E1F9; /* Slightly darker color on hover */
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
    # Display initial message if conversation history is empty
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
    def send_message():
        if st.session_state.user_input:
            interact_with_chatbot(st.session_state.user_input)
            st.session_state.user_input = ""  # Clear the input after sending

    with st.container():
        st.markdown('<div class="fixed-input-box">', unsafe_allow_html=True)
        col1, col2 = st.columns([0.9, 0.1])
        
        with col1:
            user_input = st.text_input("Type your question here...", key="user_input", label_visibility="collapsed")
        
        with col2:
            st.button("➡️", on_click=send_message)  # Button triggers the callback function
        
        st.markdown('</div>', unsafe_allow_html=True)

    # Suggested questions (if applicable)
    with st.container():
        
        initial_questions = [
            "I want to access my medical history from the last doctor visit",
            "I have a medical question",
            "I want information about a recent claim"
        ]
        
        if len(st.session_state.conversation_history) == 1:
            questions = initial_questions
        else:
            questions = generate_suggested_questions()
        
        if questions:
            # st.subheader("Suggested Questions:")
            cols = st.columns(len(questions))
            
            for i, question in enumerate(questions):
                with cols[i]:
                    if st.button(question, key=f"suggested_q_{i}", 
                                on_click=lambda q=question: interact_with_chatbot(q),
                                use_container_width=True):
                        pass
        
        st.markdown('</div>', unsafe_allow_html=True)

    # Display the two responses side by side from both models (Palmyra Med and ChatGPT)
    if len(st.session_state.conversation_history) > 2:
        # Get the last user input
        last_user_message = st.session_state.conversation_history[-3]["content"]

        palmyra_med_response = st.session_state.conversation_history[-2]["content"]
        chatgpt_response = st.session_state.conversation_history[-1]["content"]

# chat page
chat_page()