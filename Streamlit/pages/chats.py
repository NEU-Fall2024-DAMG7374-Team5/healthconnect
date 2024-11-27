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

# Initialize user input key and conversation history
if "user_input_key" not in st.session_state:
    st.session_state.user_input_key = "" 

if "messages" not in st.session_state:
    st.session_state["messages"] = [{"role": "assistant", "content": "Hello, how can I assist you today?"}]

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

def chat_page():
    st.title("Medical Chat Assistant")
    #CSS for placing the input box at the bottom
    
    st.markdown(
        """
        <style>
        .chat-container {
            height: calc(100vh - 200px);
            overflow-y: auto;
            padding: 10px;
            background-color: #f9f9f9;
            border: 1px solid #ddd;
            border-radius: 5px;
            margin-bottom: 80px;
        }
        .chat-container {
        height: auto; /* Instead of calc(100vh - 200px) */
        min-height: calc(100vh - 250px); /* Adjust as needed */
        overflow-y: auto;
        padding: 10px;
        background-color: #f9f9f9;
        border: 1px solid #ddd;
        border-radius: 5px;
        margin-bottom: 10px; /* Reduce margin */
    }

        .fixed-input-box {
            position: fixed;
            bottom: 80px;
            left: 0;
            width: 100%;
            background-color: #fff;
            padding: 10px;
            z-index: 9998;
        }
        .suggested-questions {
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            background-color: #f0f0f0;
            padding: 10px;
            z-index: 9997;
        }
        .user-message, .assistant-message {
            display: flex;
            align-items: flex-start;
            margin-bottom: 10px;
        }
        .user-message {
            justify-content: flex-end;
        }
        .message-content {
            max-width: 70%;
            padding: 10px;
            border-radius: 15px;
        }
        .user-message .message-content {
            background-color: #DCF8C6;
        }
        .assistant-message .message-content {
            background-color: #E3F2FD;
        }
        .message-icon {
            width: 30px;
            height: 30px;
            margin: 0 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Empty container for chat history
    chat_container = st.container()

    # # Display chat history
    # with chat_container:
    #     for message in st.session_state.conversation_history:
    #         if message["role"] == "user":
    #             st.markdown(
    #                 f"<div style='text-align: left; background-color: #F0F4C3; color: #000; padding: 10px; "
    #                 f"margin: 5px; border-radius: 5px;'>"
    #                 f"<strong>You:</strong> {message['content']}</div>",
    #                 unsafe_allow_html=True,
    #             )
    #         else:
    #             st.markdown(
    #                 f"<div style='text-align: left; background-color: #E3F2FD; color: #000; padding: 10px; "
    #                 f"margin: 5px; border-radius: 5px;'>"
    #                 f"<strong>Assistant:</strong> {message['content']}</div>",
    #                 unsafe_allow_html=True,
    #             )

    #             st.markdown("</div>", unsafe_allow_html=True)

     # Display chat history
    with chat_container:
        st.markdown('<div class="chat-container">', unsafe_allow_html=True)
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

     # Fixed input box above suggested questions
    with st.container():
        float_parent()
        st.markdown('<div class="fixed-input-box">', unsafe_allow_html=True)
        user_input = st.text_input("Type your question here...", key="user_input")
        st.markdown('</div>', unsafe_allow_html=True)

    # Suggested questions at the bottom
    with st.container():
        float_parent()
        st.markdown('<div class="suggested-questions">', unsafe_allow_html=True)
        suggested_questions = generate_suggested_questions()
        if suggested_questions:
            st.subheader("Suggested Follow-Up Questions:")
            cols = st.columns(len(suggested_questions))
            for i, question in enumerate(suggested_questions):
                with cols[i]:
                    if st.button(question):
                        interact_with_chatbot(question)
        st.markdown('</div>', unsafe_allow_html=True)

    # Process user input when provided
    if user_input:
        interact_with_chatbot(user_input)

    # Display the two responses side by side from both models (Palmyra Med and ChatGPT)
    if len(st.session_state.conversation_history) > 2:
        # Get the last user input
        last_user_message = st.session_state.conversation_history[-3]["content"]

        palmyra_med_response = st.session_state.conversation_history[-2]["content"]
        chatgpt_response = st.session_state.conversation_history[-1]["content"]

        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("Palmyra Med Response:")
            st.markdown(f"<div style='background-color: #E3F2FD; color: #000; padding: 10px; "
                        f"border-radius: 5px;'>{palmyra_med_response}</div>",
                        unsafe_allow_html=True)

        with col2:
            st.subheader("ChatGPT Response:")
            st.markdown(f"<div style='background-color: #F0F4C3; color: #000; padding: 10px; "
                        f"border-radius: 5px;'>{chatgpt_response}</div>",
                        unsafe_allow_html=True)
            
# chat page
chat_page()