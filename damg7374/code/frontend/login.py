import streamlit as st
# from openai import OpenAI
from dotenv import load_dotenv
import re
import os
import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import warnings
from neo4j import GraphDatabase
import neo4j
import json
import requests

load_dotenv()

# Set page configuration
st.set_page_config(
    page_title="Login Page",
    layout="centered",
    initial_sidebar_state="collapsed"  # Collapse the sidebar
)

def get_patient_id(first_name, last_name):
    url = "http://localhost:8011/get_patient_id"
    payload = {"first_name": first_name, "last_name": last_name}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()
    return None

def login_page():
    st.title("🩺HealthConnect")

    warnings.filterwarnings("ignore")
    
    option = st.selectbox("Select an Option:", ["Patient"])
    
    if option == "Patient":
        st.subheader("Enter Patient Details")

        first_name = st.text_input("First Name", placeholder="Enter First Name")
        last_name = st.text_input("Last Name", placeholder="Enter Last Name")
        password = st.text_input("Password", type="password", key="password")
        
        if st.button("Submit"):
            if not first_name or not last_name:
                st.error("Both first name and last name are required.")
            elif not first_name.isalpha() or not last_name.isalpha():
                st.error("First name and last name should contain only alphabetic characters.")
            else:
                response = get_patient_id(first_name, last_name)
                if response:
                    st.session_state.patient_id = response.get('patient_id')
                    st.session_state.first_name = first_name
                    st.session_state.last_name = last_name
                    st.session_state.latest_encounters = response.get('latest_encounters')
                    st.session_state.page = "healthconnect"
                    st.success("Login successful! Redirecting to the chat page...")
                    switch_page("healthconnect")
                else:
                    st.error("Patient details do not match our records.")


# Main logic to switch between pages
if "page" not in st.session_state:
    st.session_state.page = "login"  # Default page

if st.session_state.page == "login":
    login_page()