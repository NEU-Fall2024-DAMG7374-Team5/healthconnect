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
import warnings

load_dotenv()


import streamlit as st

# Set page configuration
st.set_page_config(
    page_title="Login Page",
    layout="centered",
    initial_sidebar_state="collapsed"  # Collapse the sidebar
)

# # Add CSS for a white background
# st.markdown(
#     """
#     <style>
#     body {
#         background-color: white !important;
#         color: black !important;
#     }
#     .stApp {
#         background-color: white !important;
#     }
#     </style>
#     """,
#     unsafe_allow_html=True
# )

# st.title("Health Connect")
# st.set_page_config(page_title='HealthConnect : Home',
# page_icon=	":robot_face:",
# layout = 'wide',
# initial_sidebar_state='collapsed')
initial_sidebar_state='collapsed'

# Dummy patient records for verification
patient_records = {
    "12345": {"first_name": "Linsey", "last_name": "Metz"},
    "1234-5678-90": {"first_name": "Jane", "last_name": "Smith"},
}

# Function to verify patient details
def verify_patient(patient_id, first_name, last_name):
    patient_data = patient_records.get(patient_id)
    if patient_data and patient_data["first_name"].lower() == first_name.lower() and patient_data["last_name"].lower() == last_name.lower():
        return True
    return False

# Login Page
def login_page():
    st.title("Health Connect")

    warnings.filterwarnings("ignore")
    
    # Patient login section
    option = st.selectbox("Select an Option:", ["Patient"])
    
    if option == "Patient":
        st.subheader("Enter Patient Details")

        patient_id = st.text_input("Patient ID", placeholder="Enter patient ID")
        first_name = st.text_input("First Name", placeholder="Enter First Name")
        last_name = st.text_input("Last Name", placeholder="Enter Last Name")
        
        if st.button("Submit"):
            if not patient_id or not first_name or not last_name:
                st.error("All fields are required.")
            elif not re.match(r"^\d+(-\d+)*$", patient_id):
                st.error("Invalid Patient ID format.")
            elif not first_name.isalpha() or not last_name.isalpha():
                st.error("Please check first name and last name")
            else:
                # Verify patient information
                if verify_patient(patient_id, first_name, last_name):
                    st.session_state.page = "chats"
                    st.success("Login successful! Redirecting to the chat page...")
                    # Switch to chat page after a short delay
                    switch_page("chats")
                else:
                    st.error("Patient details do not match our records.")

# Main logic to switch between pages
if "page" not in st.session_state:
    st.session_state.page = "login"  # Default page

if st.session_state.page == "login":
    login_page()