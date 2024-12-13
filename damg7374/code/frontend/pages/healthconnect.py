from openai import OpenAI
import streamlit as st
import re
import requests
import pandas as pd

# Set page configuration
st.set_page_config(
    page_title="HealthConnect",
    layout="wide",
    initial_sidebar_state="collapsed"  # Collapse the sidebar
)


def format_initial_message(data):

    message = "List of latest 3 encounters/appointments:\n\n\n"
    for encounter in data:
        organization = encounter['organization']
        provider = encounter['provider']
        encounter_id = encounter['encounter_id']
        start_date = encounter['start_date']
        message_line = f"Encounter Id: {encounter_id}\n\n Date: {start_date}\n\n Occured At: {organization}\n\n Treated By: {provider}\n\n\n"
        message+=message_line
    
    return message

def send_message(messages, patient_id):

    current_user_message = messages[-1]["content"]

    pattern = r"Encounter Summary = '([a-fA-F0-9\-]+)'"

    match = re.search(pattern, current_user_message)
    if match:
        print("Using Encounter Summary API endpoint")
        print(match.group(1))
        url = "http://localhost:8011/encounter_summary"
        payload = {
            "encounter_id": match.group(1),
            "patient_id": patient_id
        }
    else:
        print("Using Process Query API endpoint")
        url = "http://localhost:8011/process_query"
        payload = {
            "query": current_user_message,
            "patientid": patient_id
        }

    response = requests.post(url, json=payload)
    if response.status_code == 200:
        api_response = response.json()
        print(api_response)
        main_answer = api_response.get("data", "No specific answer found in API response")
        follow_up_questions = api_response.get("follow_up_question", {}).get("choices", [])
        questions = []
        if follow_up_questions:
            question_content = follow_up_questions[0].get("message", {}).get("content", "")
            pattern = r"\b([abc]) - (.*?)\?"
            matches = re.findall(pattern, question_content)
            questions = [ f"{match[1]}?" for match in matches]
            # questions = [re.sub(r'^[a-c]\s*-\s*', '', q.strip()) for q in question_content.split("\n") if q.strip()]
        return main_answer, questions

    return f"Error: API call failed with status code {response.status_code}", []


st.title("🩺 HealthConnect")
st.info(f"👋 Hi, {st.session_state.first_name.capitalize()} {st.session_state.last_name.capitalize()}! Following is the list of your latest 3 encounters:")
st.table(pd.DataFrame(st.session_state.latest_encounters).rename(columns={"organization":"Treated At", "provider":"Treated By", "encounter_id":"Encounter ID", "start_date": "Date"}))

if "messages" not in st.session_state:
    st.session_state["messages"] = [{"role": "assistant", "content": f"You can start with either these followup questions or type your question below"}]

get_avatar = lambda role: {"user": '👨🏻', "assistant": '🤖'}.get(role, "❓")

for msg in st.session_state.messages:
    st.chat_message(msg["role"], avatar=get_avatar(msg["role"])).write(msg["content"])

chat_box = st.chat_input(placeholder="Please type your question here")
clicked_button = None
if 'prompt' not in st.session_state or st.session_state.prompt is None:
    

    if 'followup_questions' in st.session_state:
        button_options = st.session_state.followup_questions
    else:
        button_options = [
            "List my claims with their all details?",
            f"Encounter Summary = '{st.session_state.latest_encounters[0]['encounter_id']}'",
            "List my encounters with ids, start and end dates?",
        ]

    for option in button_options:
        if st.button(option, type='primary'):
            clicked_button = option

if clicked_button:
    st.session_state.prompt = clicked_button
else:
    st.session_state.prompt = chat_box


if 'prompt' in st.session_state and st.session_state.prompt is not None:
    print("Prompt Present in Session State")
    print("-----------")
    print(st.session_state)
    print("-----------")
    st.session_state.messages.append({"role": "user", "content": st.session_state.prompt})
    st.chat_message("user").markdown(st.session_state.prompt)

    # Call the custom function
    response = send_message(st.session_state.messages, st.session_state.patient_id)
    answer = response[0]
    st.session_state.followup_questions = response[1]

    # Add assistant response to session state
    st.session_state.messages.append({"role": "assistant", "content": answer})
    # st.chat_message("assistant").write(answer)
    del st.session_state['prompt']
    print("-----------")
    print(st.session_state)
    print("-----------")
    st.rerun()