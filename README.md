# Overview
Have you ever felt overwhelmed trying to access your medical records, track past claims, or recall details from hospital visits? The process can be stressful and time-consuming, and leave you frustrated and confused. What if a solution could simplify this experience and put all your healthcare information together in one place?

HealthConnect, a chatbot application designed to change how patients interact with their medical history. Our goal is to make healthcare data more accessible, understandable, and actionable for patients, to empower patients and improve healthcare information accessibility.

## The Chat Interface
![Chat Interfact](https://github.com/user-attachments/assets/a159321e-3207-4f31-8109-5a3c32b57ee9)

A chatbot application that is designed to change how patients interact with their medical history. Our app provides a seamless interface for accessing medical records, checking claim statuses, reviewing hospital and doctor visits, and asking medical questions. It aims to empower patients by giving them easy access to their health information, all through a conversational AI interface.

## Architecture diagram 
![Architecture diagram ](https://github.com/user-attachments/assets/ba650b85-419b-4917-a468-876177aff8ae)


## Key features:
- **Data Pipeline**: We have aggregated data from 18 different datatsets to create a holistic knowledge graph that mirrors a patient's healthcare journey in the real words
- **Knowledge graphs (KG)**: Created a knowledge graph consisting of 27 nodes and 45 relationships to capture, analyze and visualize the data model
- **Langchain**: Ability to switch between multiple models depending on the patient's question
- **Guard Rails for Data Security and Access Control** - The system incorporates stringent guard rails to ensure data security and maintain patient privacy
    - It validates Cypher queries to prevent unauthorized access to the Neo4j graph database, ensuring that only valid information is retrieved.
    - Access is highly restrictive, preventing patients from viewing others' information or making unauthorized changes to their profiles.
    - Requests for profile edits or administrative actions are redirected to designated admin workflows, maintaining system integrity and compliance         with privacy standards.

## Understanding the Agentic Architecture
![Agentic Architecture](https://github.com/user-attachments/assets/efca256a-6cc9-4ea7-b9dd-a59d1517b479)

We have employed an agentic architecture to handle patient inquiries intelligently. When a user asks a question, the system routes it to determine if the query requires medical history. In which case it generates a Cypher query to fetch relevant data from the Neo4j database, and sends it along with the context to the Palmyra Med model. For general medical questions, the query is directly sent to Palmyra Med for a response, ensuring precise and context-aware interactions.


# Project Members:
1. Aditya Kawale (kawale.a@northeastern.edu)
2. Dhawal Negi (negi.d@northeastern.edu)
3. Anandita Deb (deb.a@northeastern.edu)

