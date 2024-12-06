import os
from dotenv import load_dotenv
from writerai import Writer

load_dotenv()

def get_palmyra_response(question: str) -> str:
    print("question", question)
    client = Writer(
        api_key=os.environ.get("WRITER_API_KEY")
    )
    print("client", question)
    chat_response = client.completions.create(
        model="palmyra-x-004",
        prompt=question
    )
    print("chat_response", chat_response)
    if chat_response:
        response_content = chat_response.choices[0].text.strip()
        print(response_content)
        return response_content
    else:
        return "No response from the model."