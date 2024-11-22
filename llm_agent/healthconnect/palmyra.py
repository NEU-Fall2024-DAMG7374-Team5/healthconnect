import os
from dotenv import load_dotenv
from writerai import Writer

load_dotenv()

def get_palmyra_response(question: str) -> str:
    client = Writer(
        api_key=os.environ.get("WRITER_API_KEY")
    )
    chat_response = client.completions.create(
        model="palmyra-x-004",
        prompt=question
    )
    if chat_response:
        response_content = chat_response.choices[0].text.strip()
        return response_content
    else:
        return "No response from the model."