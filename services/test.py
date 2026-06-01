from google import genai

client = genai.Client(api_key="AQ.Ab8RN6LHQz9Lo4buptTQJ4uhjNB0fFy-Ao7lHRI-oRU0r6z17g")

response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents="Explain RAG"
)
print(response.text)