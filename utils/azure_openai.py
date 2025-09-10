from openai import AzureOpenAI
import os 
import logging
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

OPENAI_API_KEY = "23f5ceda179a44a694ccfa205fd34cb3"
OPENAI_API_BASE = "https://swcdoai3bmaoa01.openai.azure.com/"
OPENAI_MODEL = "gpt-4.1"
OPENAI_API_VERSION = "2024-12-01-preview"
OPENAI_API_EMBEDDING_MODEL = "text-embedding-ada-002"



def get_embeddings(text):
    credential = DefaultAzureCredential()
    token_provider = get_bearer_token_provider(  
        DefaultAzureCredential(),  
        "https://cognitiveservices.azure.com/.default"  
    )  

    token = credential.get_token("https://cognitiveservices.azure.com/.default").token
    openai_client = AzureOpenAI(
            azure_ad_token=token,
            api_version = OPENAI_API_VERSION,
            azure_endpoint =OPENAI_API_BASE,
            timeout=30
            )
    
    embedding = openai_client.embeddings.create(
                 input = text,
                 model= OPENAI_API_EMBEDDING_MODEL
             ).data[0].embedding
    
    return embedding


def run_prompt(prompt,system_prompt):
    credential = DefaultAzureCredential()
    token_provider = get_bearer_token_provider(  
        DefaultAzureCredential(),  
        "https://cognitiveservices.azure.com/.default"  
    )  

    token = credential.get_token("https://cognitiveservices.azure.com/.default").token
    
    openai_client = AzureOpenAI(
        # azure_ad_token=token,
        api_key=OPENAI_API_KEY,
        api_version = OPENAI_API_VERSION,
        azure_endpoint =OPENAI_API_BASE
    )

    
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{ "role": "system", "content": system_prompt},
              {"role":"user","content":prompt}])
    
    return response.choices[0].message.content

# def run_prompt(system_prompt, user_prompt):
#     logging.info(f"API Key: {OPENAI_API_KEY}")
    
#     openai_client = AzureOpenAI(
#         api_key=OPENAI_API_KEY,
#         api_version=OPENAI_API_VERSION,
#         azure_endpoint=OPENAI_API_BASE
#     )

    
#     response = openai_client.chat.completions.create(
#         model=OPENAI_MODEL,
#         messages=[{ "role": "system", "content": system_prompt},
#               {"role":"user","content":user_prompt}])
    
#     return response.choices[0].message.content
