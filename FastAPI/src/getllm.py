import os
import openai
from langchain.agents import AgentType
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import AzureChatOpenAI
import platform
from pathlib import Path

# Determine secret path based on OS
OPENAI_SECRET_DIR = (
    Path("C:\tmp\openai") if platform.system() == "Windows" else Path("/etc/openai")
)

def read_secret(name: str) -> str:
    """Reads a secret value from a file in the mounted secret directory."""
    path = OPENAI_SECRET_DIR / name
    
    if not path.exists():
        raise FileNotFoundError(f"❌ Secret file '{name}' not found at {path}")
    
    content = path.read_text(encoding='utf-8').strip()
    if not content:
        raise ValueError(f"⚠️ Secret '{name}' is empty.")
    
    return content
    

OPENAI_API_KEY = read_secret("OPENAI_API_KEY")
OPENAI_API_BASE = read_secret("OPENAI_API_BASE")
OPENAI_API_TYPE = read_secret("OPENAI_API_TYPE")
OPENAI_API_VERSION = read_secret("OPENAI_API_VERSION")


def return_llm_obj():
    """Returns a configured AzureChatOpenAI LLM object using environment variables."""

    # Read environment variables (already passed via Docker or GitHub secrets)
    openai.api_key = OPENAI_API_KEY
    openai.api_base = OPENAI_API_BASE
    openai.api_type = OPENAI_API_TYPE
    openai.api_version = OPENAI_API_VERSION

    DEPLOYMENT_NAME = "gpt-4o"  # Update as needsed

    # Create and return the LLM object
    llm = AzureChatOpenAI(
        deployment_name=DEPLOYMENT_NAME,
        openai_api_key=openai.api_key,
        azure_endpoint=openai.api_base,
        openai_api_type=openai.api_type,
        openai_api_version=openai.api_version,
        max_tokens=1500,
        temperature=0,
        model_kwargs={'seed': 100, 'top_p': 0}
    )
    return llm


# def return_llm_obj(OPENAI_API_KEY, OPENAI_API_BASE,OPENAI_API_TYPE,OPENAI_API_VERSION):
#     """Returns a configured AzureChatOpenAI LLM object using environment variables."""

#     # Read environment variables (already passed via Docker or GitHub secrets)
#     openai.api_key = OPENAI_API_KEY
#     openai.api_base = OPENAI_API_BASE
#     openai.api_type = OPENAI_API_TYPE
#     openai.api_version = OPENAI_API_VERSION

#     DEPLOYMENT_NAME = "gpt-4o"  # Update as needsed

#     # Create and return the LLM object
#     llm = AzureChatOpenAI(
#         deployment_name=DEPLOYMENT_NAME,
#         openai_api_key=openai.api_key,
#         azure_endpoint=openai.api_base,
#         openai_api_type=openai.api_type,
#         openai_api_version=openai.api_version,
#         max_tokens=1500,
#         temperature=0,
#         model_kwargs={'seed': 100, 'top_p': 0}
#     )
#     return llm

