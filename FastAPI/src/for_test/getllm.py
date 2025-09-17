import os
import openai
from .envnew import (OPENAI_API_BASE, 
                    OPENAI_API_KEY, 
                    OPENAI_API_TYPE, 
                    OPENAI_API_VERSION
)
from langchain.agents import AgentType
from langchain_experimental.agents import create_pandas_dataframe_agent
#from langchain_community.chat_models import AzureChatOpenAI
from langchain_openai import AzureChatOpenAI
# from llama_index.llms.azure_openai import AzureOpenAI as LlamaIndexAzureOpenAI

def return_llm_obj() : 

    os.environ["OPENAI_API_TYPE"] = OPENAI_API_TYPE
    os.environ["OPENAI_API_BASE"] = OPENAI_API_BASE
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
    os.environ["OPENAI_API_VERSION"] = OPENAI_API_VERSION
    openai.api_key = os.environ["OPENAI_API_KEY"]
    openai.api_type = os.environ["OPENAI_API_TYPE"]
    openai.api_version = os.environ["OPENAI_API_VERSION"]
    openai.api_base = os.environ["OPENAI_API_BASE"]

    DEPLOYMENT_NAME =  "gpt-4o" #"gpt4-turbo" #"polaris35turbo" #r"gpt-4o-mini-RAK061"

    llm = AzureChatOpenAI(deployment_name=DEPLOYMENT_NAME,
			openai_api_key=openai.api_key,
			azure_endpoint=openai.api_base,
			openai_api_type=openai.api_type,
			openai_api_version=openai.api_version,
                      max_tokens=1500,
                      temperature = 0,
                      model_kwargs = {'seed':100,'top_p':0}
                     )
    return llm

def return_llm_obj_llamaindex() : 

    os.environ["OPENAI_API_TYPE"] = OPENAI_API_TYPE
    os.environ["OPENAI_API_BASE"] = OPENAI_API_BASE
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
    os.environ["OPENAI_API_VERSION"] = OPENAI_API_VERSION
    openai.api_key = os.environ["OPENAI_API_KEY"]
    openai.api_type = os.environ["OPENAI_API_TYPE"]
    openai.api_version = os.environ["OPENAI_API_VERSION"]
    openai.api_base = os.environ["OPENAI_API_BASE"]

    DEPLOYMENT_NAME =  "gpt-4o" #"gpt4-turbo" #"polaris35turbo" #r"gpt-4o-mini-RAK061"

    llm = LlamaIndexAzureOpenAI(
                        model = 'gpt-3.5-turbo',
                        deployment_name = DEPLOYMENT_NAME,
                        api_key = openai.api_key,
                        azure_endpoint = openai.api_base,
                        api_version = openai.api_version,
                        max_tokens=1500,
                        temperature = 0
    )
    return llm