"""
Query classification module to classify queries into SIMPLE, SIMPLE MULTIPLE, or COMPLEX MULTIPLE types.
"""

import warnings
warnings.filterwarnings("ignore")

#from langchain_core.prompts import PromptTemplate
#from prompt import data_dictionary, classify_prompt, sample_data, QUERY_CORRECTION_PROMPT, QUERY_COMPLEX_BREAK_PROMPT
#from getllm import return_llm_obj

from langchain.prompts import PromptTemplate
from .all_prompts import query_classify_prompt


class QueryClassifier:
    def __init__(self,llm = None):
        # Load LLM
        #self.llm = return_llm_obj()
        self.llm = llm

        self.query_classifier_prompt = PromptTemplate(
            template=query_classify_prompt,
            input_variables=["query"]
        )

        self.query_classifier_model = self.query_classifier_prompt | self.llm


    def classify_query(self, user_query: str) -> str:

        if not self.llm:
            print("Error: LLM is not initialized")
            return user_query
        
        try:

            result = self.query_classifier_model.invoke({
                "query": user_query
            })
        
            if hasattr(result, "content"):
                return result.content.strip()
            else:
                return str(result).strip()

        
        except Exception as e:
            print(f"Error classifying query: {e}")
            return user_query