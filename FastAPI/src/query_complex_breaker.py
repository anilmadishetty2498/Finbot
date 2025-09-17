
"""
Query breaking module to decompose complex queries into simpler sub-queries.
Only breaks Simple Multiple and Complex Multiple queries, leaves Simple queries as is.
"""

import warnings
warnings.filterwarnings("ignore")

#from langchain_core.prompts import PromptTemplate
#from prompt import data_dictionary, classify_prompt, sample_data, QUERY_CORRECTION_PROMPT, QUERY_COMPLEX_BREAK_PROMPT
#from getllm import return_llm_obj

from langchain.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from .data_dictionary import get_data_dictionary, get_sample_data
from .all_prompts import query_complex_break_prompt


class QueryComplexBreaker:
    def __init__(self,llm = None, custom_dictionary=None, custom_samples=None):
        # Load LLM
        #self.llm = return_llm_obj()
        self.llm = llm

        # Use provided dictionaries or get from data_dictionary module
        self.data_dictionary = custom_dictionary if custom_dictionary is not None else get_data_dictionary()
        self.sample_data = custom_samples if custom_samples is not None else get_sample_data()
        
        # Create formatted string representations for the prompt
        self.data_dict_str = self._format_data_dictionary()
        self.sample_data_str = self._format_sample_data()

        self.query_complex_break_prompt = PromptTemplate(
            template=query_complex_break_prompt,
            input_variables=["query", "data_dictionary", "sample_data"]
        )

        self.query_complex_breaker_model = self.query_complex_break_prompt | self.llm


    def _format_data_dictionary(self):
        """Format the data dictionary for inclusion in the prompt"""
        formatted_dict = []
        for column, description in self.data_dictionary.items():
            formatted_dict.append(f"{column}: {description}")
        return "\n".join(formatted_dict)
    
    def _format_sample_data(self):
        """Format the sample data for inclusion in the prompt"""
        if not self.sample_data:
            return ""
        
        formatted_data = ["Sample values for columns:"]
        for column, values in self.sample_data.items():
            if column in self.data_dictionary:  # Only include if column is in dictionary
                formatted_data.append(f"{column}: {values}")
        
        return "\n".join(formatted_data) if len(formatted_data) > 1 else ""


    def break_query(self, query: str, query_type: str = None) -> list:
        if not self.llm:
            print("Error: LLM is not initialized")
            return [query]
        try:
            input_data = {
                "query": query,
                "data_dictionary": self.data_dict_str,
                "sample_data": self.sample_data_str
            }
            result = self.query_complex_breaker_model.invoke(input_data)

            result_text = getattr(result, "content", str(result)).strip()

            if result_text.lower() == "none":
                return [query]

            broken_queries = []
            for line in result_text.split('\n'):
                if not line.strip():
                    continue
                parts = line.strip().split('. ', 1)
                if len(parts) > 1 and parts[0].isdigit():
                    broken_queries.append(parts[1])
                else:
                    broken_queries.append(line.strip())

            return broken_queries if broken_queries else [query]

        except Exception as e:
            print(f"Error breaking query: {e}")
            return [query]
        

    def refresh_data_dictionary(self):
        """Refresh the data dictionary from the module"""
        self.data_dictionary = get_data_dictionary()
        self.sample_data = get_sample_data()
        self.data_dict_str = self._format_data_dictionary()
        self.sample_data_str = self._format_sample_data()

