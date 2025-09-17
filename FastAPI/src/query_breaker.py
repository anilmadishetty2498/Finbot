"""
Query breaking module to decompose Simple Multiple queries into simpler sub-queries.
Leaves Simple and Complex Multiple queries unchanged.
"""

import warnings
warnings.filterwarnings("ignore")
from langchain.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from .data_dictionary import get_data_dictionary, get_sample_data
from .all_prompts import query_simple_multiple_break_prompt
from .logger import LoggingTool
import logging

class QueryBreaker:
    def __init__(self, llm=None, custom_dictionary=None, custom_samples=None):
        """
        Initialize the QueryBreaker class for breaking down Simple Multiple queries
        
        Args:
            llm: Language model instance
            custom_dictionary (dict, optional): Custom dictionary to use instead of default
            custom_samples (dict, op Log to consoletional): Custom sample data to use instead of default
        """
        self.logger_tool = LoggingTool(
            is_console=True,        
            logger_name="finbot"    
        )
        self.logger = self.logger_tool.create_and_set_logger()
        self.logger.info("Initializing QueryBreaker")

        self.llm = llm
        self.data_dictionary = custom_dictionary if custom_dictionary is not None else get_data_dictionary()
        self.sample_data = custom_samples if custom_samples is not None else get_sample_data()
        
        # Create formatted string representations for the prompt
        self.data_dict_str = self._format_data_dictionary()
        self.sample_data_str = self._format_sample_data()
        
        # Create the prompt template for query breaking
        self.query_break_prompt = PromptTemplate(
            template=query_simple_multiple_break_prompt,
            input_variables=["data_dictionary", "sample_data", "query"]
        )
        
        # Create runnable chain
        self.query_breaker_model = self.query_break_prompt | self.llm
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.info("QueryBreaker initialized successfully.")
    
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
            if column in self.data_dictionary:
                formatted_data.append(f"{column}: {values}")
        return "\n".join(formatted_data) if len(formatted_data) > 1 else ""
    
    def break_query(self, query: str, query_type: str = None) -> list:
        
        if not self.llm:
            self.logger.error("LLM is not initialized")
            return [query]
        
        if query_type != "SIMPLE MULTIPLE":
            self.logger.info(f"Query type {query_type} not processed by QueryBreaker; returning original query")
            return [query]
        
        try:
            result = self.query_breaker_model.invoke({
                "data_dictionary": self.data_dict_str,
                "sample_data": self.sample_data_str,
                "query": query
            })
            
            result_text = getattr(result, "content", str(result)).strip()
            
            if result_text.lower() == "none":
                self.logger.info("No breaking needed for query")
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
            
            self.logger.info(f"Query broken into: {broken_queries}")
            return broken_queries if broken_queries else [query]
            
        except Exception as e:
            self.logger.error(f"Error breaking query: {e}")
            return [query]
    
    def refresh_data_dictionary(self):
        """Refresh the data dictionary from the module"""
        self.data_dictionary = get_data_dictionary()
        self.sample_data = get_sample_data()
        self.data_dict_str = self._format_data_dictionary()
        self.sample_data_str = self._format_sample_data()