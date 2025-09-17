import multiprocess as mp
import ast
from .getllm import return_llm_obj     ######## change required to run locally src.for_test.getllm import return_llm_obj
from .logger import LoggingTool
from .task_descriptions import tasks
from .class_task_dict import TaskDictionary
from .class_chat_history import ChatHistory
from langchain.prompts import PromptTemplate
from .combined_query_corrector import CombinedQueryCorrector  # Import new corrector
from .query_classifier import QueryClassifier
from .query_complex_breaker import QueryComplexBreaker
from .query_breaker import QueryBreaker
from .class_run_parallel import RunParallel
from .class_update_prompt import UpdatePrompt
from langchain_core.output_parsers import StrOutputParser
from .class_partial_prompt_creator import PartialPromptCreator
from .all_prompts import query_prompt, task_identification_prompt
from langchain_experimental.tools.python.tool import PythonAstREPLTool
from datetime import datetime
#from .data import data_dict
from .utility_ import minimize_mem_size

# import os
# os.environ["MODIN_MEMORY"] = "16GB"  # or a bit more than your data size
# os.environ["MODIN_ENGINE"] = "ray"

# import ray 
# ray.init(
#     _memory=16 * 1024**3,                # Heap memory (Ray object refs)
#     object_store_memory=4 * 1024**3,     # For Modin shuffling
#     num_cpus=4,                          # Respect pod request/limit
#     include_dashboard=False,
#     ignore_reinit_error=True,
#     namespace="modin_app"
# )

# import modin.config as modin_cfg
# import logging
# from .data import data_dict  # Import the data dictionary
# modin_cfg.Engine.put("ray")
# print(f"Current engine: {modin_cfg.Engine.get()}")

#import modin.pandas as pd
import pandas as pd
import multiprocess as mp

import platform
from pathlib import Path


# Updated create_dynamic_data_dict to accept a DataFrame instead of a CSV path
def create_dynamic_data_dict(df):
    """
    Optimize memory usage and create a dynamic data dictionary from a DataFrame.
    Only includes columns with data_type 'object', 'category', or specific excluded columns.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        dict: A dictionary with column names as keys and cleaned unique values as values.
    """
    # Optimize memory usage
    optimized_df = minimize_mem_size(pd.DataFrame(), df)

    # Define columns to include
    col_names_exclude = ["Week", "Month_Unique", "Quarter_Unique", "Year"]

    # Create clean data dictionary
    data_dict = {}
    for col in optimized_df.columns:
        # Only include columns with object/category data types or specified columns
        if (optimized_df[col].dtype in ['object', 'category']) or (col in col_names_exclude):
            unique_vals = optimized_df[col].dropna().unique()
            clean_vals = []
            for val in unique_vals:
                try:
                    # Handle different data types properly
                    if pd.isna(val):
                        continue
                    # Convert to string and clean encoding
                    clean_val = str(val).encode('utf-8', errors='ignore').decode('utf-8')
                    # Remove any remaining problematic characters
                    clean_val = ''.join(char for char in clean_val if ord(char) < 127 or char.isalnum() or char in ' .,_-')
                    clean_vals.append(clean_val)
                except:
                    continue
            data_dict[col] = clean_vals

    return data_dict

class FinBotClass:
    def __init__(self, llm=None, df=None, role="Distinguished Data Scientist and Pandas Expert", 
                 logger=None, data_dictionary=None, sample_data=None):
        """
        Initialize the FinBotClass for processing financial data queries
        
        Args:
            llm: Language model instance (default: None)
            df: pandas DataFrame (default: None)
            role: Role description for the LLM (default: str)
            logger: Logger instance (default: None)
            data_dictionary: Data dictionary for query correction (default: None)
            sample_data: Sample data values for enhanced value correction (default: None)
        """
        self.role = role
        
        # Initialize LLM
        if llm is None:
            self.llm = return_llm_obj()
        else:
            self.llm = llm
            
        self.df = df
        self.sample_data = sample_data
        self.str_out_parser = StrOutputParser()
        self.data_dictionary = data_dictionary
        
        # Initialize logger
        if logger is None:
            log_tool = LoggingTool(filename="finbot.log", is_console=True)
            self.logger = log_tool.create_and_set_logger()
        else:
            self.logger = logger
            
        self.logger.info(f"FinBotClass initialized successfully at {datetime.now()}")

    def create_classes(self):
        """
        Creates all the necessary class instances for the query processing pipeline
        """
        self.logger.info("Creating all necessary class instances.")
        try:
            # Create core classes
            self.partial_prompt_obj = PartialPromptCreator(
                role=self.role,
                str_out_parser=self.str_out_parser,
                data_frame=self.df,
                prompt=query_prompt
            )
            prompt_data = self.partial_prompt_obj.return_partial_prompt()
            self.logger.info("PartialPromptCreator initialized successfully")
            
            self.task_dict_obj = TaskDictionary(task_dict=tasks)
            self.logger.info("TaskDictionary object created")
            
            self.chat_history_obj = ChatHistory()
            self.logger.info("ChatHistory object created")
            
            self.update_prompt_obj = UpdatePrompt(
                llm=self.llm,
                partial_prompt=prompt_data,
                chat_hist_obj=self.chat_history_obj,
                str_out_parser=self.str_out_parser,
                task_dictionary_obj=self.task_dict_obj
            )
            self.logger.info("UpdatePrompt object created")
            
            self.run_parallel_obj = RunParallel(
                llm=self.llm,
                str_output_parser=self.str_out_parser
            )
            self.logger.info("RunParallel object created")

            # Initialize the CombinedQueryCorrector - removed cache_file parameter
            self.query_corrector = CombinedQueryCorrector(
                llm=self.llm,
                data_dict=self.data_dictionary
            )
            self.logger.info("CombinedQueryCorrector initialized successfully")

            # Initialize query processing classes
            self.query_classifier_obj = QueryClassifier(llm=self.llm)
            self.logger.info("QueryClassification object created")

            self.query_complex_breaker_obj = QueryComplexBreaker(llm=self.llm)
            self.query_breaker_obj = QueryBreaker(llm=self.llm)
            self.logger.info("QueryBreaker objects created")
            
            self.logger.info("All class instances created successfully.")
            
        except Exception as e:
            self.logger.error(f"Error creating classes: {str(e)}")
            raise

    def run_single_query(self, single_query, user_id):
        """
        Execute a single query using the query chain and update chat history
        
        Args:
            single_query (str): The query to run
            user_id: Unique identifier for the user
            
        Returns:
            The query execution output
        """
        self.logger.info(f"Running single query for user {user_id}: {single_query}")
        try:
            prompt_data_final = self.update_prompt_obj.create_query(
                query=single_query,
                user_id=user_id
            )
            pandas_query = self.run_parallel_obj.run_task(
                prompt=prompt_data_final,
                num_process=5
            )
            self.logger.info(f"Generated pandas query: {pandas_query}")
            
            # Execute the query
            df = self.df

            # out = df[df['Year'] == 2025].groupby('Month_Unique')['AllocatedAmountUSD'].sum().diff()
            # self.logger.info("before eval from finbot -  month on month change rev: ", str(out))
            
            output = eval(pandas_query)
            self.logger.info(f"Query executed successfully with output type: {type(output)}")
            
            # Update chat history
            message = f"User Query: {single_query}\n\nAI Response: {str(output)}"
            self.chat_history_obj.put_chat_history(data=message, user_id=user_id)
            self.logger.debug(f"Chat history updated for user {user_id}")
            self.logger.info(f"Generated output: {output}")
            
            return output
            #return {"query": single_query, 'output' : output}
            
        except Exception as e:
            self.logger.error(f"Error running single query: {str(e)}")
            raise

    def identify_task_from_query(self, query):
        """
        Identify task from query using task identification prompt
        
        Args:
            query (str): The user query
            
        Returns:
            tuple: (identified_tasks, task_descriptions, task_context)
        """
        self.logger.info("Identifying task from query")
        prompt_template = PromptTemplate(template=task_identification_prompt, input_variables=["query"])
        formatted_prompt = prompt_template.format(query=query)
        llm_response = self.llm.invoke(formatted_prompt)
        if hasattr(llm_response, "content"):
            response_text = llm_response.content.strip()
        else:
            response_text = str(llm_response).strip()
        # Try to parse as list
        try:
            identified_tasks = ast.literal_eval(response_text)
            if not isinstance(identified_tasks, list):
                identified_tasks = [str(identified_tasks)]
        except Exception:
            # Fallback: extract between brackets
            if '[' in response_text and ']' in response_text:
                start = response_text.find('[') + 1
                end = response_text.find(']')
                content = response_text[start:end]
                identified_tasks = [item.strip().strip("'\"") for item in content.split(',')]
            else:
                identified_tasks = [response_text.strip().strip("[]'\"")]
        self.logger.info(f"Identified tasks: {identified_tasks}")
        # Lookup task descriptions
        task_descriptions = {}
        for task in identified_tasks:
            desc = self.task_dict_obj.return_task_description(task)
            if desc:
                task_descriptions[task] = desc
                self.logger.info(f"Task '{task}' found: {desc}")
            else:
                self.logger.warning(f"Task '{task}' not found in dictionary")
        # Optionally, pass the first found task description to the corrector (if any)
        task_context = None
        if task_descriptions:
            task_context = list(task_descriptions.values())[0]
            
        return identified_tasks, task_descriptions, task_context

    def break_query_based_on_type(self, original_query, query_type):
        """
        Break query based on its type using appropriate breaker
        
        Args:
            original_query (str): The original query (not corrected yet)
            query_type (str): The type of query (SIMPLE, SIMPLE MULTIPLE, COMPLEX MULTIPLE)
            
        Returns:
            list: List of broken queries (uncorrected)
        """
        self.logger.info("Breaking query if needed")
        if query_type == "SIMPLE MULTIPLE":
            broken_queries = self.query_breaker_obj.break_query(original_query, query_type)
            # Clean up broken queries
            broken_queries = [q.strip() for q in broken_queries if q.strip() and not q.strip().lower().startswith("output:")]
            self.logger.info(f"Simple Multiple broken queries: {broken_queries}")
        elif query_type == "COMPLEX MULTIPLE":
            broken_queries = self.query_complex_breaker_obj.break_query(original_query, query_type)
            # Clean up broken queries
            broken_queries = [q.strip() for q in broken_queries if q.strip() and not q.strip().lower().startswith("output:")]
            self.logger.info(f"Complex Multiple broken queries: {broken_queries}")
        else:
            broken_queries = [original_query]
            self.logger.info("No query breaking needed for SIMPLE query")
            
        return broken_queries

    def execute_queries(self, broken_queries, user_id):
        """
        Execute all broken queries and collect results
        
        Args:
            broken_queries (list): List of queries to execute
            user_id: Unique identifier for the user
            
        Returns:
            tuple: (results, sub_queries)
        """
        self.logger.info("Executing queries")
        results = []
        sub_queries = []

        for i, sub_query in enumerate(broken_queries):
            self.logger.info(f"Executing sub-query {i+1}/{len(broken_queries)}: {sub_query}")
            result = self.run_single_query(sub_query, user_id)
            results.append(result)
            sub_queries.append(sub_query)
            
        return results, sub_queries

    def correct_broken_queries(self, broken_queries, task_context=None, verbose=True):
        """
        Apply enhanced query correction to broken queries individually
        
        Args:
            broken_queries (list): List of broken queries to correct
            task_context (str): Task context for better correction (optional)
            verbose (bool): Whether to show detailed correction steps
            
        Returns:
            list: List of corrected queries
        """
        self.logger.info("Applying query correction to broken queries")
        corrected_queries = []
        
        for i, broken_query in enumerate(broken_queries):
            self.logger.info(f"Correcting broken query {i+1}/{len(broken_queries)}: {broken_query}")
            
            # Set task context if available and supported
            if task_context and hasattr(self.query_corrector, 'set_task_context'):
                self.query_corrector.set_task_context(task_context)
            elif task_context:
                self.logger.warning("Query corrector doesn't support task context setting")
                
            # Correct the broken query
            corrected_query = self.query_corrector.correct_query(broken_query, verbose=verbose)
            corrected_queries.append(corrected_query)
            
            self.logger.info(f"Original broken query: {broken_query}")
            self.logger.info(f"Corrected query: {corrected_query}")
        
        return corrected_queries

    def run_exact_query(self, query, user_id):
        df = self.df
        output= None
        
        if query.startswith("just_pandas_query : df") :
            output = eval(query.split(":", maxsplit=1)[1].strip())
            
        if output == None :
            return None
        else: 
            return {"query": [query],
                    "output": [output] } 

    def run_query(self, query, user_id, verbose=True):
        """
        Run the full query processing pipeline with updated order:
        
        STEP 0: Task Identification - Identify the type of financial task
        STEP 1: Query Classification - Classify query complexity 
        STEP 2: Query Breaking - Break complex queries into sub-queries
        STEP 3: Enhanced Query Correction - Correct each broken query individually
        STEP 4: Query Execution - Execute all corrected sub-queries
        
        Args:
            query (str): The user query
            user_id: Unique identifier for the user
            verbose (bool): Whether to show detailed correction steps
            
        Returns:
            dict: Query processing results with corrected sub-queries and outputs
        """
        self.logger.info(f"Processing query for user {user_id}: {query}")
        try:

            results = self.run_exact_query(query, user_id)

            if results:
                return results
            
            got_results=0
            
            while got_results < 3:
                
                # STEP 0: Task Identification
                self.logger.info("STEP 0: Identifying task from query")
                identified_tasks, task_descriptions, task_context = self.identify_task_from_query(query)
            
                # STEP 1: Query Classification
                self.logger.info("STEP 1: Classifying query type")
                query_type = self.query_classifier_obj.classify_query(query)
                self.logger.info(f"Classified query as: {query_type}")
    
                # STEP 2: Query Breaking based on type
                self.logger.info("STEP 2: Breaking query if needed")
                broken_queries = self.break_query_based_on_type(query, query_type)
    
                # STEP 3: Query Correction on broken queries
                self.logger.info("STEP 3: Query Correction")
                corrected_queries = self.correct_broken_queries(broken_queries, task_context, verbose)
    
                # STEP 4: Query Execution
                self.logger.info("STEP 4: Executing corrected queries")
                results, sub_queries = self.execute_corrected_queries(corrected_queries, user_id)
                
                if results == None:
                    got_results =+ 1
                    results = 'Not Found'
                    continue
                else:
                    break
                    
            # Return comprehensive output
            final_output = {
                "query": sub_queries,
                "output": results
            } 
            
            return final_output
            
        except Exception as e:
            self.logger.error(f"Error processing query: {str(e)}")
            raise

    def get_query_correction_stats(self):
        """
        Get statistics about the query corrector
        
        Returns:
            dict: Statistics about the correction system
        """
        try:
            stats = {
                "cache_size": len(self.query_corrector.fuzzy_cache) if hasattr(self.query_corrector, 'fuzzy_cache') else 0,
                "data_dict_columns": len(self.query_corrector.data_dict) if hasattr(self.query_corrector, 'data_dict') else 0,
                "corrector_initialized": hasattr(self, 'query_corrector')
            }
            
            if hasattr(self.query_corrector, 'data_dict'):
                total_values = sum(len(values) for values in self.query_corrector.data_dict.values())
                stats["total_unique_values"] = total_values
                
            return stats
        except Exception as e:
            self.logger.error(f"Error getting correction stats: {str(e)}")
            return {"error": str(e)}

    def clear_correction_cache(self):
        """
        Clear the fuzzy matching cache
        """
        try:
            if hasattr(self.query_corrector, 'fuzzy_cache'):
                self.query_corrector.fuzzy_cache.clear()
                self.logger.info("Query correction cache cleared")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error clearing correction cache: {str(e)}")
            return False
    
    def execute_corrected_queries(self, corrected_queries, user_id):
        """
        Execute all corrected queries and collect results
        
        Args:
            corrected_queries (list): List of corrected queries to execute
            user_id: Unique identifier for the user
            
        Returns:
            tuple: (results, sub_queries)
        """
        self.logger.info("Executing corrected queries")
        results = []
        sub_queries = []

        for i, corrected_query in enumerate(corrected_queries):
            self.logger.info(f"Executing corrected sub-query {i+1}/{len(corrected_queries)}: {corrected_query}")
            result = self.run_single_query(corrected_query, user_id)
            results.append(result)
            sub_queries.append(corrected_query)
            
        return results, sub_queries
