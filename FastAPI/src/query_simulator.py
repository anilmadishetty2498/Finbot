"""
Query Simulator module to generate variations of user queries while preserving their meaning.
This is useful for training LLMs to understand that different phrasings of the same question
should yield similar answers.
"""

import random
import re
from typing import List, Dict, Any, Optional
from config.data_dictionary import get_data_dictionary, get_sample_data
from utils.getllm import get_llm
from langchain.prompts import PromptTemplate

class QuerySimulator:
    def __init__(self, custom_dictionary=None, custom_samples=None, num_variations=20):
        """
        Initialize the QuerySimulator class for generating query variations
        
        Args:
            custom_dictionary (dict, optional): Custom dictionary to use instead of the default
            custom_samples (dict, optional): Custom sample data to use instead of the default
            num_variations (int): Number of variations to generate per query (default: 20)
        """
        self.llm = get_llm()
        
        # Use provided dictionaries or get from data_dictionary module
        self.data_dictionary = custom_dictionary if custom_dictionary is not None else get_data_dictionary()
        self.sample_data = custom_samples if custom_samples is not None else get_sample_data()
        self.num_variations = num_variations
        
        # Create formatted string representations for the prompt
        self.data_dict_str = self._format_data_dictionary()
        self.sample_data_str = self._format_sample_data()
        
        # Set up prompt templates
        self.variation_prompt = self._create_variation_prompt()
        self.entity_extraction_prompt = self._create_entity_extraction_prompt()
        
        # Create runnable chains
        self.variation_chain = self.variation_prompt | self.llm
        self.entity_extraction_chain = self.entity_extraction_prompt | self.llm
    
    def _format_data_dictionary(self) -> str:
        """Format the data dictionary for inclusion in the prompt"""
        formatted_dict = []
        for column, description in self.data_dictionary.items():
            formatted_dict.append(f"{column}: {description}")
        return "\n".join(formatted_dict)
    
    def _format_sample_data(self) -> str:
        """Format the sample data for inclusion in the prompt"""
        if not self.sample_data:
            return ""
        
        formatted_data = ["Sample values for columns:"]
        for column, values in self.sample_data.items():
            if column in self.data_dictionary:  # Only include if column is in dictionary
                formatted_data.append(f"{column}: {values}")
        
        return "\n".join(formatted_data) if len(formatted_data) > 1 else ""
    
    def _create_variation_prompt(self) -> PromptTemplate:
        """Create the prompt template for generating query variations"""
        template = """
You are an AI assistant specializing in generating variations of logistics data analysis queries while maintaining their original intent.

DATA DICTIONARY:
{data_dictionary}

SAMPLE DATA:
{sample_data}

ORIGINAL QUERY: {query}

EXTRACTED ENTITIES: {entities}

QUERY TYPE: {query_type}

TASK:
Generate {num_variations} unique variations of the original query while following these rules:
1. Preserve the core meaning and intent of the original query
2. Maintain all specific entity references (e.g., specific region names, customer names, metrics)
3. Vary the phrasing, word order, vocabulary, and sentence structure
4. Use different question forms (e.g., "What is" vs "Show me" vs "I need to know")
5. Add or remove minor details that don't change the core request
6. Include some variations with different levels of formality or verbosity
7. For drill-down queries, preserve the specific drill-down entity but vary how it's referenced
8. Ensure all variations would produce the same analytical result as the original query

FORMAT YOUR RESPONSE AS A NUMBERED LIST:
1. [First variation]
2. [Second variation]
...
{num_variations}. [Last variation]

Do not include explanations or additional text. Return ONLY the numbered variations.
"""
        return PromptTemplate(
            input_variables=["data_dictionary", "sample_data", "query", "entities", "query_type", "num_variations"],
            template=template
        )
    
    def _create_entity_extraction_prompt(self) -> PromptTemplate:
        """Create the prompt template for extracting entities from a query"""
        template = """
You are an AI assistant specializing in extracting specific entities from logistics data analysis queries.

DATA DICTIONARY:
{data_dictionary}

SAMPLE DATA:
{sample_data}

ORIGINAL QUERY: {query}

TASK:
Extract and identify all specific entities mentioned in the query. Entities include:
1. Specific geographical locations (regions, areas, countries, cities)
2. Specific customer names
3. Specific product types or codes
4. Specific metrics being requested (volume, revenue, yield)
5. Specific time periods mentioned (last week, current month, etc.)
6. Specific comparison terms (against Rofo, Month on Month, Year over Year)
7. Any other specific named entities that are crucial to the query's meaning

FORMAT YOUR RESPONSE AS A JSON OBJECT with entity types as keys and the specific entities as values:
{{
  "locations": ["Asia Pacific Region", "Europe Region"],
  "customers": ["Customer XYZ"],
  "products": ["Spot", "Contract"],
  "metrics": ["Revenue", "Volume"],
  "time_periods": ["last week"],
  "comparisons": ["Month on Month"],
  "other": ["any other specific entities"]
}}

If a category has no entities, include it with an empty array.
Parse only EXPLICIT entities from the query, not implied ones.
"""
        return PromptTemplate(
            input_variables=["data_dictionary", "sample_data", "query"],
            template=template
        )
    
    def _determine_query_type(self, query: str) -> str:
        """
        Determine if a query is a simple question or a drill-down question
        
        Args:
            query (str): The query to analyze
            
        Returns:
            str: 'SIMPLE' or 'DRILL_DOWN'
        """
        # Keywords that might indicate a drill-down query
        drill_down_indicators = [
            r"\b(region|area|country|city|trade|customer)\b",
            r"\btop\s+\d+\b",
            r"\bworst\s+\d+\b",
            r"\bbest\s+\d+\b",
            r"\bby\s+(region|area|country|city|trade|customer)\b"
        ]
        
        # Check if any drill-down indicators are present
        for pattern in drill_down_indicators:
            if re.search(pattern, query, re.IGNORECASE):
                return "DRILL_DOWN"
        
        return "SIMPLE"
    
    def extract_entities(self, query: str) -> Dict[str, List[str]]:
        """
        Extract specific entities from a query
        
        Args:
            query (str): The query to extract entities from
            
        Returns:
            dict: Dictionary of extracted entities by category
        """
        if not self.llm:
            print("Error: LLM is not initialized")
            return {
                "locations": [], "customers": [], "products": [],
                "metrics": [], "time_periods": [], "comparisons": [], "other": []
            }
        
        try:
            result = self.entity_extraction_chain.invoke({
                "data_dictionary": self.data_dict_str,
                "sample_data": self.sample_data_str,
                "query": query
            })
            
            # Extract the result text
            if hasattr(result, "content"):
                entities_text = result.content
            elif isinstance(result, dict) and "text" in result:
                entities_text = result["text"]
            elif isinstance(result, str):
                entities_text = result
            else:
                entities_text = str(result)
            
            # Parse the JSON response
            # Find the JSON object in the text (it might be surrounded by other text)
            import json
            import re
            
            # Try to find a JSON object in the text
            json_match = re.search(r'({[\s\S]*})', entities_text)
            if json_match:
                try:
                    entities = json.loads(json_match.group(1))
                    # Ensure all expected keys are present
                    for key in ["locations", "customers", "products", "metrics", "time_periods", "comparisons", "other"]:
                        if key not in entities:
                            entities[key] = []
                    return entities
                except json.JSONDecodeError:
                    print("Error parsing JSON from LLM response")
            
            # If JSON parsing fails, return empty structure
            return {
                "locations": [], "customers": [], "products": [],
                "metrics": [], "time_periods": [], "comparisons": [], "other": []
            }
            
        except Exception as e:
            print(f"Error extracting entities: {e}")
            return {
                "locations": [], "customers": [], "products": [],
                "metrics": [], "time_periods": [], "comparisons": [], "other": []
            }
    
    def _format_entities_for_prompt(self, entities: Dict[str, List[str]]) -> str:
        """
        Format the extracted entities for inclusion in the prompt
        
        Args:
            entities (dict): Dictionary of extracted entities by category
            
        Returns:
            str: Formatted entities string
        """
        formatted = []
        for category, values in entities.items():
            if values:
                formatted.append(f"{category.capitalize()}: {', '.join(values)}")
        
        if not formatted:
            return "No specific entities identified."
        
        return "\n".join(formatted)
    
    def generate_variations(self, query: str) -> List[str]:
        """
        Generate variations of a user query while preserving its meaning
        
        Args:
            query (str): The original user query
            
        Returns:
            list: List of query variations
        """
        if not self.llm:
            print("Error: LLM is not initialized")
            return [query]
        
        try:
            # Extract entities from the query
            entities = self.extract_entities(query)
            formatted_entities = self._format_entities_for_prompt(entities)
            
            # Determine query type
            query_type = self._determine_query_type(query)
            
            # Generate variations
            result = self.variation_chain.invoke({
                "data_dictionary": self.data_dict_str,
                "sample_data": self.sample_data_str,
                "query": query,
                "entities": formatted_entities,
                "query_type": query_type,
                "num_variations": self.num_variations
            })
            
            # Extract the result text
            if hasattr(result, "content"):
                variations_text = result.content
            elif isinstance(result, dict) and "text" in result:
                variations_text = result["text"]
            elif isinstance(result, str):
                variations_text = result
            else:
                variations_text = str(result)
            
            # Parse the numbered list
            variations = []
            for line in variations_text.strip().split('\n'):
                # Extract the variation text after the number and period
                match = re.match(r'^\s*\d+\.\s*(.*)', line)
                if match:
                    variations.append(match.group(1).strip())
            
            # If we couldn't parse any variations, return the original query
            if not variations:
                return [query]
            
            return variations
            
        except Exception as e:
            print(f"Error generating variations: {e}")
            return [query]
    
    def batch_generate_variations(self, queries: List[str]) -> Dict[str, List[str]]:
        """
        Generate variations for multiple queries
        
        Args:
            queries (list): List of original user queries
            
        Returns:
            dict: Dictionary mapping original queries to their variations
        """
        results = {}
        for query in queries:
            results[query] = self.generate_variations(query)
        return results