from langchain_experimental.tools.python.tool import PythonAstREPLTool
from langchain.prompts import PromptTemplate
from langchain_core.tools.render import ToolsRenderer, render_text_description

class PartialPromptCreator : 
    
    """
    Class PromptCreator will perform the following 

    - The PromptTemplate object will be created 
    - Create tool names 
    - 
    """
    def __init__(self, 
                 role,
                 str_out_parser,
                 data_frame,
                 prompt
                ) : 

        self.prompt = prompt
        self.str_out_parser = str_out_parser
        self.role = role
        self.data_frame = data_frame

    
    def create_query_prompt(self):

        prompt_data = (PromptTemplate
                       .from_template(template = self.prompt))
        
        return prompt_data 

    def create_tool(self) : 

        df_locals = {}
        df_locals["df"] = self.data_frame
        tools = [PythonAstREPLTool(locals=df_locals)]
        rendereds_tools = render_text_description(list(tools))
        return tools, rendereds_tools

    
    def create_tool_names(self, tools) : 

        tool_names = ", ".join([t.name for t in tools])
        return tool_names
        
    
    def create_partial_prompt(self,
                                    prompt =  None, 
                                    tools = None,
                                    tool_names = None,
                                    role= "Principal data scientist"
                                   ): 

        prompt_data = prompt.partial(
                                     tools = tools,
                                     tool_names = tool_names,
                                     role = role,
                                     df = self.data_frame
        )
        
        return prompt_data

    def return_partial_prompt(self) : 

        prompt_data = self.create_query_prompt()
        tools, rendereds_tools = self.create_tool()
        tool_names = self.create_tool_names(tools)
        prompt_data = self.create_partial_prompt(
                                                  prompt = prompt_data, 
                                                  tools = tools,
                                                  tool_names = tool_names,
                                                  role = self.role
        )
        return prompt_data
