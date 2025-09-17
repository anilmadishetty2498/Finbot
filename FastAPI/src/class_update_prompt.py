from langchain_experimental.tools.python.tool import PythonAstREPLTool
from langchain.prompts import PromptTemplate
from langchain_core.tools.render import ToolsRenderer, render_text_description
from src.all_prompts import query_prompt, task_identification_prompt

class UpdatePrompt :
    """
    Class CreateQuery will perform the following task 

    - 
    """
    def __init__(self,
                 llm,
                 partial_prompt,
                 chat_hist_obj,
                 str_out_parser,
                 task_dictionary_obj
                ):
        self.llm = llm
        self.partial_prompt = partial_prompt
        self.chat_hist_obj = chat_hist_obj
        self.str_out_parser = str_out_parser
        self.task_dictionary_obj = task_dictionary_obj
        
    def get_chat_history(self, user_id) : 

        return self.chat_hist_obj.get_chat_history(user_id)
        
    def add_chat_history(self, prompt, history) : 

        prompt_data = prompt.partial(
            chat_history = history
        )
        return prompt_data

    def identify_tasks(self, query, prompt) : 
    
        prompt_data = (PromptTemplate.from_template(template = prompt))
        prompt = prompt_data.format(query = query)
        task = self.llm.invoke(prompt)
        task = self.str_out_parser.invoke(task)
        return eval(task)


    def add_tasks (self, tasks, prompt_data) : 

        if type(tasks) == list : 

            tasks_str = ""

            for task in tasks : 
                
                task_description = self.task_dictionary_obj.return_task_description(task = task)
                if task_description : 
                    tasks_str = tasks_str + '\n' + task_description

        elif type(tasks) == str : 

            task_description = self.task_dictionary_obj.return_task_description(task = task)

        
        prompt_data = prompt_data.partial(
                            how_to_task = tasks_str
        )

        return prompt_data

    def add_query(self, 
                      prompt_data,
                      query):

        prompt_data = prompt_data.format(
                            query = query
        )

        return prompt_data

    def create_query(self, query, user_id) : 

        chat_history  = self.get_chat_history(user_id = user_id)
        prompt_data = self.add_chat_history(prompt = self.partial_prompt, 
                                            history =  chat_history
        )
        tasks_in_prompt = self.identify_tasks(query = query,
                                              prompt = task_identification_prompt
        )
        prompt_data = self.add_tasks(tasks = tasks_in_prompt, 
                                     prompt_data = prompt_data
        )
        prompt_data = self.add_query(query = query,
                                         prompt_data = prompt_data)
        return prompt_data