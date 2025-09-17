class TaskDictionary :

    """
    Class TaskDictionary will perform following tasks

    - 
    
    """

    def __init__(self, task_dict) : 

        self.task_dictionary = task_dict

    def put_task(self, key, description) : 

        self.task_dictionary[key] = description

    def put_dict(self, task_dict) : 

        self.task_dictionary = task_dict

    def return_task_description(self, task) : 

        return self.task_dictionary.get(task,None)