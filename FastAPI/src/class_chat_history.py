class ChatHistory : 

    def __init__(self) :

        self.memory = dict()

    def put_chat_history(self, data, user_id) : 

        if self.memory.get(user_id) == None : 

            self.memory[user_id] = []
            self.memory[user_id].append(data)

        else : 

            if len(self.memory[user_id]) > 3 : 

                self.memory[user_id].pop(0)
            self.memory[user_id].append(data)
    
    def get_chat_history(self, user_id) : 

        if self.memory.get(user_id) == None : 

            return None

        else : 

            data = """
            Chat History 
            
            You must only use the following chat history to answer the query where answer depends on the answer of previous query """
            
            for chat in self.memory[user_id] : 

                data = data + "\n\n" + chat

        return data