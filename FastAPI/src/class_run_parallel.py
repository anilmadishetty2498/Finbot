from multiprocessing.dummy import Pool as ThreadPool
import queue as queue_pack
 
class RunParallel:
   
    def __init__(self, llm, str_output_parser):
        self.llm = llm
        self.queue = queue_pack.Queue()
        self.str_output_parser = str_output_parser
        self.respons_list = []
   
    def validate_response(self, response: str) -> bool:
        """Validate if a response contains meaningful information."""
       
        vd_prompt1 = f"You know pandas. If the following response represents full pandas query then return 'Found' "
        vd_prompt2 = f"otherwise return 'Not Found' Following is response \n : {str(response)}"
        validation_prompt = vd_prompt1 + vd_prompt2
       
        llm_validation = self.llm.invoke(validation_prompt)
        return 'Not Found' in llm_validation.content
   
    def ask_llm_single_proc(self, prompt):
        max_run = 3
        total_run = 0
       
        while total_run < max_run:
            response = self.llm.invoke(prompt)
            response = self.str_output_parser.invoke(response)
           
            print("I am inside ask agent")
            print("Response validation result:")
            print(self.validate_response(response))
           
            if self.validate_response(response):  # If "Not Found" - retry
                total_run += 1
                continue
            else:  # If "Found" - valid response
                self.queue.put(response)
                return
       
        # After max retries, put failure message
        self.queue.put("Result not found: No valid pandas query generated")
       
    def make_queue_empty(self):
        while True:
            try:
                self.queue.get_nowait()
            except queue_pack.Empty:
                break
               
    def run_task(self, prompt, num_process):
        self.make_queue_empty()
        self.respons_list = []
       
        # Use ThreadPool instead of Process
        pool = ThreadPool(num_process)
        pool.map(lambda _: self.ask_llm_single_proc(prompt), range(num_process))
        pool.close()
        pool.join()
       
        answer_dict = {}
       
        while not self.queue.empty():
            queue_val = self.queue.get()
            answer_dict[queue_val] = answer_dict.get(queue_val, 0) + 1
               
        if answer_dict:
            res = max(answer_dict, key=answer_dict.get)
            return res
        else:
            return "No valid result found"

# import multiprocess as mp
# import queue as queue_pack

# class RunParallel : 
    
#     def __init__(self, llm, str_output_parser) : 

#         self.llm = llm
#         self.queue = mp.Queue()
#         self.str_output_parser = str_output_parser
#         self.respons_list  = [] 
    
#     def validate_response(self, response: str) -> bool:
        
#         """Validate if a response contains meaningful information."""
        
#         vd_prompt1 = f"You know pandas. If the following response represents full pandas query then return 'Found' "
#         vd_prompt2 = f"otherwise return 'Not Found' Following is response \n : {str(response)}"
#         validation_prompt = vd_prompt1 + vd_prompt2
#         llm_validation = self.llm.invoke(validation_prompt)
#         return 'Not Found' in llm_validation.content
    
#     def ask_llm_single_proc(self, prompt, queue) :
    
#         max_run = 3
#         total_run = 0 
#         output_val = ""
        
#         while total_run < max_run : 
            
#             response =  self.llm.invoke(prompt)
#             response = self.str_output_parser.invoke(response)
            
#             print("I am inside ask agent")
#             print("Responce is #############")
#             print(self.validate_response(response))
#             if self.validate_response(response) :
#                 total_run += 1
#                 continue
#             else:
#                 # Wrap the response in a container div to ensure proper serialization
#                 self.queue.put(response)
#                 return ""

#         queue.put("Result not found :" + response)
        
#     def make_queue_empty(self):
        
#         while True:
            
#             try:
                
#                 self.queue.get_nowait()
                
#             except queue_pack.Empty:
                
#                 break
                
#     def run_task(self, prompt, num_process) : 

#         self.make_queue_empty()
#         self.respons_list = []
#         for i in range(num_process) : 

#             self.respons_list.append(mp.Process(target=self.ask_llm_single_proc,
#                                            args=(prompt, 
#                                                  self.queue)
#                                           )
#                                )
                           
#         for proc in self.respons_list : 
#             proc.start()

#         for proc in self.respons_list : 
#             proc.join()

#         answer_dict = {} 
            
#         while not self.queue.empty() : 
            
#             queue_val = self.queue.get()
#             if not answer_dict.get(queue_val) : 
#                 answer_dict[queue_val] = 1
#             else : 
#                 answer_dict[queue_val] += 1
                
#         res = max(answer_dict, key=answer_dict.get)
#         return res