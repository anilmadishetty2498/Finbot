import os
import logging
from logging import Logger
from logging import Formatter
from datetime import datetime

class LoggingTool : 
    
    """
    class LoggingTool provide functionalities for logging
    1. It can log on console 
    2. It can log into a file 
    3. It can log on console and log into a file
    """
    
    def __init__(self,filename : str = None, 
                 is_console : bool = True, 
                 logger_name : str = "finetunig")-> None: 
        
        """
        Initialization method for class LoggingTool

        Parameters : 

        filename : str, Default value is None. it is file where logger will log into. 
        is_streaming : bool, Default value is True. It will decide if logger has to log on console or not
        logger_name : str, logger name used in logger creation

        Output : 

        None
        """
        
        self.file_name = filename
        self.today_date = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
        self.logging = logging
        self.is_console = is_console
        self.logger_name = logger_name
        
        
    def create_logger(self) -> Logger:
        """
        This method will create a logger object 

        Paramaters : 
        
        Method have no paramaters 

        Output : 

        Object of Logger class
        """
        
        logger = logging.getLogger(self.logger_name)
        self.logging.basicConfig(level = self.logging.INFO)
        logger.today_date = self.today_date
        return logger

    def create_formatter(self)-> Formatter:
        
        """
        This method will create a Formatter object 

        Paramaters : 
        
        Method have no paramaters 

        Output : 

        Object of Formatter class
        """
        
        formatter = self.logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S')
        return formatter

    def set_console_logger(self, logger : Logger, 
                             formatter : Formatter) -> Logger :
        
        """
        This method will set console logging

        Paramaters : 
        
        logger : Object of Logger 
        formatter : Object of Formatter

        Output : 

        Object of Logger class
        """
        
        stream = self.logging.StreamHandler()
        stream.setFormatter(formatter)
        logger.addHandler(stream)
        logger.handlers = [logger.handlers[0]].copy()
        return logger

    def set_file_logger(self, logger : Logger, 
                             formatter : Formatter) -> Logger :
        
        """
        This method will set file logging

        Paramaters : 
        
        logger : Object of Logger 
        formatter : Object of Formatter

        Output : 

        Object of Logger class
        """
        
        if not os.path.isdir("logs"):

            os.makedirs("logs")
            
        #log_file_name =  "logs/"+ self.file_name + "log_" + self.today_date 
        log_file_name = f"logs/{self.file_name}_log_{self.today_date}.txt"

        file = self.logging.FileHandler(filename = log_file_name)
        file.setFormatter(formatter)
        logger.addHandler(file)
        logger.log_file_name = log_file_name
        print("logger.log_file_name : ",logger.log_file_name)
        return logger

    def create_and_set_logger(self) -> Logger :
        
        """
        Create Logger object and set it for console and file

        Steps : 

        1. Create logger object 
        2. Create formatter object
        3. Given on condition set only console logger
        4. Given on condition set only file logger
        5. Give on condition set console and file logger

        Paramateres  : 

        This method has no parameters 

        Output : 

        Object of Logger class
        
        """
        
        logger = self.create_logger()
        formatter = self.create_formatter()
        
        
        if self.is_console and not self.file_name :

            logger = self.set_console_logger(logger = logger, 
                                               formatter = formatter)
        elif not self.is_console and self.file_name :

            logger = self.set_file_logger(logger = logger, 
                                               formatter = formatter)
            
        elif self.is_console and self.file_name :

            logger = self.set_console_logger(logger = logger, 
                                               formatter = formatter)
            logger = self.set_file_logger(logger = logger, 
                                               formatter = formatter)

        logger.propagate = False   
        logger.info("Logger Started") 
        if self.file_name : 
            
            logger.info(" We can get logfile at : "+ logger.log_file_name)
        return logger
