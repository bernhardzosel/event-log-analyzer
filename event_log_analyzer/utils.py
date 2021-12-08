"""
This module contains various functions that are needed in our tool, e.g. initialization of logging. 
"""
import functools
import os
import time
import logging
from pathlib import Path

#Logging for Monitoring Program Flow
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(levelname)s:%(asctime)s:%(message)s", datefmt="%Y-%m-%d %H:%M:%S")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


#Logging for Giving Information about Patterns that apply or where they are violated
pattern_logger = logging.getLogger("pattern_logger")
pattern_logger.setLevel(logging.INFO)

pattern_formatter = logging.Formatter("%(message)s")

Path(f'{os.getcwd()}/output').mkdir(parents=True, exist_ok=True)
pattern_file_handler = logging.FileHandler(f'{os.getcwd()}/output/patterns.log', mode='w')
pattern_file_handler.setLevel(logging.INFO)
pattern_file_handler.setFormatter(pattern_formatter)

pattern_logger.addHandler(pattern_file_handler)

def log_time(logger, text):
    """this function can be used as a decorator, which writes the time needed for the decorated function together with a text to the specified logger


    Parameters
    ----------
    logger : Logger
        the logger in which we want to write the information
    text : str
        a text printed together with the time in seconds that the decorated method took to execute
    """
    @functools.wraps(logger)
    def timer(func):
        @functools.wraps(func)
        def wrapper_timer(*args, **kwargs):
            start = time.perf_counter()
            value = func(*args, **kwargs)
            elapsed_time = time.perf_counter() - start
            logger.info(f"{text}: {elapsed_time:0.4f} seconds")
            return value
        return wrapper_timer
    return timer


                
    
