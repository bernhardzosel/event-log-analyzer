"""
This module is responsible for validating event logs before storing them in the database as well validating the config files. 
"""
import pandas as pd
import json
import jsonschema

def validate(event_log):   
    """
    checks whether the log is sorted by the timestamp column
    
    Parameters
    -----------
    event_log: DataFrame
        the dataframe of the event log in atomic event log format ('Timestamp' column required)
    
    Raises
        ------
        ValueError
            if the timestamps are incorrect, i.e. no timestamp column is present or if the log is not sorted by this timestamp
    """       
    if 'Timestamp' not in event_log.columns:
        raise ValueError(f"a Timestamp attribute is needed")
    
    #check whether timestamps are sorted
    if not pd.Index(event_log["Timestamp"]).is_monotonic:
        raise ValueError(f"The event log in not sorted by timestamps")
        
def validate_config(json_schema_path, config_file):
    """validates the given config_file, whether it is valid according to the given schema

    Parameters
    ----------
    json_schema : str
        the path to the JSON schema, where the config file format has been specified
    config_file : dict
        the JSON object that was parsed
        
    Raises
    ----------
    ValueError if the config file is invalid
    """
    with open(json_schema_path) as json_schema:
        schema = json.load(json_schema)   
        
    try:
        jsonschema.validate(config_file, schema)
    except:   
        raise ValueError("the config file is invalid!")
        
    