"""
This module contains thr functionality to import new event logs.
"""
import json
import pandas as pd
import pathlib
from event_log_analyzer.event_log import EventLogStorage, StorageType
from event_log_analyzer.adapter import ActivityInstanceAdder, ColumnRenamer, IntervalToEventLogTransformer, Sorter, TimestampModifier, TimestampRenamer, RowIDAdder
from event_log_analyzer.validate import validate_config
from event_log_analyzer.utils import log_time, logger
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.conversion.log import converter as log_converter


@log_time(logger, "import duration")
def import_event_log(config_file, storage_type=StorageType.COLUMN_BASED_AT_ONCE):
    """
    Import and validate an event log into the internal representation of an EventLogStorage object
    
    Parameters
    -----------
    config_file
        the path to a JSON configuration file
        
    storage_type: StorageType
        the storage type of the database where we want to import the log, default StorageType.COLUMN_BASED_AT_ONCE
    
    Returns
    -----------
    log
        EventLogStorage object
    """          
    logger.info(f"import event log") 
 
    with open(config_file) as json_config_file:
        config = json.load(json_config_file)  
        validate_config(f"{pathlib.Path(__file__).parent}/config_format.schema.json", config)   
            
            
    #specific input format to dataframe    
    if config["path"].endswith(".csv"):
        raw_df = import_csv_file(config)
    elif config["path"].endswith(".xes"):
        raw_df = import_xes_file(config)
    else:
        raise ValueError("the imported file has the wrong format (currently only .csv possible)!")
    
        
    #transform all event log formats to interval log
    adapters = [RowIDAdder(), ColumnRenamer(), TimestampRenamer(), IntervalToEventLogTransformer(), ActivityInstanceAdder(), Sorter()]
    df = raw_df
    for adapter in adapters:
        df = adapter.transform(config, df)
    dataframe = df
    
    event_log_storage = EventLogStorage(config, storage_type)
    event_log_storage.add_new_dataframe(dataframe)
    return event_log_storage

@log_time(logger, "extracting dataframe from xes file")
def import_xes_file(config):
    """
    Import the event log from the file format into a pandas dataframe with timestamps
    
    Parameters
    -----------
    config_file
        the path to a JSON configuration file
    
    Returns
    -----------
    raw_dataframe
        the pandas dataframe without any modification
    """
    event_log = xes_importer.apply(config["path"])
    raw_df = log_converter.apply(event_log, variant=log_converter.Variants.TO_DATA_FRAME)

    return raw_df
    
@log_time(logger, "extracting dataframe from csv file")
def import_csv_file(config):    
    """
    Import the event log from the file format into a pandas dataframe
    
    Parameters
    -----------
    config_file
        the path to a JSON configuration file
    
    Returns
    -----------
    raw_dataframe
        the pandas dataframe without any modification (only the string timestamps are converted to real timestamps)
    """
    df = pd.read_csv(config["path"], sep=config["separator"])
    df = TimestampModifier().transform(config, df)

    return df 