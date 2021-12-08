"""
This module contains all functionality related to the adaption process to transform new event logs in a unified format
"""
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import interval_lifecycle
from pm4py.util import constants
from event_log_analyzer.utils import log_time, logger


class Adapter(ABC):  
    """
    Adapter interface which every adapter needs to implement
    """  
    @abstractmethod
    def transform(self, cfg, df):
        """transforms the given dataframe and returns it

        Parameters
        ----------
        cfg : json
            the configuration file
        df : pandas.DataFrame
            the event log as a data frame that should be transformed
            
        Returns
        -------
        pandas.DataFrame
            returns the transformed data frame
        """
        pass
    
class TimestampModifier(Adapter):
    """
    transforms all timestamps, indicated in the config file, to a common format (must be done before timestamps are renamed)
    """
    def get_sec(self, time_str):
        """converts a timestamp in HH:mm:ss format to seconds

        Parameters
        ----------
        time_str : str
            a string in the following format HH:mm:ss, where the hours are continuous, i.e. this timestamp can be considered as duration

        Returns
        -------
        int
            duration in seconds
        """
        h, m, s = time_str.split(':')
        assert int(h)>=0 and int(m)>=0 and int(m)<=60 and int(s)>=0 and int(s)<=60, "Time Format is wrong"
        
        return int(h) * 3600 + int(m) * 60 + int(s) 
    
    @log_time(logger, "modify timestamps")
    def transform(self, cfg, df):
        for attr in cfg["time_attributes"]:
            if cfg["relative_time"]:
                df[attr] = df[attr].apply(lambda x: datetime(1970, 1, 1, 0, 0, 0, 0) + timedelta(seconds=self.get_sec(x)))
            else:
                try:
                    df[attr] = df[attr].apply(lambda x: datetime.strptime(x, cfg["time_format"]))
                except ValueError:
                        raise ValueError(f"""Incorrect timestamp format, should be {cfg["time_format"]}""")
            df[attr] = pd.to_datetime(df[attr])

        return df
    
class RowIDAdder(Adapter):
    """
    maps a given row id to the Row_ID attribute, if nothing is specified a new row column id is added to the log
    """
    @log_time(logger, "add row id")
    def transform(self, cfg, df):
        if "row_id_column" not in cfg or cfg["row_id_column"] == None:
            df['Row_ID'] = range(0, len(df.index))
        else:
            df = df.rename(columns={cfg["row_id_column"]: "Row_ID"})
        return df

class ColumnRenamer(Adapter):
    """
    maps all attributes names as specified in the config file (this adapter should be applied after all other adapters that need the original attribute names have been applied)
    """
    @log_time(logger, "rename columns")
    def transform(self, cfg, df):
        if 'Job' not in df.columns:
            if "job_column" in cfg and cfg["job_column"] != None:
                df = df.rename(columns={cfg["job_column"]: "Job"})
            
        if 'Machine' not in df.columns:
            if "machine_column" in cfg and cfg["machine_column"] != None:
                df = df.rename(columns={cfg["machine_column"]: "Machine"})
                
        if 'Transaction_Type' not in df.columns:
            if "transaction_type_column" in cfg and cfg["transaction_type_column"] != None:
                df = df.rename(columns={cfg["transaction_type_column"]: "Transaction_Type"})  

        if 'Resource' not in df.columns:
            if "resource_columns" in cfg and cfg["resource_columns"] != None and len(cfg["resource_columns"])>0:
                df = df.rename(columns={cfg["resource_columns"][0]: "Resource"})  
        
        return df

class TimestampRenamer(Adapter):
    """
    maps all timestamp attribute names as specified in the config file (in atomic event log, we map them to 'Timestamp'; in interval log, we map them to 'Start' and 'Complete' timestamps)
    """
    @log_time(logger, "rename timestamps")
    def transform(self, cfg, df):
        if cfg["event_log_format"] == "atomic":
            assert len(cfg["time_attributes"])==1, "in an event format only one timestamp is allowed" 
            df = df.rename(columns={cfg["time_attributes"][0]: "Timestamp"})
        elif cfg["event_log_format"] == "interval":
            assert len(cfg["time_attributes"])==2, "in an event format only two timestamps are allowed"
            start_attribute = cfg["time_attributes"][0]
            complete_attribute = cfg["time_attributes"][1]
            df = df.rename(columns={start_attribute: "Start", complete_attribute: "Complete"})
        return df
            
class IntervalToEventLogTransformer(Adapter):
    """
    if a log is given in interval format, this adapter converts it to atomic event log format (two timestamps 'Start' and 'Complete' are required -> TimestampRenamer needs to be applied before)
    """
    @log_time(logger, "interval to event log")
    def transform(self, cfg, df):
        if cfg["event_log_format"] == 'interval':  
            df['Activity_Instance'] = range(0, len(df.index))   #!!! no need to use the activity instance adder anymore
            logger.info(f"log in interval format --> transform to atomic event log")
            parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'Job'}
            interval_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
                
            event_log = interval_lifecycle.to_lifecycle(interval_log, parameters={
            constants.PARAMETER_CONSTANT_START_TIMESTAMP_KEY: "Start",
            constants.PARAMETER_CONSTANT_TIMESTAMP_KEY: "Complete",
            constants.PARAMETER_CONSTANT_ACTIVITY_KEY: "Machine",
            constants.PARAMETER_CONSTANT_TRANSITION_KEY: "Transaction_Type"})
            
            df = log_converter.apply(event_log, variant=log_converter.Variants.TO_DATA_FRAME)
            df = df.rename(columns={"Complete": "Timestamp"})
            df = df.drop(df.filter(regex='@@').columns, axis=1)    
            return df
        else:
            return df

class ActivityInstanceAdder(Adapter):
    """
    adds an activity instance, if not already present (only applicable to logs in atomic format with 'Timestamp' attribute -> TimestampRenamer needs to be applied before)
    """
    @log_time(logger, "adding activity instances manually (find corresponding events that belong to the same operation)")
    def transform(self, cfg, df):
        if "Activity_Instance" in df.columns:        #activity instance already existing, no need to create it
            return df
        elif "activity_instance_column" in cfg and cfg["activity_instance_column"] != None:
            return df.rename(columns={cfg["activity_instance_column"]: "Activity_Instance"})
        else:
            log_grouped_by_attr = df.groupby(cfg["group_attributes"], dropna=False)
                    
            df_list = []
            i = 0
            for group in log_grouped_by_attr:
                group_events = group[1].sort_values("Timestamp")
                group_events["Activity_Instance"] = i
                i = i + 1
                df_list.append(group_events)
                if "start" not in group_events.values or "complete" not in group_events.values:
                    raise ValueError("The event log contains operations that do not have start and complete events")
            
            merged_df = pd.concat(df_list) 
            return merged_df

class Sorter(Adapter):
    """
    sorts the event log if this is not already done by default
    """
    @log_time(logger, "sort the log")
    def transform(self, cfg, df):
        if "sorted" in cfg and cfg["sorted"] == True and cfg["event_log_format"] == 'atomic':
            df = df.sort_values(["Row_ID"], ignore_index=True)  
            # since other adapters might change the order if the event log was already sorted during input (sorted=True),
            # we sort again according to the Row_ID, that was given in the beginning of the adaption steps
            return df
        else:
            df = df.sort_values(["Timestamp","Job","Machine"], ignore_index=True)
            return df

class EventToIntervalLog(Adapter):
    """
    if a log is given in atomic format, this adapter converts it to interval event log format (only applicable if log contains 'Job', 'Machine', 'Timestamp' and 'Transaction_Type' attributes)
    """
    @log_time(logger, "event to interval log")
    def transform(self, cfg, df):
        parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'Job'}
        event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

        interval_log = interval_lifecycle.to_interval(event_log, parameters={
        constants.PARAMETER_CONSTANT_TIMESTAMP_KEY: "Timestamp",
        constants.PARAMETER_CONSTANT_ACTIVITY_KEY: "Machine",
        constants.PARAMETER_CONSTANT_TRANSITION_KEY: "Transaction_Type"})

        df = log_converter.apply(interval_log, variant=log_converter.Variants.TO_DATA_FRAME)
        df = df.rename(columns={"start_timestamp": "Start", "Timestamp": "Complete"})
        df = df.drop(df.filter(regex='@@startevent_').columns, axis=1)
        return df
    
    def transform_without_pm4py(self, cfg, df):
        """
        this method shows how the transform() method could be implemented without pm4py
        """
        group_attr = cfg["group_attributes"]
        order_attr = "Timestamp"
        transaction_type = cfg["transaction_type_column"]
        assert len(cfg["time_attributes"])==1, "in an activity format only one timestamp is allowed"
        time_attr = cfg["time_attributes"][0]
        start_transaction_types = cfg["start_transaction_types"]
        complete_transaction_types = cfg["complete_transaction_types"]
        frames = []
        
        grouped_dataframe = df.groupby(group_attr)
        for g in grouped_dataframe:
            df = g[1]
            #Filter out events that are not part of start- and complete-transaction-types (like schedule)
            filtered_df = g[1][(g[1]["Transaction_Type"].isin(start_transaction_types)) | (g[1]["Transaction_Type"].isin(complete_transaction_types))]

            df_events = filtered_df.sort_values(order_attr)
            assert len(df_events) == 2, "start and complete transaction types do not match"
            assert df_events.iloc[0][transaction_type] in start_transaction_types, "wrong start transaction type"
            assert df_events.iloc[1][transaction_type] in complete_transaction_types, "wrong complete transaction type"
            
            df_activities = df_events[(df_events[transaction_type] == "start")].drop(columns=transaction_type)#.rename(columns={order_attribute: "Start"})
            df_activities = df_activities.assign(Complete=df_events.iloc[1][order_attr])
                    
            frames.append(df_activities)
        
        result = pd.concat(frames)
        
        result = result.rename(columns={time_attr: "Start"})
        
        return result