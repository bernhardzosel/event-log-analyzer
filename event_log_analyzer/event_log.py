"""
This module is responsible for storing and managing the event log data. 
"""
import os
import pandas as pd
import sqlite3
import duckdb
from event_log_analyzer.validate import validate
from event_log_analyzer.adapter import EventToIntervalLog
from event_log_analyzer.utils import log_time, logger
from enum import Enum

DATABASE_NAME = 'event_log'
INTERVAL_DATABASE_NAME = 'interval_log'

class StorageType(Enum):
    """
    The storage types specify the data layout that is used internally to store the event logs.
    """
    COLUMN_BASED=1
    ROW_BASED=2
    COLUMN_BASED_AT_ONCE=3

class EventLogStorage:
    """
    The EventLogStorage object stores all information about a given event log. To itialize the object, the dataframe that stores the event log already needs to be in a correct format, adaption needs to be done before!
    
    Attributes
    -----------
    _con : Connection
        the database connection
    
    _dataframe
        the actual event log data
        
    _config : json
        the configuration file
        
    _storage_type : StorageType
        specifies the data layout used in the event log storage
    """
    
    def __init__(self, cfg, storage_type=StorageType.ROW_BASED):
        """
        Parameters
        ----------
        cfg : json
            the database connection
        storage_type : StorageType, optional
            specifies the data layout used in the event log storage, by default StorageType.ROW_BASED
        """
        self._config = cfg
        
        self._storage_type = storage_type 
        if self._storage_type == StorageType.ROW_BASED:
            logger.info("Connect with SQLite Database")
            self._con = sqlite3.connect(f'{os.getcwd()}/event_log_storage_sqlite.db')
        elif self._storage_type == StorageType.COLUMN_BASED or self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            logger.info("Connect with DuckDB Database")
            self._con = duckdb.connect(f'{os.getcwd()}/event_log_storage_duck.db')

    
    @log_time(logger, "Storing dataframe into database")
    def add_new_dataframe(self, new_df):
        """
        validates the log and then adds it into the EventLogStorage database and the database is optimized (e.g. by creating indexes)
        
        Parameters
        -----------
        new_log : pandas.DataFrame
            the event log needs to be in the goal format, i.e. in atomic event log form with one timestamp "Timestamp" in a pandas timestamp format
        """
        validate(new_df)
                
        if self._storage_type == StorageType.ROW_BASED:
            new_df.to_sql(DATABASE_NAME, self._con, if_exists='replace', index=False)
            cursor = self._con.cursor()
            cursor.execute(f"CREATE INDEX job_index ON {DATABASE_NAME} (Job)")
            cursor.execute(f"CREATE INDEX machine_index ON {DATABASE_NAME} (Machine)")
        elif self._storage_type == StorageType.COLUMN_BASED or self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            self._con.execute(f"DROP TABLE IF EXISTS {DATABASE_NAME}")
            self._con.from_df(new_df).create(DATABASE_NAME)

    def get_event_log(self):
        """
        loads the event log from the database
        
        Returns
        -----------
        dataframe : pandas.DataFrame
            the whole event log from the data base as a data frame
        """
        query = f"SELECT * FROM {DATABASE_NAME}"
        
        if self._storage_type == StorageType.ROW_BASED:
            df = pd.read_sql_query(query, self._con)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%Y-%m-%d %H:%M:%S")
            return df
        elif self._storage_type == StorageType.COLUMN_BASED or self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            df = self._con.execute(query).fetchdf()
            return df 
    
    def get_sequence(self, attr, needed_columns=[]):  #currently not used
        """
        groups the event log database and returns a generator object of the sequence of the given attribute as a generator object
        
        Parameters
        -----------
        attr : str
            the attribute by which the event log should be grouped
            
        needed_columns : List[str], optional
            a list of attributes/columns that need to be accessed, by default [] (the empty list stands for all attributes)
             
        Yields
        ------
        pandas.DataFrame
            The next sequence as a dataframe.
        """
        if self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            for key, group in self.get_event_log().groupby([attr]):
                yield group
        
        else:
            query = f"""SELECT {attr}
            FROM {DATABASE_NAME}
            GROUP BY {attr}
            """
            df_with_the_attr = pd.read_sql_query(query, self._con)
            values_of_attr = df_with_the_attr[attr].unique()
            
            if len(needed_columns)==0:
                s = '*'
            else:
                s = ','.join(needed_columns)
                        
            for x in values_of_attr:
                query = f"""
                SELECT {s}
                FROM {DATABASE_NAME}
                WHERE {attr} = '{x}'
                """
                if self._storage_type == StorageType.ROW_BASED: 
                    df = pd.read_sql_query(query, self._con)
                    yield df
                elif self._storage_type == StorageType.COLUMN_BASED:
                    df = self._con.execute(query).fetchdf()
                yield df  
        
    def print_event_log(self):
        """
        prints the event log on the console
        """
        df = self.get_event_log()
        print(df)  
        
    def save_adapted_event_log(self, file_name="adapted_event_log.csv"):
        """saves the adapted event log that is internally stored in the database in the given file

        Parameters
        ----------
        file_name : str, optional
            file name where the log should be saved, by default "adapted_event_log.csv"
        """
        self.get_event_log().to_csv(f'{os.getcwd()}/output/{file_name}')
                     
    def create_interval_log(self):
        """
        if possible construct an interval log out of the stored atomic event log and save it into the database as a separate table
        """
        df = self.get_event_log()
        interval_df = EventToIntervalLog().transform(self._config, df)
        
        interval_df = interval_df.sort_values(["Start", "Complete", "Job","Machine"], ignore_index=True)
        interval_df['Interval_ID'] = range(0, len(interval_df.index))
    
        if self._storage_type == StorageType.ROW_BASED:
            interval_df.to_sql(INTERVAL_DATABASE_NAME, self._con, if_exists='replace', index=False)
            cursor = self._con.cursor()
            cursor.execute(f"CREATE INDEX job_index_2 ON {INTERVAL_DATABASE_NAME} (Job)")
            cursor.execute(f"CREATE INDEX machine_index_2 ON {INTERVAL_DATABASE_NAME} (Machine)")
        elif self._storage_type == StorageType.COLUMN_BASED:
            self._con.execute(f"DROP TABLE IF EXISTS {INTERVAL_DATABASE_NAME}")
            self._con.from_df(interval_df).create(INTERVAL_DATABASE_NAME)
        elif self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            self._interval_datataframe = interval_df
    
    def get_interval_log(self):
        """
        loads the interval log from the database
        
        Returns
        -----------
        dataframe : pandas.DataFrame
            the whole interval log from the data base as a data frame
        """
        query = f"SELECT * FROM {INTERVAL_DATABASE_NAME}"
        
        if self._storage_type == StorageType.ROW_BASED:
            df = pd.read_sql_query(query, self._con)
            df['Start'] = pd.to_datetime(df['Start'], format="%Y-%m-%d %H:%M:%S")
            df['Complete'] = pd.to_datetime(df['Complete'], format="%Y-%m-%d %H:%M:%S")
            return df
        elif self._storage_type == StorageType.COLUMN_BASED:
            df = self._con.execute(query).fetchdf()
            return df
        elif self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            return self._interval_datataframe
             
    def get_interval_sequence(self, attr, needed_columns=[]):   #This is the operation Sequence we introduced in Chapter 2.2.2.4
        """
        groups the interval log database and returns a generator object of the sequence of the given attribute as a generator object
        
        Parameters
        -----------
        attr : str
            the attribute by which the event log should be grouped
            
        needed_columns : List[str], optional
            a list of attributes/columns that need to be accessed, by default [] (the empty list stands for all attributes)
             
        Yields
        ------
        pandas.DataFrame
            The next interval sequence as a dataframe.
        """
        if self._storage_type == StorageType.COLUMN_BASED_AT_ONCE:
            for key, group in self.get_interval_log().groupby([attr]):
                yield group
        
        else:
            query = f"""SELECT {attr}
            FROM {INTERVAL_DATABASE_NAME}
            GROUP BY {attr}
            """
            df_with_the_attr = pd.read_sql_query(query, self._con)
            values_of_attr = df_with_the_attr[attr].unique()
                    
            if len(needed_columns)==0:
                s = '*'
            else:
                s = ','.join(needed_columns)        
                        
            for x in values_of_attr:
                query = f"""
                SELECT *
                FROM {INTERVAL_DATABASE_NAME}
                WHERE {attr} = '{x}'
                """
                if self._storage_type == StorageType.ROW_BASED: 
                    df = pd.read_sql_query(query, self._con)
                    yield df
                elif self._storage_type == StorageType.COLUMN_BASED:
                    df = self._con.execute(query).fetchdf()
                    yield df                  