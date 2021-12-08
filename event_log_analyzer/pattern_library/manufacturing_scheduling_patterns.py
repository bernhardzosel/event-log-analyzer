"""
This module contains all patterns, that have the Manufacturing Scheduling Pattern as a prerequisite 
"""
import pandas as pd
import numpy as np
from event_log_analyzer.utils import logger, log_time, pattern_logger
from event_log_analyzer.pattern_library.pattern import Pattern

CHECK_ON_INTERVAL = True    #for test reasons we can specify whether the pattern conditions should be checked on an atomic or an interval log
  

class ManufacturingScheduling(Pattern):
    """
    This Pattern sets the basis for all further Scheduling patterns, it has to be checked before the other patterns can be checked to ensure that the pattern contains all needed information.       
    """
    name = "Manufacturing_Scheduling_Pattern"   
    dependencies = {"requires":[], "forces":[]} 
    
    @log_time(logger, "Manufacturing Scheduling Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        the pattern applies if all needed attributes are available (Job, Machine, Transaction_Type) and if the transaction_types represent intervals, 
        if the pattern applies we create an interval log which is better suited for checking the further pattern conditions
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """     
        self.check_dependencies()

        if self.applies is None:
            df = event_log.get_event_log()
            if 'Job' not in df.columns:
                pattern_logger.info("\t>>>\tManufacturing Scheduling does not hold!!! (no Job attribute existing)")
                return False        
            elif 'Machine' not in df.columns:
                pattern_logger.info("\t>>>\tManufacturing Scheduling does not hold!!! (no Machine attribute existing)")
                return False
            if 'Transaction_Type' not in df.columns:
                pattern_logger.info("\t>>>\tManufacturing Scheduling does not hold!!! (no Transaction Type existing in atomic event log representation)")
                return False
            
            event_log.create_interval_log()
            
            return True   
        else:
            return self.applies

    
class JobShop(Pattern):
    """
    In the Job Shop Pattern, each job needs to be processed on a subset of the machines in a predetermined order which might be different between the jobs.
    """
    dependencies = {"requires":['Manufacturing_Scheduling_Pattern'], "forces":[]}
    name = "Job_Shop_Pattern"
    
    @log_time(logger, "Job Shop Condition a)")
    def cond_a(self, event_log):
        """
        jobs cannot be processed without a machine
        
        vectorization is used on the whole event log to check whether there exists an event without a machine
        """
        dataframe = event_log.get_event_log()
        series = dataframe["Machine"].isna()
        if series.any():
            pattern_logger.info("\t>>>\tJob Shop Condition a) does not hold!!!")
            for index, row in dataframe[series].iterrows():
                pattern_logger.info(f"""\t\tRow ID {row["Row_ID"]}: No machine is assigned to job "{row["Job"]}"!""")
            return False
        return True
    
    @log_time(logger, "Job Shop Condition b)")
    def cond_b(self, event_log):
        """
        no two operations of the same job can be processed at the same time
        
        the condition is checked on the atomic event log
        """
        for df in event_log.get_sequence("Job"):   
            look_for_op = None         
            for index, row in df.iterrows():
                if look_for_op != None:
                    if row['Transaction_Type']!="complete" or row["Activity_Instance"]!=look_for_op:
                        pattern_logger.info(f"""\t>>>\tJob Shop Condition b) does not hold!!!""")
                        pattern_logger.info(f"""\t\tRow ID {row["Row_ID"]}: Job "{row["Job"]}" runs two operations at the same time!""")
                        return False
                    else:
                        look_for_op = None
                if row["Transaction_Type"]=="start":
                    look_for_op = row["Activity_Instance"]
                
        return True
    
    @log_time(logger, "Job Shop Condition b)")
    def cond_b_interval(self, event_log):
        """
        no two operations of the same job can be processed at the same time
        
        the condition is checked on the interval log
        """
        for df in event_log.get_interval_sequence("Job"):
            number_rows = len(df)
            for i in range(0,number_rows-1):
                if df['Complete'].values[i] > df['Start'].values[i+1]:
                    pattern_logger.info(f"\t>>>\tJob Shop Condition b) does not hold!!!") 
                    pattern_logger.info(f"""\t\tRow ID {df["Row_ID"].values[i+1]}: Job "{df['Job'].values[i+1]}" runs two operations at the same time!""")
                    return False
        return True
     
    @log_time(logger, "Job Shop Condition c)")
    def cond_c(self, event_log):
        """
        No machine can process more than one operation at the same time
            
        the condition is checked on the atomic event log
        """
        for df in event_log.get_sequence("Machine"): 
            look_for_op = None         
            for index, row in df.iterrows():
                if look_for_op != None:
                    if row['Transaction_Type']!="complete" or row["Activity_Instance"]!=look_for_op:
                        pattern_logger.info(f"""\t>>>\tJob Shop Condition c) does not hold!!!""")
                        pattern_logger.info(f"""\t\tRow ID {row["Row_ID"]}: Machine "{row["Machine"]}" processes two operations at the same time!""")
                        return False
                    else:
                        look_for_op = None
                if row["Transaction_Type"]=="start":
                    look_for_op = row["Activity_Instance"]

        return True
    
    @log_time(logger, "Job Shop Condition c)")
    def cond_c_interval(self, event_log):
        """
        No machine can process more than one operation at the same time
            
        the condition is checked on the interval log
        """
        for df in event_log.get_interval_sequence("Machine"):
            number_rows = len(df)
            for i in range(0,number_rows-1):
                if df['Complete'].values[i] > df['Start'].values[i+1]:
                    pattern_logger.info(f"\t>>>\tJob Shop Condition c) does not hold!!!")
                    pattern_logger.info(f"""\t\tRow ID {df["Row_ID"].values[i+1]}: Machine "{df['Machine'].values[i+1]}" processes two operations at the same time!""")
                    return False
        return True
    
    @log_time(logger, "Job Shop Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        the pattern applies if all three conditions (a), (b) and (c) apply
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """   
        self.check_dependencies()
        
        if self.applies is None:            
            a = self.cond_a(event_log)
            if CHECK_ON_INTERVAL:                       #only for test reasons between checking the conditions on atomic or interval log
                b = self.cond_b_interval(event_log)
                c = self.cond_c_interval(event_log)     
            else:                           
                b = self.cond_b(event_log) 
                c = self.cond_c(event_log)
            
            return a and b and c
        
        else:
            return self.applies
    
class FlowShop(Pattern):
    """
    In the Flow Shop Pattern, each job needs to be processed on each of the machines in a predetermined order and the order is the same for all jobs.
    """
    dependencies = {"requires":['Job_Shop_Pattern'], "forces":[]}
    
    name = "Flow_Shop_Pattern"
    
    @log_time(logger, "Flow Shop Condition a)")
    def cond_a(self, event_log):
        """
        every job can be processed on every machine at most once
        """
        for df in event_log.get_interval_sequence("Job"):
            if len(df['Machine']) != len(df['Machine'].unique()):
                pattern_logger.info("\t>>>\tFlow Shop Condition a) does not hold!!!")
                seen = set()
                duplicates = [x for x in df['Machine'] if x in seen or seen.add(x)]
                j = df["Job"][0]
                pattern_logger.info(f"""\t\tRow ?: Job "{j}" is processed on machine(s) {duplicates} more than once!""")
                return False
        return True
            
    @log_time(logger, "Flow Shop Condition b)")
    def cond_b(self, event_log):
        """
        every job consists of exactly as much operations as machines exist in total
        """
        dataframe = event_log.get_event_log()
        allMachines = dataframe["Machine"].unique()
        
        for df in event_log.get_interval_sequence("Job"):
            if len(df) != len(allMachines):
                pattern_logger.info("\t>>>\tFlow Shop Condition b) does not hold!!!")
                j = df["Job"][0]
                pattern_logger.info(f"""\t\tRow ?: Job "{j}" has {len(df)} operations but in total there exist {len(allMachines)} machines""")
                return False
        return True
    
    @log_time(logger, "Flow Shop Condition c)")
    def cond_c(self, event_log):
        """
        the route of every job through the machines must be the same
        """
        df_first_job = next(event_log.get_interval_sequence("Job"))
        machine_route = df_first_job['Machine'].values #machine route of first job
        
        for df in event_log.get_interval_sequence("Job"):  #now starting from second after one next() call
            if not np.array_equal(machine_route, df['Machine'].values):
                pattern_logger.info("\t>>>\tFlow Shop Condition c) does not hold!!!")
                first_job = df_first_job["Job"][0]
                pattern_logger.info(f"""\t\tRow ?: The routes of the operations of job "{first_job}" ({machine_route}) and job "{df['Job'][0]}" ({df['Machine'].values}) differ!""")
                return False
        return True
    
    @log_time(logger, "Flow Shop Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        the pattern applies if all three conditions (a), (b) and (c) apply
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """   
        self.check_dependencies()
        
        if self.applies is None:            
            a = self.cond_a(event_log)
            b = self.cond_b(event_log)           
            c = self.cond_c(event_log)
            
            return a and b and c
        else:
            return self.applies
    
class Permutation(Pattern):
    """
    In the Permutation Pattern, when processing a number of jobs on machines under the Flow Shop Pattern no job is allowed to overtake another job, so the order in which jobs are processed on a machine is the same for all machines.
    """
    dependencies = {"requires":['Flow_Shop_Pattern'], "forces":[]}
    name = "Permutation_Pattern"
    
    @log_time(logger, "Permutation Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        check whether the Permutation Pattern applies, i.e. jobs cannot overtake each other, all machines process the jobs in the same order

        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        self.check_dependencies()
        if self.applies is None:
            df_first_machine = next(event_log.get_interval_sequence("Machine"))
            job_route = df_first_machine['Job'].values
            
            for df in event_log.get_interval_sequence("Machine"):  #now starting from second after one next() call
                if not np.array_equal(job_route, df['Job'].values):
                    pattern_logger.info("\t>>>\tPermutation Pattern does not hold!!!")
                    first_machine = df_first_machine["Machine"][0]
                    pattern_logger.info(f"""\t\tRow ?: On Machine "{df['Machine'][0]}" the jobs are processed in a different order({df['Job'].values}) than on machine "{first_machine}" ({job_route})!""")
                    return False
            return True
        else:
            return self.applies
    
class NoWait(Pattern):
    """
    In the No Wait Pattern, when a number of jobs are to be processed on a set of machines, jobs are not allowed to wait between operations.
    """
    dependencies = {"requires":['Flow_Shop_Pattern'], "forces":[]}
    name = "No_Wait_Pattern"
    
    @log_time(logger, "No Wait Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        jobs are not allowed to wait between operations
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        self.check_dependencies()
        if self.applies is None:                      
            for df in event_log.get_interval_sequence("Job"):
                for i in range(1,len(df)):
                    if df['Complete'].values[i-1] != df['Start'].values[i]:
                        pattern_logger.info(f"\t>>>\t{self.name} does not hold!!!")
                        pattern_logger.info(f"""\t\tRow {df['Row_ID'].values[i-1]}: Job "{df['Job'].values[i]}" has a break between machine "{df['Machine'].values[i-1]}" and machine "{df['Machine'].values[i]}" """)
                        return False                    
            return True
        else:
            return self.applies
    
class OneBlocking(Pattern):
    """
    In the 1-Blocking Pattern, when processing a number of jobs on machines under the Flow Shop pattern, there must at any time be at most one job queuing in front of a machine, so the buffer between any two machines has capacity 1.
    """
    dependencies = {"requires":['Permutation_Pattern'], "forces":[]}

    name = "1-blocking_Pattern"
    
    @log_time(logger, "One Blocking Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        at any time there cannot be more than one job queing in front of a machine
        
        Note: The pattern is not implemented yet, therefore the pattern never applies!!!
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        self.check_dependencies()
        if self.applies is None:
            return False
        else:
            return self.applies

class DistinguishableResource(Pattern):
    """
    In the Distinguishable Resource Pattern, when processing a number of operations, exactly one resource from a set of distinguishable resources needs to be present for each of the operations.
    
    Currently the pattern only checks one resource (in the 'Resource' column), when having several resources, they need to be manufally checked one by one
    """
    dependencies = {"requires":['Manufacturing_Scheduling_Pattern'], "forces":[]}
    name = "Distinguishable_Resource_Pattern"
    
    @log_time(logger, "Distinguishable Resource Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        one resource from a set of distinguishable resources must be present at every operation, but only one operation can be processed per resource
        (checking on atomic event log)
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        self.check_dependencies()
        if self.applies is None:
            df = event_log.get_event_log()
            
            if 'Resource' not in df.columns:
                pattern_logger.info("\t>>>\tDistinguishable Resource does not hold!!!")
                pattern_logger.info(f"""\t\tRow ?: No resource column is specified!""")
                return False
            
            series = df["Resource"].isna()
            if series.any():
                pattern_logger.info("\t>>>\tDistinguishable Resource does not hold!!!")
                for index, row in df[series].iterrows():
                    pattern_logger.info(f"""\t\tRow {row["Row_ID"]}: No resource is assigned to job "{row["Job"]}" at machine "{row["Machine"]}"!""")
                return False
            
            for df in event_log.get_sequence("Resource"): 
                look_for_op = None         
                for index, row in df.iterrows():
                    if look_for_op != None:
                        if row['Transaction_Type']!="complete" or row["Activity_Instance"]!=look_for_op:
                            pattern_logger.info(f"""\t>>>\tRow {row["Row_ID"]}: Resource "{row["Resource"]}" processes two operations at the same time!!!""")
                            return False
                        else:
                            look_for_op = None
                    if row["Transaction_Type"]=="start":
                       look_for_op = row["Activity_Instance"]
            return True
        else:
            return self.applies
    
class IndistinguishableResource(Pattern):  
    """
    In the Indistinguishable Resource Pattern, when processing a number of operations, exactly one resource from a set of indistinguishable resources needs to be present for each operation.
   
    Currently the pattern only checks one resource (in the 'Resource' column), when having several resources, they need to be manufally checked one by one
    """
    dependencies = {"requires":['Manufacturing_Scheduling_Pattern'], "forces":[]}
    name = "Indistinguishable_Resource_Pattern"
    
    @log_time(logger, "Indistinguishable Resource Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        for each operation there must be exactly one resource from a set of indistinguishable resources
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        self.check_dependencies()
        if self.applies is None:
            first_df = next(event_log.get_interval_sequence("Job"))
            if 'Resource' not in first_df.columns:
                pattern_logger.info("\t>>>\tIndistinguishable Resource does not hold!!!")
                pattern_logger.info(f"""\t\tRow ?: No resource column is specified!""")
                return False
            
            ressources = []
            for df in event_log.get_interval_sequence("Job"):
                for res in df["Resource"]:
                    if pd.isna(res):
                        pattern_logger.info("\t>>>\tIndistinguishable Resource does not hold!!!")
                        pattern_logger.info(f"""\t\tRow ?: Not all jobs are assigned to a resource!""")
                        return False
                    ressources.append(res)
            all_same = all(element == ressources[0] for element in ressources)
            if not all_same:
                pattern_logger.info("\t>>>\tIndistinguishable Resource does not hold!!!")
                pattern_logger.info(f"""\t\tRow ?: The resources are not indistinguishable (they differ)!""")
                return False
            return True 
        else:
            return self.applies
    
class ResourceSetupTimes(Pattern):
    """
    In the Resource Setup Times Pattern, a number of operations are to be processed by some distinguishable resources (Distinguishable Resources Pattern needs to apply) and each resource needs a certain amount of time between two operations, where the time depends on the type of the two tasks.
    
    Note: The pattern is not implemented yet, therefore the pattern never applies!!!
    """
    dependencies = {"requires":['Distinguishable_Resource_Pattern'], "forces":[]}
    name = "Resource_Setup_Times_Pattern"
    
    @log_time(logger, "Resource Setup Times Pattern checking duration")
    def pattern_applies(self, event_log):
        """
        each resource needs a certain amount of time between two operations
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        self.check_dependencies()
        if self.applies is None:
            return False
        else:
            return self.applies