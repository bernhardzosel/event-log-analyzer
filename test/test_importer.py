import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from event_log_analyzer import importer as event_log_importer

def test_import_event_log():
    log = event_log_importer.import_event_log("test/data/event_log.json")
    
    log_df = log.get_event_log()
    assert len(log_df) == 18, "there are 3 jobs and 3 machines, i.e. 9 operations -> 2*9=18 atomic events" 
    
    job_sequence = log.get_sequence("Job")
    machine_sequence = log.get_sequence("Machine")
    assert len(list(job_sequence)) == 3, "there are three different jobs, therefore the job sequence should be of length 3"
    assert len(list(machine_sequence)) == 3, "there are three different machines, therefore the machine sequence should be of length 3"
    
    log.create_interval_log()
    job_interval_sequence = log.get_interval_sequence("Job")
    assert len(list(job_interval_sequence)) == 3, "there are three different jobs, therefore the job sequence should be of length 3"
    
def test_import_interval_log():
    log = event_log_importer.import_event_log("test/data/interval_log.json")
    
    log_df = log.get_event_log()
    assert len(log_df) == 18, "there are 3 jobs and 3 machines, i.e. 9 operations -> converted to atomic event log with 18 atomic events"
    
    job_sequence = log.get_sequence("Job")
    machine_sequence = log.get_sequence("Machine")
    assert len(list(job_sequence)) == 3, "there are three different jobs, therefore the job sequence should be of length 3"
    assert len(list(machine_sequence)) == 3, "there are three different machines, therefore the machine sequence should be of length 3"
    
    log.create_interval_log()
    job_interval_sequence = log.get_interval_sequence("Job")
    assert len(list(job_interval_sequence)) == 3, "there are three different jobs, therefore the job sequence should be of length 3"
    
def test_import_log_in_xes_format():
    log = event_log_importer.import_event_log("test/data/xes_gen_event_log.json")
    
    log_df = log.get_event_log()    
    assert len(log_df) == 1000, "there are 50 jobs and 10 machines, i.e. 500 operations -> converted to atomic event log with 1000 atomic events"
    
    job_sequence = log.get_sequence("Job")
    machine_sequence = log.get_sequence("Machine")
    assert len(list(job_sequence)) == 50, "there are three different jobs, therefore the job sequence should be of length 3"
    assert len(list(machine_sequence)) == 10, "there are three different machines, therefore the machine sequence should be of length 3"
    
    log.create_interval_log()
    job_interval_sequence = log.get_interval_sequence("Job")
    assert len(list(job_interval_sequence)) == 50, "there are three different jobs, therefore the job sequence should be of length 3"