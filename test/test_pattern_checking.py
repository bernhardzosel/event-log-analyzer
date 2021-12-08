import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from event_log_analyzer import importer as event_log_importer
from event_log_analyzer.pattern_library import pattern_structure


def test_check_patterns():
    log = event_log_importer.import_event_log("test/data/event_log.json")
    
    ps = pattern_structure.PatternStructure()
    ps.check_all_patterns(log)
    
    applying_patterns = list(map(lambda x: x.name, ps.applying_pattern_list()))
        
    assert "Manufacturing_Scheduling_Pattern" in applying_patterns, "Manufacturing Scheduling Pattern should apply on the event log"
    assert "Job_Shop_Pattern" in applying_patterns, "Job Shop Pattern should apply on the event log"
    assert "Flow_Shop_Pattern" in applying_patterns, "Flow Shop Pattern should apply on the event log"
    assert "No_Wait_Pattern" not in applying_patterns, "No Wait Pattern should not apply on this event log"
    
def test_check_patterns_interval_log():
    log = event_log_importer.import_event_log("test/data/interval_log.json")
    
    ps = pattern_structure.PatternStructure()
    ps.check_all_patterns(log)
    
    applying_patterns = list(map(lambda x: x.name, ps.applying_pattern_list()))
        
    assert "Manufacturing_Scheduling_Pattern" in applying_patterns, "Manufacturing Scheduling Pattern should apply on the event log"
    assert "Job_Shop_Pattern" in applying_patterns, "Job Shop Pattern should apply on the event log"
    assert "Flow_Shop_Pattern" in applying_patterns, "Flow Shop Pattern should apply on the event log"
    assert "No_Wait_Pattern" not in applying_patterns, "No Wait Pattern should not apply on this event log"
    
def test_check_patterns_xes_format():
    log = event_log_importer.import_event_log("test/data/xes_gen_event_log.json")
    
    ps = pattern_structure.PatternStructure()
    ps.check_all_patterns(log)
    
    applying_patterns = list(map(lambda x: x.name, ps.applying_pattern_list()))
        
    assert "Manufacturing_Scheduling_Pattern" in applying_patterns, "Manufacturing Scheduling Pattern should apply on the event log"
    assert "Job_Shop_Pattern" in applying_patterns, "Job Shop Pattern should apply on the event log"
    assert "Flow_Shop_Pattern" in applying_patterns, "Flow Shop Pattern should apply on the event log"
    assert "No_Wait_Pattern" not in applying_patterns, "No Wait Pattern should not apply on this event log"
    
def test_check_patterns_on_real_log():
    log = event_log_importer.import_event_log("test/data/production_data.json")
    
    ps = pattern_structure.PatternStructure()
    ps.check_all_patterns(log)
    
    applying_patterns = list(map(lambda x: x.name, ps.applying_pattern_list()))

    assert len(applying_patterns) == 1, "only one pattern is valid so far"        
    assert "Manufacturing_Scheduling_Pattern" in applying_patterns, "Manufacturing Scheduling Pattern should apply on the event log"
