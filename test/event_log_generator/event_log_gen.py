import datetime 
import pandas as pd
from random import randrange
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.util import dataframe_utils

NUMBER_OF_JOBS = 50
NUMBER_OF_MACHINES = 10


Jobs = []
for i in range(1, NUMBER_OF_JOBS + 1):
    s = f"j{i}"
    Jobs.append(s)

Machines = []
for i in range(1, NUMBER_OF_MACHINES + 1):
    s = f"m{i}"
    Machines.append(s)

resource = "Worker"

def next_random_date(start):
    return start + datetime.timedelta(minutes=randrange(1,10))

events=[]
previousDate = datetime.datetime(2013, 9, 20,13,00)
for j in Jobs:
    for m in Machines:
        timestamp_start = next_random_date(previousDate)
        timestamp_end = next_random_date(timestamp_start)
        previousDate = timestamp_end
        events.append([j,m,resource, timestamp_start, 'start'])
        events.append([j,m,resource, timestamp_end, 'complete'])

dataframe = pd.DataFrame(data=events, columns=['Job', 'Machine','Resource','Timestamp','Transaction_Type'])

dataframe.to_csv("test/data/gen_event_log.csv", sep=';')

dataframe = dataframe_utils.convert_timestamp_columns_in_df(dataframe)

parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'Job'}
event_log = log_converter.apply(dataframe, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

xes_exporter.apply(event_log, 'test/data/xes_gen_event_log.xes' )