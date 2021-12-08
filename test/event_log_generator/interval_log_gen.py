from random import randrange
import datetime 
import pandas as pd

NUMBER_OF_JOBS = 100
NUMBER_OF_MACHINES = 100

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
previousDate = datetime.datetime(2013, 9, 20, 13, 00)
for j in Jobs:
    for m in Machines:
        start = next_random_date(previousDate)
        end = next_random_date(start)
        previousDate = end
        events.append([j,m,resource, start, end])

dataframe = pd.DataFrame(data=events, columns=['Job', 'Machine','Resource','Start','End'])

dataframe.to_csv("data/generated_interval_log.csv", sep=';')
