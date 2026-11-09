#!/usr/bin/env python3
"""Operator-invoked read-only export; connection config stays in the old environment.
Uses the old profile loader; does not embed credentials or modify the old project.
"""
import sys,json,pathlib,importlib,argparse,datetime
sys.dont_write_bytecode=True
p=argparse.ArgumentParser();p.add_argument('--old-source',required=True);p.add_argument('--family',required=True,choices=['tracing_sas_linux','tracing_sas_win','tracing_sas_jumper']);p.add_argument('--target',required=True);p.add_argument('--days',type=int,default=5)
a=p.parse_args();sys.path.insert(0,a.old_source)
m=importlib.import_module('mining.'+a.family+'.calendar_query');day=datetime.date.fromisoformat(a.target)
print(json.dumps({a.family:{a.target:m.fleet_proc_host_cardinality([(day-datetime.timedelta(days=i)).isoformat() for i in range(1,a.days+1)])}},ensure_ascii=False,indent=2))
