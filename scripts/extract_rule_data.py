#!/usr/bin/env python3
"""Extract literal rule data through AST, without executing old code."""
import ast,json,pathlib
root=pathlib.Path(__file__).resolve().parents[1]
src=root.parent/'sls-hunter-spark-mining/src/main/python/mining'
out=root/'src/main/resources/rules'
def value(n):
 if isinstance(n,ast.Call):
  if isinstance(n.func,ast.Name) and n.func.id in ('frozenset','tuple'):return value(n.args[0])
  if isinstance(n.func,ast.Name) and n.func.id=='VendorRule':return {'bases':value(n.args[0]),'markers':value(n.args[1]),'id':value(n.args[2])}
 if isinstance(n,(ast.List,ast.Set,ast.Tuple)):return [value(x) for x in n.elts]
 if isinstance(n,ast.Dict):return {value(k):value(v) for k,v in zip(n.keys,n.values)}
 return ast.literal_eval(n)
for family in ('tracing_sas_linux','tracing_sas_win','tracing_sas_jumper'):
 data={}
 for fname in ('structural_intrusion_score.py','whitelist_rules.py','whitelist_assets.py','behavior_patterns.py'):
  path=src/family/fname
  if not path.exists():continue
  for n in ast.parse(path.read_text()).body:
   if isinstance(n,(ast.Assign,ast.AnnAssign)):
    key=(n.targets[0] if isinstance(n,ast.Assign) else n.target)
    if isinstance(key,ast.Name) and key.id.isupper():
     try:data[key.id]=value(n.value)
     except (ValueError,TypeError):pass
 (out/(family+'.json')).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
