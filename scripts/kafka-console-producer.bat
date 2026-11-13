@echo off
setlocal
cd /d "%~dp0"

python generate_security_test_events.py --kafka-key > "%~dp0security-test-kafka.txt"
if errorlevel 1 exit /b 1

rem Default key.separator is a real tab, matching the generator. Do not pass
rem $'\t' or \t: cmd leaves those as literal text, and Kafka does not unescape them.
C:\yuhata\sites\kafka_2.13-4.0.0\bin\windows\kafka-console-producer.bat --bootstrap-server 172.30.82.120:9092,172.30.82.121:9092,172.30.82.122:9092 --topic topic-sas-linux-test --property parse.key=true < "%~dp0security-test-kafka.txt"
