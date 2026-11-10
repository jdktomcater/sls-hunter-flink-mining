python3 scripts/generate_security_test_events.py --kafka-key \
  > /tmp/security-test-kafka.txt

kafka-console-producer.sh \
  --bootstrap-server 172.30.82.120:9092,172.30.82.121:9092,172.30.82.122:9092 \
  --topic topic-sas-linux-test \
  --property parse.key=true \
  --property key.separator=$'\t' \
  < /tmp/security-test-kafka.txt