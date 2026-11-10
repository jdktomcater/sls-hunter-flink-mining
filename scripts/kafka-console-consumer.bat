kafka-console-consumer.sh \
  --bootstrap-server 172.30.82.120:9092,172.30.82.121:9092,172.30.82.122:9092 \
  --topic sls-mining-window-fixed \
  --consumer-property isolation.level=read_committed \
  --group "security-fixture-observer-$(date +%s)"