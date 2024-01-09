from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import os
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
KAFKA_RETENTION_BYTE=int(os.environ['REDPANDA_RETENTION_BYTE'])
try:
    KAFKA_RETENTION_BYTE=int(KAFKA_RETENTION_BYTE)
except:
    KAFKA_RETENTION_BYTE=None
admin=KafkaAdminClient(bootstrap_servers=KAFKA_ADDRESS)
new_topics=[]
for topic in ['review','inference-input','inference-output']:
    if topic not in admin.list_topics():
            print(topic)
            new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1,topic_configs={'retention.bytes': KAFKA_RETENTION_BYTE}))
try:
    admin.create_topics(new_topics)
except:
    pass