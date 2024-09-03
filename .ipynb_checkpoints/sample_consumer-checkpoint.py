import sys
import json
import time
import logging
import traceback
from datetime import datetime
from pytz import timezone

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.consumer.fetcher import ConsumerRecord

from utils import get_basic_logger, get_kafka_url, get_default_kafka_consumer_configs


class MessageConsumer:
    def __init__(self, logger, topic, partition, num_partitions, replication_factor=1):
        
        logger = get_basic_logger() if not logger else logger
        self.logger = logger
        
        kafka_addr = get_kafka_url()
        
        """ Check topic exists. """
        try:
            admin = KafkaAdminClient(bootstrap_servers=[kafka_addr])
            topics = admin.list_topics()
            
            if topic not in topics:
                new_topic = NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
                resp = admin.create_topics(new_topics=[new_topic], validate_only=False)
                self.logger.info(f'create new topic: {resp}')
                
            topics = admin.list_topics()
            self.logger.info(f'exists topics: {topic}')
            
        except TopicAlreadyExistsError as taee:
            self.logger.info(f'topic : {topic} is exists.')
            
        except Exception as e:
            self.logger.error(type(e))
            self.logger.error(e)

            
        """ Consumer """
        configs = get_default_kafka_consumer_configs()
        configs['bootstrap_servers'] = kafka_addr
        configs['max_poll_records'] = 100
        configs['enable_auto_commit'] = False
        configs['max_partition_fetch_bytes'] = 1048576*2
            
        
        self.c = KafkaConsumer(**configs)
        self.logger.info(self.c.__dict__)
        
        tp = TopicPartition(topic, partition)
        self.c.assign([tp])
        
        now = datetime.now(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f'[{now}] Run Consumer TOPIC_NAME : {topic}, PARTITION : {partition} init COMPLETE')
        
        
    def run(self):
        self.logger.info(f'------[START CONSUMER]------')
        while True:
            
            msg_by_type = dict()
            
            # 1. get bulk messages
            bulk_messages = self.c.poll(timeout_ms=2000)
            if not bulk_messages:
                continue
            
            for _, msgs in bulk_messages.items():
                for msg in msgs: #msg : ConsumerRecord

                    # 1) headers
                    headers = msg.headers
                    headers_dict = dict()
                    for hkey, hvalue in headers:
                        if isinstance(hvalue, bytes):
                            hvalue = hvalue.decode('utf-8')
                        if hvalue.isnumeric():
                            hvalue = int(hvalue)
                        headers_dict[hkey] = hvalue
                    msg_type = headers_dict['msg_type'] if 'msg_type' in headers_dict else 0
                    
                    if not msg_type:
                        continue
                        
                    ts = str(msg.timestamp)
                    if len(ts) > 10:
                        ts = float(f'{ts[:10]}.{ts[10:]}')
                    produce_time = datetime.fromtimestamp(ts, timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S.%f')
            
                    self.logger.info(f"[GET MESSAGE TYPE {msg_type}] offset: {msg.offset}, {produce_time}")
                       
                            
                    # 2) value
                    try:
                        if msg.value == b'\x00\x00\x00\x00\x00\x00':
                            continue
                        msg_value = msg.value.decode('utf-8').strip()
                        if not msg_value:
                            continue
                        msg_value = json.loads(msg_value) # dict

                        if msg_type not in msg_by_type:
                            msg_by_type[msg_type] = dict()
                        if user_id not in msg_by_type[msg_type]:
                            msg_by_type[msg_type][user_id] = list()
                        msg_by_type[msg_type][user_id].append(msg_value)

                    except json.decoder.JSONDecodeError as de:
                        self.logger.error(f'Invalid value: {msg_value}, {len(msg_value)}, {de}')

                    except Exception as e:
                        self.logger.error(type(e))
                        self.logger.error(e)
                        self.logger.error(traceback.format_exc())
            
            # 2. process msg_by_type
            print (msg_by_type)
            self.c.commit()
            
                    
if __name__ == '__main__':
    
    logger = get_basic_logger(log_level=logging.INFO)
    topic = 'test_batch4'
    partition=0
    num_partitions=4
    replication_factor=1
    
    mc = MessageConsumer(logger, topic, partition, num_partitions, replication_factor)
    mc.run()