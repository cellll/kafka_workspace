import os
import sys
import json
from threading import Thread
import cv2
from confluent_kafka import Producer, KafkaError, KafkaException


class MessageProducer:
    def __init__(self, logger, topic='test_topic', partition=0):
        
        vp_utils = __import__('utils', fromlist = ['get_basic_logger', 'get_default_kafka_uri', 'get_default_kafka_producer_configs'])
        get_basic_logger = getattr(vp_utils, 'get_basic_logger')
        get_default_kafka_uri = getattr(vp_utils, 'get_default_kafka_uri')
        get_default_kafka_producer_configs = getattr(vp_utils, 'get_default_kafka_producer_configs')
        
        if not logger:
            logger = get_basic_logger()
        self.logger = logger
        
        self.logger.info(get_default_kafka_uri())
        kafka_addr = get_default_kafka_uri()
        configs = get_default_kafka_producer_configs()
        configs['bootstrap.servers'] = kafka_addr
        
        self._producer = Producer(configs)
        self._producer.init_transactions()
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()
        
        self.topic = topic
        self.partition = partition
#         self.num_partition = 12

        
    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(1)
        
        
    def close(self):
        self._cancelled = True
        self._poll_thread.join()
        
        
    def acks(self, err, msg):
        if err:
            self.logger.error (f'[ERROR] {err}')
        else:
            self.logger.info (f'[SUCCESS] {msg.topic()}_{msg.partition()}, Offset : {msg.offset()}, Length : {len(msg.value())}, Timestamp : {msg.timestamp()}')
            
        
    def produce(self, data:dict, msg_type:int, eof:bool=False):
        
        try: 
            eof = 1 if eof else 0
            self._producer.begin_transaction()
            
            headers = {'msg_type': str(msg_type), 'eof' : str(eof)}
            self._producer.produce(
                self.topic, 
                json.dumps(data),
                partition = self.partition,
                headers = headers,
                callback=self.acks
            )
            self._producer.commit_transaction()
            
        except KafkaException as ke:
            code = ke.args[0].code()
            if code == -195:
                err_msg = 'broker down'
            elif code == -185:
                err_msg = 'check broker id'
            elif code == -172:
                err_msg = 'transaction problem occured'
            else:
                err_msg = ''
            self.logger.error(f'[ERROR] {ke}, {err_msg}')
            self._producer.abort_transaction()
            
        except Exception as e:
            self.logger.error(f'[ERROR] {e}')
            self._producer.abort_transaction()
            
if __name__ == '__main__':
    
    mp = MessageProducer(logger=None, topic='test_topic2', partition=0)
    data = {
        'test': 'data'
    }
    mp.produce(data=data, msg_type=1, eof=1)
    print ('Done')
    mp.close()
    
    
    
    
    
    
    
    
    
