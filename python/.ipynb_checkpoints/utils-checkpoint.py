import os
import uuid
import configparser
import logging

###################################################### kafka
def get_default_kafka_uri():
    dirpath = os.path.dirname(os.path.realpath(__file__))
    cfg_filepath = os.path.join(dirpath, 'kafka.cfg')
    cfg = configparser.ConfigParser()
    cfg.read(cfg_filepath)
    uri = cfg.get('kafka', 'uri')
    return uri

def get_default_kafka_producer_configs():
    dirpath = os.path.dirname(os.path.realpath(__file__))
    cfg_filepath = os.path.join(dirpath, 'kafka.cfg')
    cfg = configparser.ConfigParser()
    cfg.read(cfg_filepath)
    
    config = dict()
    for key, value in cfg['producer'].items():
        if isinstance(value, str):
            if value.isnumeric():
                value = int(value)
        key = key.replace('_', '.')
        config[key] = value
        
    config['enable.idempotence'] = True if config['enable.idempotence'] == 1 else False
    config['transactional.id'] = str(uuid.uuid4())
    return config

def get_default_kafka_consumer_configs():
    dirpath = os.path.dirname(os.path.realpath(__file__))
    cfg_filepath = os.path.join(dirpath, 'kafka.cfg')
    cfg = configparser.ConfigParser()
    cfg.read(cfg_filepath)
    
    config = dict()
    for key, value in cfg['consumer'].items():
        if isinstance(value, str):
            if value.isnumeric():
                value = int(value)
        key = key.replace('_', '.')
        config[key] = value
        
    return config


###################################################### logger

def get_basic_logger():
    logger = logging.getLogger()
#     logger_format = '[%(levelname)s] [%(asctime)s] [%(funcName)15s():%(lineno)d] %(message)s'
    logger_format = '[%(levelname)s] [%(asctime)s] [%(module)s:%(funcName)s():%(lineno)d] %(message)s'
    logging.basicConfig(format=logger_format)
    logger.setLevel(logging.INFO)
    return logger