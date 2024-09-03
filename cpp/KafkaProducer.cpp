#include "KafkaProducer.h"
#include <cstdlib>
#include <cstdio>
#include <cstring>

void XDeliveryReportCb::dr_cb(RdKafka::Message &message){
    if (message.err()){
        std::cerr << "% Message delivery failed : " << message.errstr() << std::endl;
    }
    else {
        std::cerr << "% Message delivered to topic " << message.topic_name() 
        << " [" << message.partition() << "] at offset" 
        << message.offset() << std::endl;

    }   
}


XProducer::XProducer(std::string brokers, std::string topic){
    this->topic = topic;
    this->conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    if (this->conf->set("bootstrap.servers", brokers, this->errstr) != RdKafka::Conf::CONF_OK){
        std::cerr << this->errstr << std::endl;
    }

    // signal(SIGINT, sigterm);
    // signal(SIGTERM, sigterm);

    if (this->conf->set("dr_cb", &this->x_dr_cb, this->errstr) != RdKafka::Conf::CONF_OK){
        std::cerr << this->errstr << std::endl;
    }

    this->producer = RdKafka::Producer::create(this->conf, this->errstr);
    if (!producer){
        std::cerr << "Failed to create producer : " << this->errstr << std::endl;
        exit(1);
    }
    delete this->conf;
}

void XProducer::print(){
    std::cout << this->conf << " " << this->producer << " " << std::endl;
}

void XProducer::produce(std::string message){
    retry:
        RdKafka::ErrorCode err = this->producer->produce(
            this->topic,
            0,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char *>(message.c_str()), message.size(),
            NULL, 0,
            0,
            NULL,
            NULL
        );
        
        if (err != RdKafka::ERR_NO_ERROR){
            std::cerr << "% Failed to produce to topic" << this->topic << " : " << RdKafka::err2str(err) << std::endl;
            if (err == RdKafka::ERR__QUEUE_FULL){
                this->producer->poll(1000);
                goto retry;
            }
        } else {
            std::cerr << "% Enqueued message (" << message.size() << " bytes)" << " for topic " << this->topic << std::endl;
        }
        this->producer->poll(0);
        // this->producer->flush(1000);
}