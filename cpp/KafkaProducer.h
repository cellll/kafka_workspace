#ifndef KAFKAPRODUCER_H
#define KAFKAPRODUCER_H

#include <iostream>
#include <string>
#include <csignal>

#include <librdkafka/rdkafkacpp.h>


class XDeliveryReportCb : public RdKafka::DeliveryReportCb {
    public:
        void dr_cb(RdKafka::Message &message);
};


class XProducer {
    private:
        RdKafka::Conf *conf;
        RdKafka::Producer *producer;
        XDeliveryReportCb x_dr_cb;
        std::string topic;
        std::string errstr;

    public:
        XProducer(std::string brokers, std::string topic);
        void print();
        void produce(std::string message);
};

#endif