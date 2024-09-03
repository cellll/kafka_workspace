package org.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Map;
import java.util.Properties;

/**
 * org.apache.kafka.clients.producer.KafkaProducer function
 * @version 0.1
 */

public class ProducerFunction {
    /**
     * org.apache.kafka.clients.producer.KafkaProducer
     * @see #ProducerFunction(String brokers)
     */
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;

    public ProducerFunction(String brokers) {
        /**
         * Kafka broker 연결, Producer config (properties)
         */
        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        prop.put("retries", 1);
//        prop.put("request.timeout.ms", 1000);
//        prop.put("linger.ms", 100);
//        prop.put("delivery.timeout.ms", 2000);
//        prop.put("block.on.buffer.full", "true");
        this.producer = new KafkaProducer<String, String>(prop);
        this.mapper = new ObjectMapper();
    }

    private Callback getDefaultCallback(){
        /**
         * Default DeliveryReport callback
         */
        return new Callback() {
            @Override
            public void onCompletion(RecordMetadata m, Exception e) {
                if (e != null){
                    e.printStackTrace();
                } else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                }
            }
        };
    }

    public void send(String topic, String value){
        /**
         * 메시지 전송
         *
         * @param String topic : topic name
         * @param String value : string format message value
         */

        this.producer.send(new ProducerRecord<>(topic, value));
        this.producer.flush();
    }

    public void send(String topic, String value, Callback callback){
        /**
         * 메시지 전송 with callback
         *
         * @param String topic : topic name
         * @param String value : string format message value
         * @param Callback callback : callback
         */
        if (callback == null){
            callback = this.getDefaultCallback();
        }

        this.producer.send(new ProducerRecord<>(topic, value), callback);
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, String value){
        /**
         * Partition 지정, Key, Value 메시지 전송
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param String value : string format message value
         */
        this.producer.send(new ProducerRecord<>(topic, partition, key, value));
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, String value, Callback callback){
        /**
         * Partition 지정, Key, Value 메시지 전송 with callback
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param String value : string format message value
         * @param Callback callback : callback
         */
        if (callback == null){
            callback = this.getDefaultCallback();
        }

        this.producer.send(new ProducerRecord<>(topic, partition, key, value), callback);
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, String value, Map<String, String> headers) {
        /**
         * Partition 지정, Headers, Key, Value 메시지 전송
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param String value : string format message value
         * @param Map<String, String> headers : Map format headers
         */
        RecordHeaders hds = new RecordHeaders();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                hds.add(entry.getKey(), entry.getValue().getBytes());
            }
        }

        this.producer.send(new ProducerRecord<>(topic, partition, key, value, hds));
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, String value, Map<String, String> headers, Callback callback) {
        /**
         * Partition 지정, Headers, Key, Value 메시지 전송 with callback
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param String value : string format message value
         * @param Map<String, String> headers : Map format headers
         */
        if (callback == null){
            callback = this.getDefaultCallback();
        }

        RecordHeaders hds = new RecordHeaders();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                hds.add(entry.getKey(), entry.getValue().getBytes());
            }
        }

        this.producer.send(new ProducerRecord<>(topic, partition, key, value, hds), callback);
        this.producer.flush();
    }

    public void send (String topic, Map<String, String> value) throws JsonProcessingException {
        /**
         * 메시지 전송
         *
         * @param String topic : topic name
         * @param Map<String, String> value : Map format message value
         */

        String data = this.mapper.writeValueAsString(value);

        this.producer.send(new ProducerRecord<>(topic, data));
        this.producer.flush();
    }

    public void send (String topic, Map<String, String> value, Callback callback) throws JsonProcessingException {
        /**
         * 메시지 전송 with callback
         *
         * @param String topic : topic name
         * @param Map<String, String> value : Map format message value
         * @param Callback callback : callback
         *
         */
        if (callback == null){
            callback = this.getDefaultCallback();
        }

        String data = this.mapper.writeValueAsString(value);

        this.producer.send(new ProducerRecord<>(topic, data), callback);
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, Map<String, String> value) throws JsonProcessingException {
        /**
         * Partition 지정, Key, Value 메시지 전송
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param Map<String, String> value : Map format message value
         *
         */


        String data = this.mapper.writeValueAsString(value);

        this.producer.send(new ProducerRecord<>(topic, partition, key, data));
        this.producer.flush();
    }
    public void send (String topic, int partition, String key, Map<String, String> value, Callback callback) throws JsonProcessingException {
        /**
         * Partition 지정, Key, Value 메시지 전송 with callback
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param Map<String, String> value : Map format message value
         * @param Callback callback : callback
         */
        if (callback == null){
            callback = this.getDefaultCallback();
        }

        String data = this.mapper.writeValueAsString(value);

        this.producer.send(new ProducerRecord<>(topic, partition, key, data), callback);
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, Map<String, String> value, Map<String, String> headers) throws JsonProcessingException {
        /**
         * Partition 지정, Headers, Key, Value 메시지 전송
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param Map<String, String> value : Map format message value
         * @param Map<String, String> headers : Map format headers
         *
         */

        RecordHeaders hds = new RecordHeaders();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                hds.add(entry.getKey(), entry.getValue().getBytes());
            }
        }

        String data = this.mapper.writeValueAsString(value);

        this.producer.send(new ProducerRecord<>(topic, partition, key, data, hds));
        this.producer.flush();
    }

    public void send (String topic, int partition, String key, Map<String, String> value, Map<String, String> headers, Callback callback) throws JsonProcessingException {
        /**
         * Partition 지정, Headers, Key, Value 메시지 전송 with callback
         *
         * @param String topic : topic name
         * @param int partition : topic partition index
         * @param String key : string format message key
         * @param Map<String, String> value : Map format message value
         * @param Map<String, String> headers : Map format headers
         * @param Callback callback : callback
         *
         */

        if (callback == null){
            callback = this.getDefaultCallback();
        }

        RecordHeaders hds = new RecordHeaders();
        if (headers != null){
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                hds.add(entry.getKey(), entry.getValue().getBytes());
            }
        }

        String data = this.mapper.writeValueAsString(value);

        this.producer.send(new ProducerRecord<>(topic, partition, key, data, hds), callback);
        this.producer.flush();
    }
}
