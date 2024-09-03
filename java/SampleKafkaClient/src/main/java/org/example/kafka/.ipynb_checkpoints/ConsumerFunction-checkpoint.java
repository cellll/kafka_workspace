package org.example.kafka;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.javatuples.Tuple;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * org.apache.kafka.clients.consumer.KafkaConsumer function
 * @version 0.1
 */

public class ConsumerFunction {
    /**
     * rg.apache.kafka.clients.consumer.KafkaConsumer
     * @see #ConsumerFunction(String brokers)
     */
    private KafkaConsumer<String, String> consumer;

    public ConsumerFunction(String brokers) {
        /**
         * Kafka broker 연결, Consumer config (properties)
         */

        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("auto.offset.reset", "latest");
        prop.put("group.id", "cgroup-1");

        this.consumer = new KafkaConsumer<String,String>(prop);
    }

    public void setSubscribe(String topic){
        /**
         * Consume topic 지정
         * partition 구분 없이 Topic 전체 consume 시 사용
         *
         * @param String topic : topic name
         */
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void setAssign(List<Pair<String, Integer>> topicPartitionPairList) {
        /**
         * Consume topic, partition 할당
         * 특정 topic의 특정 partition consume 시 사용
         * List<Pair<>>로 여러 쌍의 topic, partition 할당 가능
         *
         * @param List<Pair<String, Integer>> : 할당할 Topic, Partition 쌍 (Pair) List
         */

        List<TopicPartition> partitionList = new ArrayList<>();
        for (Pair<String, Integer> p : topicPartitionPairList) {
            partitionList.add(new TopicPartition(p.getKey(), p.getValue()));
        }
        if (partitionList.size() > 0) {
            this.consumer.assign(partitionList);
        }
    }

    public ConsumerRecords<String, String> getMessage(int second){
        /**
         * Consume messsage
         *
         * @param int second : poll second (ex. 1이면 1초간 메시지 수신 대기, 대기 중 수신한 메시지 return)
         * @return ConsumerRecords<String, String> records : 수신한 1개 메시지(ConsumerRecord)의 모음 (ConsumerRecords)
         */
        ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofSeconds(second));
        return records;
    }
}
