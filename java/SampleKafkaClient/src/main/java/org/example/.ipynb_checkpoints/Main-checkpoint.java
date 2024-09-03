package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartitionInfo;
import org.example.kafka.AdminClientFunction;
import org.example.kafka.ConsumerFunction;
import org.example.kafka.ProducerFunction;
import org.javatuples.Triplet;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;



public class Main {
    public static void main(String[] args) {

        String brokers = "127.0.0.1:38093";
        String topic = "t1";
        int numPartition = 3;
        short replicationFactor = 2;

        /*
         * AdminClientFunction sample
         */
        AdminClientFunction acf = new AdminClientFunction(brokers);

        try {
            // topic이 존재하는지 체크
            boolean isExist = acf.isExistTopic(topic);

            // topic이 존재하지 않을 때 createTopic
            if (!isExist) {
                Set<Triplet<String, Integer, Short>> topics = new HashSet<>();
                topics.add(new Triplet<>(topic, numPartition, replicationFactor));

                boolean ctr = acf.createTopic(topics, 3);
                System.out.println("Create topic " + topic + " : " + ctr);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }


        try {
            // topic이 존재하는지 체크
            boolean isExist = acf.isExistTopic(topic);
            // topic이 존재할 때 describeTopic
            if (isExist) {
                TopicDescription description = acf.describeTopic(topic);
                if (description != null){
                    // TopicPartitionInfo : topic내 각각 partition에 대한 정보
                    List<TopicPartitionInfo> partInfos = description.partitions();
                    for (TopicPartitionInfo info : partInfos) {
                        System.out.println(info);
                    }
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        try {
            // topic 삭제
//            boolean dtr = acf.deleteTopic(topic, 5);
//            System.out.println("Delele topic " + topic + " : " + dtr);
            System.out.println(acf.listTopicNames());

        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        /*
         * ProducerFunction sample
         */

        ProducerFunction pf = new ProducerFunction(brokers);

        // Map format message
        Map<String, String> msgMap = new HashMap<>(){
            {
                put("A", "B");
                put("C", "D");
            }
        };

        // Map format headers
        Map<String, String> headerMap = new HashMap<>(){
            {
                put("H1", "h2");
                put("H3", "h4");
            }
        };

        // callback 에 null을 전달하면 메시지 전송 후 기본 callback 실행
        pf.send(topic, "ASDF", null);
        try {
            pf.send(topic, msgMap, null);
            pf.send(topic, 0, "key1", msgMap, headerMap, null);
            pf.send(topic, 1, "key1", msgMap, headerMap, null);
            pf.send(topic, 2, "key1", msgMap,  null, null);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }


        /*
         * ConsumerFunction sample
         */

        ConsumerFunction cf = new ConsumerFunction(brokers);

        /*
         * Consumer에 할당할 Topic, partition 정보
         * Pair<String, Integer> : topic name, partition index
         */
        List<Pair<String, Integer>> pairList = new ArrayList<>(){
            {
                add(new MutablePair<>(topic, 0));
                add(new MutablePair<>(topic, 2));
            }
        };
        cf.setAssign(pairList);

        while (true){
            // 메시지 consume
            ConsumerRecords<String, String> records = cf.getMessage(2);

            System.out.println("Topic " + topic + " Records : " + records.count());

            if (!records.isEmpty()){
                for (ConsumerRecord<String, String> record : records){
                    System.out.println("topic : " + record.topic());
                    System.out.println("partition : " + record.partition());
                    System.out.println("offset : " + record.offset());
                    System.out.println("headers : " + record.headers());
                    System.out.println("value : " + record.value());
                }
            }
        }
    }
}