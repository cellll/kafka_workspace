package org.example.kafka;

import org.apache.kafka.clients.admin.*;
import org.javatuples.Triplet;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * org.apache.kafka.clients.admin.AdminClient function
 * @version 0.1
 */

public class AdminClientFunction {
    /**
     * org.apache.kafka.clients.admin.AdminClient
     * @see #AdminClientFunction(String brokers)
     */
    private AdminClient adminClient;

    public AdminClientFunction(String brokers) {
        /**
         * Kafka broker 연결(properties)
         */

        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);
        this.adminClient = AdminClient.create(prop);
    }

    public Set<String> listTopicNames() throws ExecutionException, InterruptedException, TimeoutException {
        /**
         * Broker내 전체 Topic name 조회
         * timeout : default 5sec
         *
         * @return 전체 Topic name
         */

        ListTopicsResult topicsResult = this.adminClient.listTopics();
        Set<String> topicNames = topicsResult.names().get(5, TimeUnit.SECONDS);
        return topicNames;
    }

    public boolean isExistTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
        /**
         * Topic이 존재하는지 체크
         *
         * @param String topic : 조회할 topic name
         * @return 존재 : true, 없음 : false
         */

        Set<String> existTopicNames = this.listTopicNames();
        return (existTopicNames.contains(topic)) ? true : false;
    }

    public boolean createTopic(Collection<Triplet<String, Integer, Short>> topicInfos, int timeout) throws ExecutionException, InterruptedException, TimeoutException {
        /**
         * Topic 생성
         *
         * @param Collection<Triplet<String, Integer, Short>> : String topicName, Integer numPartition, Short replicationFactor로 구성된 Triplet
         * @param int timeout : timeout (seconds)
         * @return 정상 동작 : true, 비정상 동작 : false
         */

        List<NewTopic> newTopics = new ArrayList<>();
        for (Triplet<String, Integer, Short> trp : topicInfos){
//            String topic = trp.getValue0();
//            int numPartition = trp.getValue1();
//            short replicationFactor = trp.getValue2();
            newTopics.add(new NewTopic(trp.getValue0(), trp.getValue1(), trp.getValue2()));
        }
        CreateTopicsResult ctr = this.adminClient.createTopics(newTopics);
        ctr.all().get(timeout, TimeUnit.SECONDS);
        return ctr.all().isDone();
    }

    public TopicDescription describeTopic(String topic) throws ExecutionException, InterruptedException {
        /**
         * Topic 정보 조회
         *
         * @param String topic : topic name
         * @return org.apache.kafka.clients.admin.TopicDescription : topic 정보 담은 TopicDescription instance
         */

        TopicDescription description = null;
        DescribeTopicsResult dtr = this.adminClient.describeTopics(Collections.singletonList(topic));
        Map<String, TopicDescription> tdMap = dtr.allTopicNames().get();
        if (tdMap.keySet().contains(topic)){
            description = tdMap.get(topic);
        }

        return description;
    }

    public boolean deleteTopic(String topic, int timeout) throws ExecutionException, InterruptedException, TimeoutException {
        /**
         * Topic 삭제
         *
         * @param String topic : topic name
         * @param int timeout : timeout (seconds)
         * @return 정상 동작 : true, 비정상 동작 : false
         */

        DeleteTopicsResult dtr = this.adminClient.deleteTopics(Collections.singletonList(topic));
        dtr.all().get(timeout, TimeUnit.SECONDS);
        return dtr.all().isDone();
    }
}
