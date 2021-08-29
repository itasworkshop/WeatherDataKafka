package com.myapp.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myapp.kafka.model.WeatherData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class WeatherDataConsumerApp {
    private static Scanner in;

        public static void main(String[] argv)throws Exception{


            ConsumerThread consumerRunnable = new ConsumerThread("weatherTopic","weatherGroupId");
            consumerRunnable.start();

            //consumerRunnable.getKafkaConsumer().wakeup();
            //System.out.println("Stopping consumer .....");
            //consumerRunnable.join();
        }

private static class ConsumerThread extends Thread{
    private String topicName;
    private String groupId;
    private KafkaConsumer<String,JsonNode> kafkaConsumer;

    public ConsumerThread(String topicName, String groupId){
        this.topicName = topicName;
        this.groupId = groupId;
    }
    public void run() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");


        //Figure out where to start processing messages from
        kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        ObjectMapper mapper = new ObjectMapper();

        //Start processing messages
        try {
            while (true) {
                ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, JsonNode> record : records) {
                    JsonNode jsonNode = record.value();
                    System.out.println(mapper.treeToValue(jsonNode,WeatherData.class));
                }
            }
        }catch(WakeupException ex){
            System.out.println("Exception caught " + ex.getMessage());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } finally{
            kafkaConsumer.close();
            System.out.println("After closing KafkaConsumer");
        }
    }
    public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
        return this.kafkaConsumer;
    }
}
}