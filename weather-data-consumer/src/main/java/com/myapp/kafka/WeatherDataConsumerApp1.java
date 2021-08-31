package com.myapp.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.myapp.kafka.deserializer.WeatherDataDeserializer;
import com.myapp.kafka.model.WeatherData;
import com.mysql.jdbc.Connection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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
    static final String DB_URL = "jdbc:mysql://localhost/mysql";
    static final String USER = "root";
    static final String PASS = "datadata";
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

        SimpleModule module = new SimpleModule("WeatherDataDeserializer", new Version(3, 1, 8, null, null, null));
        module.addDeserializer(WeatherData.class, new WeatherDataDeserializer(WeatherData.class));


        //Figure out where to start processing messages from
        kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);
        //Start processing messages


        try {
            while (true) {
                ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, JsonNode> record : records) {
                    JsonNode jsonNode = record.value();
                    WeatherData rec = mapper.treeToValue(jsonNode,WeatherData.class);
                    System.out.println(rec);
                    syncDbRecord(rec);
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

    public void syncDbRecord(WeatherData obj){
        try(Connection conn = (Connection) DriverManager.getConnection(DB_URL, USER, PASS);
            Statement stmt = conn.createStatement();
        ) {
            // Execute a query
            System.out.println("Inserting records into the table...");
            String sql = "INSERT INTO WeatherData VALUES ("+
            obj.getDate_time() +", "+ obj.getCity() +", "+obj.getcurrently_apparenttemperature()+","+obj.getcurrently_humidity()+","+ obj.getcurrently_precipintensity()+","+ obj.getcurrently_precipprobability()+","+ obj.getcurrently_preciptype()+","+ obj.getcurrently_temperature()+","+obj.getcurrently_visibility()+","+obj.getcurrently_windspeed()+")";
            stmt.executeUpdate(sql);

            System.out.println("Inserted records into the table...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
}