package com.myapp.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myapp.kafka.model.WeatherData;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class WeatherDataProducerApp {

    public static void main(String[] argv) throws Exception {

        String topicName = "weatherTopic";
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");

        Producer producer = new KafkaProducer(configProperties);

        //SimpleModule module = new SimpleModule("WeatherDataDeserializer", new Version(3, 1, 8, null, null, null));
        //module.addDeserializer(WeatherData.class, new WeatherDataDeserializer(WeatherData.class));

        ObjectMapper objectMapper = new ObjectMapper();
        //objectMapper.registerModule(module);

        File myfile = new File("/home/praveen/IdeaProjects/weather-data-producer/src/main/resources/weather.json");

        List<WeatherData> WeatherDataList = Arrays.asList(objectMapper.readValue(myfile, WeatherData[].class));
        WeatherDataList.forEach(System.out::println);
        //WeatherData weatherData = objectMapper.readValue(myfile, WeatherData.class);

        for (WeatherData r : WeatherDataList) {
            System.out.println("Current data being read is " + r);
            JsonNode jsonNode = objectMapper.valueToTree(r);
            ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(topicName, jsonNode);
            System.out.println("Current record is " + record);
            producer.send(record);
            //Future<RecordMetadata> ft = producer.send(record);
            //System.out.println(ft.get());
        }
    }
}
