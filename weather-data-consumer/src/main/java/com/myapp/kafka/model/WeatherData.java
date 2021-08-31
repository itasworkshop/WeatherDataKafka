package com.myapp.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

//{"weather_data.date_time":"2019-06-04 14:05:13",
// "weather_data.city":"Zurich",
// "weather_data.currently_currently_apparenttemperature":29.09,
// "weather_data.currently_currently_humidity":0.43,
// "weather_data.currently_currently_precipintensity":0.0305,
// "weather_data.currently_currently_precipprobability":0.01,
// "weather_data.currently_currently_preciptype":"rain",
// "weather_data.currently_currently_temperature":29.09,
// "weather_data.currently_currently_visibility":10.12,
// "weather_data.currently_currently_windspeed":1.94},
public class WeatherData {

    @JsonProperty("weather_data.date_time")
    private String date_time;
    @JsonProperty("weather_data.city")
    private String city;
    @JsonProperty("weather_data.currently_apparenttemperature")
    private float currently_apparenttemperature;
    @JsonProperty("weather_data.currently_humidity")
    private float currently_humidity;
    @JsonProperty("weather_data.currently_precipintensity")
    private double currently_precipintensity;
    @JsonProperty("weather_data.currently_precipprobability")
    private float currently_precipprobability;
    @JsonProperty("weather_data.currently_preciptype")
    private String currently_preciptype;
    @JsonProperty("weather_data.currently_temperature")
    private float currently_temperature;
    @JsonProperty("weather_data.currently_visibility")
    private float currently_visibility;
    @JsonProperty("weather_data.currently_windspeed")
    private float currently_windspeed;

    public WeatherData(String date_time, String city, float currently_apparenttemperature, float currently_humidity, double currently_precipintensity, float currently_precipprobability, String currently_preciptype, float currently_temperature, float currently_visibility, float currently_windspeed) {
        this.date_time = date_time;
        this.city = city;
        this.currently_apparenttemperature = currently_apparenttemperature;
        this.currently_humidity = currently_humidity;
        this.currently_precipintensity = currently_precipintensity;
        this.currently_precipprobability = currently_precipprobability;
        this.currently_preciptype = currently_preciptype;
        this.currently_temperature = currently_temperature;
        this.currently_visibility = currently_visibility;
        this.currently_windspeed = currently_windspeed;
    }

    public WeatherData() {

    }

    public String getDate_time() {
        return date_time;
    }

    public void setDate_time(String date_time) {
        this.date_time = date_time;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public float getcurrently_apparenttemperature() {
        return currently_apparenttemperature;
    }

    public void setcurrently_apparenttemperature(float currently_apparenttemperature) {
        this.currently_apparenttemperature = currently_apparenttemperature;
    }

    public float getcurrently_humidity() {
        return currently_humidity;
    }

    public void setcurrently_humidity(float currently_humidity) {
        this.currently_humidity = currently_humidity;
    }

    public double getcurrently_precipintensity() {
        return currently_precipintensity;
    }

    public void setcurrently_precipintensity(double currently_precipintensity) {
        this.currently_precipintensity = currently_precipintensity;
    }

    public float getcurrently_precipprobability() {
        return currently_precipprobability;
    }

    public void setcurrently_precipprobability(float currently_precipprobability) {
        this.currently_precipprobability = currently_precipprobability;
    }

    public String getcurrently_preciptype() {
        return currently_preciptype;
    }

    public void setcurrently_preciptype(String currently_preciptype) {
        this.currently_preciptype = currently_preciptype;
    }

    public float getcurrently_temperature() {
        return currently_temperature;
    }

    public void setcurrently_temperature(float currently_temperature) {
        this.currently_temperature = currently_temperature;
    }

    public float getcurrently_visibility() {
        return currently_visibility;
    }

    public void setcurrently_visibility(float currently_visibility) {
        this.currently_visibility = currently_visibility;
    }

    public float getcurrently_windspeed() {
        return currently_windspeed;
    }

    public void setcurrently_windspeed(float currently_windspeed) {
        this.currently_windspeed = currently_windspeed;
    }

    @Override
    public String toString() {
        return "WeatherData{" +
                "date_time='" + date_time + '\'' +
                ", city='" + city + '\'' +
                ", currently_apparenttemperature=" + currently_apparenttemperature +
                ", currently_humidity=" + currently_humidity +
                ", currently_precipintensity=" + currently_precipintensity +
                ", currently_precipprobability=" + currently_precipprobability +
                ", currently_preciptype='" + currently_preciptype + '\'' +
                ", currently_temperature=" + currently_temperature +
                ", currently_visibility=" + currently_visibility +
                ", currently_windspeed=" + currently_windspeed +
                '}';
    }

    public WeatherData parse() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        WeatherData example = objectMapper.readValue(new File("/home/praveen/IdeaProjects/weather-data-producer/src/main/resources/simple.json"), WeatherData.class);
        System.out.println("read data is "+example);
        return example;
    }

}
