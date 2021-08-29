package com.myapp.kafka.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.myapp.kafka.model.WeatherData;

import java.io.IOException;

public class WeatherDataDeserializer extends StdDeserializer<WeatherData> {

    public WeatherDataDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public WeatherData deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
        WeatherData weatherData = new WeatherData();
        //while (!parser.isClosed()) {
            JsonToken jsonToken = parser.currentToken();

            System.out.println(parser.getCurrentName());
            if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                String fieldName = parser.getCurrentName();
                System.out.println(fieldName);

                jsonToken = parser.nextToken();

                if ("weather_data.date_time".equals(fieldName)) {
                    weatherData.setDate_time(parser.getValueAsString());
                } else if ("weather_data.city".equals(fieldName)) {
                    weatherData.setCity(parser.getValueAsString());
                } else if ("weather_data.currently_apparenttemperature".equals(fieldName)) {
                    weatherData.setcurrently_apparenttemperature(parser.getFloatValue());
                } else if ("weather_data.currently_humidity".equals(fieldName)) {
                    weatherData.setcurrently_humidity(parser.getFloatValue());
                } else if ("weather_data.currently_precipintensity".equals(fieldName)) {
                    weatherData.setcurrently_precipintensity(parser.getFloatValue());
                } else if ("weather_data.currently_precipprobability".equals(fieldName)) {
                    weatherData.setcurrently_precipprobability(parser.getFloatValue());
                } else if ("weather_data.currently_preciptype".equals(fieldName)) {
                    weatherData.setcurrently_preciptype(parser.getValueAsString());
                } else if ("weather_data.currently_temperature".equals(fieldName)) {
                    weatherData.setcurrently_temperature(parser.getFloatValue());
                } else if ("weather_data.currently_visibility".equals(fieldName)) {
                    weatherData.setcurrently_visibility(parser.getFloatValue());
                } else if ("weather_data.currently_windspeed".equals(fieldName)) {
                    weatherData.setcurrently_windspeed(parser.getFloatValue());
                } else {
                    System.out.println("Error in deserializing bytes ");
                }

           // }

        }
        System.out.println("from deser" + weatherData);
        return weatherData;
    }
}
