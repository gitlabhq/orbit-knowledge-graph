package com.example.weather.service;

import com.example.weather.model.Forecast;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeatherService {
    private static final Map<String, Sample> SAMPLES = new LinkedHashMap<>();

    static {
        SAMPLES.put("berlin",        new Sample(14.5, "cloudy",         72));
        SAMPLES.put("tokyo",         new Sample(21.0, "clear",          60));
        SAMPLES.put("san francisco", new Sample(17.2, "foggy",          80));
        SAMPLES.put("new york",      new Sample(9.8,  "rain",           88));
        SAMPLES.put("kyiv",          new Sample(11.4, "partly cloudy",  65));
    }

    private final Map<String, Sample> samples;

    public WeatherService() { this(SAMPLES); }

    public WeatherService(Map<String, Sample> samples) { this.samples = samples; }

    public Forecast fetch(String city) {
        String key = city.toLowerCase().strip();
        Sample sample = samples.get(key);
        if (sample == null) throw new UnknownCityException(city);
        return new Forecast(city, sample.temperatureC(), sample.condition(), sample.humidity());
    }

    public List<String> knownCities() {
        return samples.keySet().stream().map(WeatherService::titleize).collect(Collectors.toList());
    }

    private static String titleize(String key) {
        return Stream.of(key.split(" "))
            .map(part -> Character.toUpperCase(part.charAt(0)) + part.substring(1))
            .collect(Collectors.joining(" "));
    }

    public record Sample(double temperatureC, String condition, int humidity) {}
}
