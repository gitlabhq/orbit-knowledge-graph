package com.example.weather.model;

public record Forecast(String city, double temperatureC, String condition, int humidity) {
    public double temperatureF() {
        return temperatureC * 9.0 / 5.0 + 32.0;
    }
}
