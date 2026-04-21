package com.example.weather.service;

public class UnknownCityException extends RuntimeException {
    public UnknownCityException(String city) {
        super("no sample data for \"" + city + "\"");
    }
}
