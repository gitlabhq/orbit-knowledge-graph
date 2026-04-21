package com.example.weather.formatter;

import com.example.weather.model.Forecast;

import java.util.Locale;

public class Formatter {
    public enum Format { TEXT, JSON }

    private final Format format;

    public Formatter(Format format) { this.format = format; }

    public String render(Forecast f) {
        return switch (format) {
            case TEXT -> renderText(f);
            case JSON -> renderJson(f);
        };
    }

    private String renderText(Forecast f) {
        return String.format(Locale.ROOT,
            "City:        %s%nCondition:   %s%nTemperature: %.1f C / %.1f F%nHumidity:    %d%%",
            f.city(), f.condition(), f.temperatureC(), f.temperatureF(), f.humidity());
    }

    private String renderJson(Forecast f) {
        return String.format(Locale.ROOT,
            "{\"city\":\"%s\",\"temperature_c\":%.1f,\"temperature_f\":%.1f,\"condition\":\"%s\",\"humidity\":%d}",
            f.city(), f.temperatureC(), f.temperatureF(), f.condition(), f.humidity());
    }
}
