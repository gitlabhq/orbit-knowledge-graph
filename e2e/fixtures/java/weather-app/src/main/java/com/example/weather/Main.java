package com.example.weather;

import com.example.weather.cli.Cli;
import com.example.weather.formatter.Formatter;
import com.example.weather.model.Forecast;
import com.example.weather.service.UnknownCityException;
import com.example.weather.service.WeatherService;

public class Main {
    public static void main(String[] args) {
        System.exit(new Main().run(args));
    }

    public int run(String[] args) {
        Cli cli = new Cli();
        Cli.Options opts;
        try {
            opts = cli.parse(args, System.err);
        } catch (IllegalArgumentException e) {
            return 1;
        }
        if (opts == null) return 1;

        if (opts.help) {
            System.out.println(cli.usage());
            return 0;
        }

        WeatherService service = new WeatherService();

        if (opts.list) {
            service.knownCities().forEach(System.out::println);
            return 0;
        }

        if (opts.city == null) {
            System.err.println("error: --city is required");
            System.err.println(cli.usage());
            return 1;
        }

        try {
            Forecast forecast = service.fetch(opts.city);
            System.out.println(new Formatter(opts.format).render(forecast));
            return 0;
        } catch (UnknownCityException e) {
            System.err.println("error: " + e.getMessage());
            return 2;
        }
    }
}
