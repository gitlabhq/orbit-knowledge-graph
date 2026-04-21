package com.example.weather.cli;

import com.example.weather.formatter.Formatter;

import java.io.PrintStream;
import java.util.Arrays;

public class Cli {
    public static class Options {
        public String city;
        public Formatter.Format format = Formatter.Format.TEXT;
        public boolean list;
        public boolean help;
    }

    public Options parse(String[] argv, PrintStream stderr) {
        Options o = new Options();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            switch (a) {
                case "-c", "--city" -> o.city = require(argv, ++i, a, stderr);
                case "-f", "--format" -> o.format = parseFormat(require(argv, ++i, a, stderr));
                case "-l", "--list" -> o.list = true;
                case "-h", "--help" -> o.help = true;
                default -> {
                    stderr.println("error: unknown argument " + a);
                    return null;
                }
            }
        }
        return o;
    }

    public String usage() {
        return "Usage: weather --city CITY [--format text|json]\n"
             + "       weather --list\n"
             + "       weather --help";
    }

    private static String require(String[] argv, int i, String flag, PrintStream stderr) {
        if (i >= argv.length) {
            stderr.println("error: missing value for " + flag);
            throw new IllegalArgumentException(flag);
        }
        return argv[i];
    }

    private static Formatter.Format parseFormat(String s) {
        return switch (s.toLowerCase()) {
            case "text" -> Formatter.Format.TEXT;
            case "json" -> Formatter.Format.JSON;
            default -> throw new IllegalArgumentException("unknown format: " + s
                + " (expected one of " + Arrays.toString(Formatter.Format.values()) + ")");
        };
    }
}
