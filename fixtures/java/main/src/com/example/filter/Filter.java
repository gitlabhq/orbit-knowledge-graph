package com.example.filter;

public interface Filter {
    default boolean apply(String input) {
        return true;
    }
}
