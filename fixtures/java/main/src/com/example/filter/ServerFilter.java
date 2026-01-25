package com.example.filter;

public class ServerFilter implements Filter {
    // Inner class named Filter in the same package as Filter
    class Filter extends ServerFilter {
        @Override
        public boolean apply(String input) {
            return super.apply(input);
        }
    }
}
