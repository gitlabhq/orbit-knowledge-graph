package com.example.util;

public class Outer {
    public static class Inner {
        public void innerMethod() {}
        public static void innerStatic() {}
    }

    public void outerMethod() {}

    public static Outer make() { return new Outer(); }
}


