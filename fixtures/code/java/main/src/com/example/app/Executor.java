package com.example.app;

public class Executor {
    public static void executeFn() {}

    public void execute(Runnable runnable) {
        runnable.run();
    }
}


