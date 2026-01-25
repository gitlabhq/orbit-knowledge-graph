package com.example.edgecases;

public class ResolveTypeFailingOnNestedChild {
    static class Child {
        static abstract class GrandChild {
            void greet() {
                System.out.println("Hello");
            }
        }
    }

    class GrandChild extends Child.GrandChild {
        public GrandChild() {
            super();
        }

        @Override
        void greet() {
            super.greet();
        }
    }
}