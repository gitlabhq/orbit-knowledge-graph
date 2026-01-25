package com.example.helpers;

public class Helpers {
    public static void doHelp() {}

    abstract class InnerHelpers {
        public void innerDoHelp() {}
    }

    class InnerInnerHelpers extends InnerHelpers {
        public void innerInnerDoHelp() {
            super.innerDoHelp();
        }
    }
}


