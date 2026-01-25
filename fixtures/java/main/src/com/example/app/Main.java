package com.example.app;

import com.example.util.Outer;
import com.example.helpers.*;
import java.util.ArrayList;
import java.util.List;

public class Main extends Application {
    Foo myParameter;

    public Main() {
        myParameter = new Foo();
    }

    @Traceable
    public void main() {
        if (this.myParameter.bar() instanceof Bar bar) {
            bar.baz();
        }

        myParameter.executor.execute(Executor::executeFn);
        await(() -> super.run());

        // Cross-package usage with wildcard import
        Outer o = Outer.make();
        o.outerMethod();

        // Nested class usage
        Outer.Inner inner = new Outer.Inner();
        inner.innerMethod();
        Outer.Inner.innerStatic();

        // Another class in util package
        Helpers.doHelp();

        // Enum class usage
        EnumClass.ENUM_VALUE_1.enumMethod1();
        
        var enumValue = EnumClass.ENUM_VALUE_2;
        enumValue.enumMethod2();

        var list = new ArrayList<String>();
        var list2 = List.of("a", "b", "c");
    }

    public void await(Runnable fn) {
        fn.run();
    }
}


