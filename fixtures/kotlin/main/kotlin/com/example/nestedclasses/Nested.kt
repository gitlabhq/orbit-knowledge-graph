package com.example.nestedclasses

class Parent {
    class Child {
        open class GrandChild {
            open fun greet() {
                println("Hello")
            }
        }
    }

    class GrandChild : Child.GrandChild() {
        override fun greet() {
            super.greet()
        }
    }
}