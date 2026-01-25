package com.example.foo

import com.example.foo.Bar

class Foo : Bar() {
    companion object {
        fun companionFoo() {}
    }

    fun fooInFooBody() {}

    fun foo() {
        super.bar()
        fooInFooBody()
    }

    inner class InnerFoo {
        fun innerFoo() {
            fooInFooBody()
        }
    }
}