package com.example

import com.example.foo.Foo
import com.example.nestedclasses.Parent

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.example.enums.Enum

val logger: Logger = LoggerFactory.getLogger(Main::class.java)

fun main() {
    val foo = Foo()
    
    // Resolve simple function call
    foo.foo()

    // Resolve companion object function call
    foo.companionFoo()

    // Resolve parent function call
    foo.baz()

    // Resolve imported type
    logger.info("Hello, World!")

    // Inferred nested type resolution
    val inferedGrandChild = Parent.Child.GrandChild()
    inferedGrandChild.greet()

    // Types nested type resolution
    val typedGrandChild: Parent.GrandChild = Parent.GrandChild()
    typedGrandChild.greet()

    // Enum type resolution
    Enum.ENUM_VALUE_1.enumMethod()

    val enumValue = Enum.ENUM_VALUE_2
    enumValue.enumMethod2()
}