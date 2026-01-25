package com.example.extensions

import com.example.extensions.utils.*
import com.example.extensions.entities.ExtendMe
import com.example.extensions.entities.ExtendMeFromProperty

fun ExtendMe.printValue() {
    println(value)
}

val ExtendMe.extend
    get() = ExtendMeFromProperty(value, "<wrapper>")

fun callToExtensions() {
    val extendMe = ExtendMe("Hello")

    extendMe.printValue()
    extendMe.extend.printValue()
}

fun callToImportedExtensions() {
    val extendMe = ExtendMe("Hello")
 
    extendMe.reverse()
    extendMe.reversed.printValue()
}