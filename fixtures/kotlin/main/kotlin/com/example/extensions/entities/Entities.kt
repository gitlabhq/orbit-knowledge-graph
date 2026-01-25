package com.example.extensions.entities

data class ExtendMe(val value: String)

data class ExtendMeFromProperty(val value: String, val wrapper: String) {
    fun printValue() {
        println(wrapper)
        println(value)
        println(wrapper)
    }
}