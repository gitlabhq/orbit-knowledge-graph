package com.example.when

import com.example.entites.Person
import com.example.entites.Admin
import com.example.entites.User

data class Config(val type: String)

fun whenTypeInference(config: Config) {
    val person = when (config.type) {
        "admin" -> Admin()
        else -> User()
    }

    person.getName()
}