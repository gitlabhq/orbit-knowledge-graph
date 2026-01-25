package com.example.entites

interface Person {
    fun getName(): String
}

class User : Person {
    override fun getName(): String {
        return "User"
    }
}

class Admin : Person {
    override fun getName(): String {
        return "Admin"
    }
}