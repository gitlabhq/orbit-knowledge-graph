package com.example.try

import com.example.entites.Admin
import com.example.entites.User

data class TryConfig(val isAdmin: Boolean)

fun tryTypeInference(config: TryConfig?) {
    val result = try {
        if (config == null) {
            throw Exception("config is null")
        } else {
            if (config.isAdmin) {
                Admin()
            } else {
                User()
            }
        }
    } catch (e: Exception) {
        e.printStackTrace()
        null
    }

    result?.getName()
}