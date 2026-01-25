package com.example.if

import com.example.entites.Admin
import com.example.entites.User

data class IfConfig(val isAdmin: Boolean)

fun ifTypeInference(config: IfConfig) = if (config.isAdmin) Admin() else User()

fun usageOfIfTypeInference(config: IfConfig) {
    ifTypeInference(config).getName()
}