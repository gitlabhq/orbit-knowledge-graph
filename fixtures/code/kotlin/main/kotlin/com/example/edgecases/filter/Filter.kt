package com.example.edgecases.filter

interface Filter {
    fun filter(value: String): Boolean = true
}