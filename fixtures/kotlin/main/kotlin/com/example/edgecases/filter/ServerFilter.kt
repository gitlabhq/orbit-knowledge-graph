package com.example.edgecases.filter

open class ServerFilter : Filter {
    class Filter : ServerFilter() {
        override fun filter(value: String): Boolean {
            return super.filter(value)
        }
    }

    override fun filter(value: String): Boolean {
        return super.filter(value)
    }
}