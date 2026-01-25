package com.example.operator

class AnimalList {
    companion object {
        fun of(list1: AnimalList, list2: AnimalList): AnimalList {
            return (list1 + list2).display()
        }
    }

    operator fun plus(other: AnimalList): AnimalList {
        return AnimalList()
    }

    fun display() = Unit
}