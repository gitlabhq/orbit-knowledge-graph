package com.example.extensions.utils

import com.example.extensions.entities.ExtendMe

fun ExtendMe.reverse() = ExtendMe(value.reversed())

val ExtendMe.reversed
    get() = ExtendMe(value.reversed())