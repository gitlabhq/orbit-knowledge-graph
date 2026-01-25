package com.example.extensions.imported

import external.ExternalType

fun ExternalType.print() {
    println(this.value)
}

fun callToImported() {
    val externalType = ExternalType("Hello")
    externalType.print()
}