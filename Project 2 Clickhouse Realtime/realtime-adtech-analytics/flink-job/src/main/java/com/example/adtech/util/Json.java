package com.example.adtech.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Shared, thread-safe Jackson mapper. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }
}
