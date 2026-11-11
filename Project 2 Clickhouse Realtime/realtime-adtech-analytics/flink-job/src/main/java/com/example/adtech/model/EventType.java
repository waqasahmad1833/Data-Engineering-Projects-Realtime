package com.example.adtech.model;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Ad event types in funnel order. */
public enum EventType {
    BID("bid"),
    IMPRESSION("impression"),
    CLICK("click"),
    CONVERSION("conversion");

    private static final Map<String, EventType> BY_WIRE_NAME =
            Arrays.stream(values()).collect(Collectors.toMap(EventType::wireName, Function.identity()));

    private final String wireName;

    EventType(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static Optional<EventType> fromWireName(String name) {
        return name == null ? Optional.empty()
                : Optional.ofNullable(BY_WIRE_NAME.get(name.trim().toLowerCase(Locale.ROOT)));
    }
}
