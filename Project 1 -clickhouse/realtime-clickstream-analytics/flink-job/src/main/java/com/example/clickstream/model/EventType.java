package com.example.clickstream.model;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Supported clickstream event types and their wire names. */
public enum EventType {
    PAGE_VIEW("page_view", false),
    PRODUCT_VIEW("product_view", true),
    ADD_TO_CART("add_to_cart", true),
    REMOVE_FROM_CART("remove_from_cart", true),
    CHECKOUT_START("checkout_start", false),
    PURCHASE("purchase", true);

    private static final Map<String, EventType> BY_WIRE_NAME = Arrays.stream(values())
            .collect(Collectors.toMap(EventType::wireName, Function.identity()));

    private final String wireName;
    private final boolean requiresProduct;

    EventType(String wireName, boolean requiresProduct) {
        this.wireName = wireName;
        this.requiresProduct = requiresProduct;
    }

    public String wireName() {
        return wireName;
    }

    public boolean requiresProduct() {
        return requiresProduct;
    }

    public static Optional<EventType> fromWireName(String name) {
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_WIRE_NAME.get(name.trim().toLowerCase(Locale.ROOT)));
    }
}
