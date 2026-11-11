package com.example.clickstream.model;

import java.io.Serializable;

/**
 * A validated clickstream event.
 *
 * <p>Public fields and a no-arg constructor make this a Flink POJO, so Flink uses its efficient
 * POJO serializer instead of falling back to Kryo.
 */
public class ClickEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public String eventId;
    public String eventType;
    public String userId;
    public String sessionId;
    public String pageUrl;
    public String referrer;
    public String productId;
    public Double price;
    public Integer quantity;
    public String userAgent;
    public String deviceType;
    public String country;
    /** Event time (epoch millis, UTC) as reported by the client. */
    public long eventTime;
    /** Time the event was parsed by Flink (epoch millis, UTC). */
    public long ingestedAt;

    public ClickEvent() {
    }

    public boolean isPurchase() {
        return EventType.PURCHASE.wireName().equals(eventType);
    }

    /** Revenue contributed by this event: price x quantity for purchases, otherwise 0. */
    public double revenue() {
        if (!isPurchase() || price == null) {
            return 0.0;
        }
        return price * (quantity == null ? 1 : quantity);
    }

    @Override
    public String toString() {
        return "ClickEvent{eventId='" + eventId + "', eventType='" + eventType + "', userId='" + userId
                + "', sessionId='" + sessionId + "', pageUrl='" + pageUrl + "', productId='" + productId
                + "', price=" + price + ", quantity=" + quantity + ", deviceType='" + deviceType
                + "', country='" + country + "', eventTime=" + eventTime + '}';
    }
}
