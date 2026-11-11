package com.example.adtech.sink;

import java.io.IOException;

/** Non-200 response from ClickHouse's HTTP interface. */
public class ClickHouseWriteException extends IOException {

    private static final long serialVersionUID = 1L;

    private final int statusCode;

    public ClickHouseWriteException(int statusCode, String replica, String body) {
        super("ClickHouse " + replica + " returned HTTP " + statusCode + ": " + body);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    /** Server errors, timeouts and throttling are retried (on another replica); client errors are not. */
    public boolean isRetryable() {
        return statusCode >= 500 || statusCode == 408 || statusCode == 429;
    }
}
