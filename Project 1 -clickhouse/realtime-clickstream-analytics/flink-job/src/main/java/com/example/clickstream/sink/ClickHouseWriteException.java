package com.example.clickstream.sink;

import java.io.IOException;

/** A non-200 response from the ClickHouse HTTP interface. */
public class ClickHouseWriteException extends IOException {

    private static final long serialVersionUID = 1L;

    private final int statusCode;

    public ClickHouseWriteException(int statusCode, String responseBody) {
        super("ClickHouse returned HTTP " + statusCode + ": " + responseBody);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    /** Server-side and throttling errors are worth retrying; client errors (bad data/schema) are not. */
    public boolean isRetryable() {
        return statusCode >= 500 || statusCode == 408 || statusCode == 429;
    }
}
