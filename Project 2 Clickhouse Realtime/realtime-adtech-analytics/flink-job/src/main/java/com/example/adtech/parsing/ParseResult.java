package com.example.adtech.parsing;

import com.example.adtech.model.AdEvent;

/** Either a valid {@link AdEvent} or a categorised error. */
public final class ParseResult {

    private final AdEvent event;
    private final String errorType;
    private final String errorMessage;

    private ParseResult(AdEvent event, String errorType, String errorMessage) {
        this.event = event;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
    }

    public static ParseResult success(AdEvent event) {
        return new ParseResult(event, null, null);
    }

    public static ParseResult failure(String errorType, String errorMessage) {
        return new ParseResult(null, errorType, errorMessage);
    }

    public boolean isValid() { return event != null; }
    public AdEvent getEvent() { return event; }
    public String getErrorType() { return errorType; }
    public String getErrorMessage() { return errorMessage; }
}
