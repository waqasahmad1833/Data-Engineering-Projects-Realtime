package com.example.clickstream.parsing;

import com.example.clickstream.model.ClickEvent;

/** Either a valid {@link ClickEvent} or a categorised error. */
public final class ParseResult {

    private final ClickEvent event;
    private final String errorType;
    private final String errorMessage;

    private ParseResult(ClickEvent event, String errorType, String errorMessage) {
        this.event = event;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
    }

    public static ParseResult success(ClickEvent event) {
        return new ParseResult(event, null, null);
    }

    public static ParseResult failure(String errorType, String errorMessage) {
        return new ParseResult(null, errorType, errorMessage);
    }

    public boolean isValid() {
        return event != null;
    }

    public ClickEvent getEvent() {
        return event;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
