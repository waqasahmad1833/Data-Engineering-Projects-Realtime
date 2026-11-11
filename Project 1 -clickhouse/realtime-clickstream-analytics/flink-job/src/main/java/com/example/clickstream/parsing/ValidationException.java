package com.example.clickstream.parsing;

/** Raised when an event payload is structurally valid JSON but violates the event contract. */
public class ValidationException extends Exception {

    private static final long serialVersionUID = 1L;

    private final String errorType;

    public ValidationException(String errorType, String message) {
        super(message);
        this.errorType = errorType;
    }

    public String getErrorType() {
        return errorType;
    }
}
