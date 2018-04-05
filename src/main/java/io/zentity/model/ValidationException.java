package io.zentity.model;

public class ValidationException extends Exception {
    ValidationException(String message) {
        super(message);
    }
}