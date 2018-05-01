package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public class DateValue extends StringValue {

    public final String type = "date";

    public DateValue(JsonNode value) throws ValidationException {
        super(value);
    }
}
