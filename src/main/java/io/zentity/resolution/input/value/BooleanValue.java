package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public class BooleanValue extends Value {

    public BooleanValue(JsonNode value) throws ValidationException {
        super(value);
    }

    /**
     * Serialize the attribute value from a JsonNode object to a String object.
     *
     * @return
     */
    @Override
    public String serialize(JsonNode value) {
        if (value.isNull())
            return "null";
        return value.asText();
    }

    /**
     * Validate the "boolean" value.
     *
     * @param value Attribute value.
     * @throws ValidationException
     */
    @Override
    public void validate(JsonNode value) throws ValidationException {
        if (!value.isBoolean() && !value.isNull())
            throw new ValidationException("Expected 'boolean' attribute data type.");
    }

}
