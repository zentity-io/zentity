package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public class StringValue extends Value {

    public final String type = "string";

    public StringValue(JsonNode value) throws ValidationException {
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
        return value.textValue();
    }

    /**
     * Validate the value.
     *
     * @param value Attribute value.
     * @throws ValidationException
     */
    @Override
    public void validate(JsonNode value) throws ValidationException {
        if (!value.isTextual() && !value.isNull())
            throw new ValidationException("Expected '" + this.type + "' attribute data type.");
    }
}
