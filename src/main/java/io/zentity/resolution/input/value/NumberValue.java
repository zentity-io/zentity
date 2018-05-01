package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public class NumberValue extends Value {

    public final String type = "number";

    public NumberValue(JsonNode value) throws ValidationException {
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
        else if (value.isIntegralNumber())
            return value.bigIntegerValue().toString();
        else if (value.isFloatingPointNumber())
            return String.valueOf(value.doubleValue());
        else
            return value.numberValue().toString();
    }

    /**
     * Validate the value.
     *
     * @param value Attribute value.
     * @throws ValidationException
     */
    @Override
    public void validate(JsonNode value) throws ValidationException {
        if (!value.isNumber() && !value.isNull())
            throw new ValidationException("Expected '" + this.type + "' attribute data type.");
    }
}
