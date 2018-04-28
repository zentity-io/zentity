package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public abstract class Value implements ValueInterface {

    protected final JsonNode value;
    protected final String serialized;

    /**
     * Validate and hold the object of a value.
     *
     * @param value Attribute value.
     */
    Value(JsonNode value) throws ValidationException {
        this.validate(value);
        this.value = value.isNull() ? null : value;
        this.serialized = this.serialize(value);
    }

    /**
     * Factory method to construct a Value.
     *
     * @param attributeType Attribute type.
     * @param value         Attribute value.
     * @return
     * @throws ValidationException
     */
    public static Value create(String attributeType, JsonNode value) throws ValidationException {
        switch (attributeType) {
            case "boolean":
                return new BooleanValue(value);
            case "number":
                return new NumberValue(value);
            case "string":
                return new StringValue(value);
            default:
                throw new ValidationException("'" + attributeType + " is not a recognized attribute type.");
        }
    }

    @Override
    public abstract String serialize(JsonNode value);

    @Override
    public abstract void validate(JsonNode value) throws ValidationException;

    @Override
    public Object value() {
        return this.value;
    }

    @Override
    public String serialized() {
        return this.serialized;
    }

    public int compareTo(Value o) {
        return this.serialized.compareTo(o.serialized);
    }

    @Override
    public String toString() {
        return this.serialized;
    }

}
