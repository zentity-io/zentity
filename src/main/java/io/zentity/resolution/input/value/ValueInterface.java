package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public interface ValueInterface extends Comparable<Value> {

    /**
     * Validate the attribute value. Throw an exception on validation error. Pass on success.
     *
     * @param value Attribute value.
     */
    void validate(JsonNode value) throws ValidationException;

    /**
     * Serialize the attribute value from a JsonNode object to a String object.
     */
    String serialize(JsonNode value);

    /**
     * Return the attribute type.
     *
     * @return
     */
    Object type();

    /**
     * Return the attribute value.
     *
     * @return
     */
    Object value();

    /**
     * Return the serialized attribute value.
     *
     * @return
     */
    String serialized();

}
