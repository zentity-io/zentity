package io.zentity.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class Attribute {

    public static final Set<String> VALID_TYPES = new HashSet<>(
            Arrays.asList("string", "long", "integer", "short", "byte", "double", "float", "boolean")
    );

    private final String name;
    private String type = "string";

    public Attribute(String name, JsonNode json) throws ValidationException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public Attribute(String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public String name() {
        return this.name;
    }

    public String type() {
        return this.type;
    }

    public void type(JsonNode value) throws ValidationException {
        validateType(value);
        this.type = value.textValue();
    }

    private void validateName(String value) throws ValidationException {
        if (value.matches("^\\s*$"))
            throw new ValidationException("'attributes' has an attribute with empty name.");
    }

    private void validateType(JsonNode value) throws ValidationException {
        if (!value.isTextual())
            throw new ValidationException("'attributes." + this.name + ".type' must be a string.");
        if (!VALID_TYPES.contains(value.textValue()))
            throw new ValidationException("'attributes." + this.name + ".type' does not support the value '" + value.textValue() + "'.");
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'attributes." + this.name + "' must be an object.");
    }

    /**
     * Deserialize, validate, and hold the state of an attribute object of an entity model.
     * Expected structure of the json variable:
     * <pre>
     * {
     *   "type": ATTRIBUTE_TYPE
     * }
     * </pre>
     *
     * @param json Attribute object of an entity model.
     * @throws ValidationException
     */
    public void deserialize(JsonNode json) throws ValidationException {
        validateObject(json);

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();
            switch (name) {
                case "type":
                    this.type(value);
                    break;
                default:
                    throw new ValidationException("'attributes." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(new ObjectMapper().readTree(json));
    }

}
