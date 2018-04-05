package io.zentity.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class IndexField {

    public static final Set<String> REQUIRED_FIELDS = new HashSet<>(
            Arrays.asList("attribute")
    );

    private final String index;
    private final String name;
    private String attribute;
    private String matcher = null;

    public IndexField(String index, String name, JsonNode json) throws ValidationException {
        validateName(name);
        this.index = index;
        this.name = name;
        this.deserialize(json);
    }

    public IndexField(String index, String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.index = index;
        this.name = name;
        this.deserialize(json);
    }

    public String index() {
        return this.index;
    }

    public String name() {
        return this.name;
    }

    public String attribute() {
        return this.attribute;
    }

    public void attribute(JsonNode value) throws ValidationException {
        validateAttribute(value);
        this.attribute = value.textValue();
    }

    public String matcher() {
        return this.matcher;
    }

    public void matcher(JsonNode value) throws ValidationException {
        validateMatcher(value);
        this.matcher = value.isTextual() ? value.textValue() : null;
    }

    private void validateName(String value) throws ValidationException {
        if (value.matches("^\\s*$"))
            throw new ValidationException("'indices." + this.index + "' has a field with an empty name.");
    }

    private void validateAttribute(JsonNode value) throws ValidationException {
        if (!value.isTextual())
            throw new ValidationException("'indices." + this.index + "." + this.name + ".attribute' must be a string.");
        if (value.textValue().matches("^\\s*$"))
            throw new ValidationException("'indices." + this.index + "." + this.name + ".attribute' must not be empty.");
    }

    private void validateMatcher(JsonNode value) throws ValidationException {
        if (!value.isTextual() && !value.isNull())
            throw new ValidationException("'indices." + this.index + "." + this.name + ".matcher' must be a string or null.");
        if (value.textValue().matches("^\\s*$"))
            throw new ValidationException("'indices." + this.index + "." + this.name + ".matcher' must not be empty.");
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'indices." + this.index + "." + this.name + "' must be an object.");
        if (object.size() == 0)
            throw new ValidationException("'indices." + this.index + "." + this.name + "' is empty.");
    }

    /**
     * Deserialize, validate, and hold the state of a field object of an index field object of an entity model.
     * Expected structure of the json variable:
     * <pre>
     * {
     *   "attribute": ATTRIBUTE_NAME,
     *   "matcher": MATCHER_NAME
     * }
     * </pre>
     *
     * @param json Index object of an entity model.
     * @throws ValidationException
     */
    public void deserialize(JsonNode json) throws ValidationException {
        validateObject(json);

        // Validate the existence of required fields.
        for (String field : REQUIRED_FIELDS)
            if (!json.has(field))
                throw new ValidationException("'indices." + this.index + "." + this.name + "' is missing required field '" + field + "'.");

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();
            switch (name) {
                case "attribute":
                    this.attribute(value);
                    break;
                case "matcher":
                    this.matcher(value);
                    break;
                default:
                    throw new ValidationException("'indices." + this.index + "." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(new ObjectMapper().readTree(json));
    }

}
