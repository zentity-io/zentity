package io.zentity.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class Matcher {

    public static final Set<String> REQUIRED_FIELDS = new HashSet<>(
            Arrays.asList("clause")
    );
    public static final Set<String> VALID_TYPES = new HashSet<>(
            Arrays.asList("value")
    );
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String name;
    private String clause;
    private String type = "value";

    public Matcher(String name, JsonNode json) throws ValidationException, JsonProcessingException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public Matcher(String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public String name() {
        return this.name;
    }

    public String clause() {
        return this.clause;
    }

    public void clause(JsonNode value) throws ValidationException, JsonProcessingException {
        validateClause(value);
        this.clause = mapper.writeValueAsString(value);
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
            throw new ValidationException("'matchers' field has a matcher with an empty name.");
    }

    private void validateClause(JsonNode value) throws ValidationException {
        if (!value.isObject())
            throw new ValidationException("'matchers." + this.name + ".clause' must be an object.");
        if (value.size() == 0)
            throw new ValidationException("'matchers." + this.name + ".clause' is empty.");
    }

    private void validateType(JsonNode value) throws ValidationException {
        if (!value.isTextual())
            throw new ValidationException("'matchers." + this.name + ".type' must be a string.");
        if (!VALID_TYPES.contains(value.textValue()))
            throw new ValidationException("'matchers." + this.name + ".type' does not support the value '" + value.textValue() + "'.");
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'matchers." + this.name + "' must be an object.");
        if (object.size() == 0)
            throw new ValidationException("'matchers." + this.name + "' is empty.");
    }

    /**
     * Deserialize, validate, and hold the state of a matcher object of an entity model.
     * Expected structure of the json variable:
     * <pre>
     * {
     *   "clause": MATCHER_CLAUSE,
     *   "type": MATCHER_TYPE
     * }
     * </pre>
     *
     * @param json Matcher object of an entity model.
     * @throws ValidationException
     */
    public void deserialize(JsonNode json) throws ValidationException, JsonProcessingException {
        validateObject(json);

        // Validate the existence of required fields.
        for (String field : REQUIRED_FIELDS)
            if (!json.has(field))
                throw new ValidationException("'matchers." + this.name + "' is missing required field '" + field + "'.");

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();
            switch (name) {
                case "clause":
                    this.clause(value);
                    break;
                case "type":
                    this.type(value);
                    break;
                default:
                    throw new ValidationException("'matchers." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(new ObjectMapper().readTree(json));
    }

}
