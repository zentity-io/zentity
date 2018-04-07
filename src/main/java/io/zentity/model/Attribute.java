package io.zentity.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class Attribute {

    public static final Set<String> VALID_TYPES = new HashSet<>(
            Arrays.asList("string", "number", "boolean")
    );
    private static final Pattern REGEX_EMPTY = Pattern.compile("^\\s*$");

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

    public static Boolean convertTypeBoolean(JsonNode value) {
        if (value.isNull())
            return null;
        return value.booleanValue();
    }

    public static Number convertTypeNumber(JsonNode value) {
        if (value.isNull())
            return null;
        else if (value.isIntegralNumber())
            return value.bigIntegerValue();
        else if (value.isFloatingPointNumber())
            return value.doubleValue();
        else
            return value.numberValue();
    }

    public static String convertTypeString(JsonNode value) {
        if (value.isNull())
            return null;
        return value.textValue();
    }

    public static Object convertType(String targetType, JsonNode value) throws ValidationException {
        switch (targetType) {
            case "boolean":
                return convertTypeBoolean(value);
            case "number":
                return convertTypeNumber(value);
            case "string":
                return convertTypeString(value);
            default:
                throw new ValidationException("'" + targetType + " is not a recognized attribute data type.");
        }
    }

    public static boolean isTypeBoolean(JsonNode value) {
        return value.isBoolean();
    }

    public static boolean isTypeNumber(JsonNode value) {
        return value.isNumber();
    }

    public static boolean isTypeString(JsonNode value) {
        return value.isTextual();
    }

    public static void validateTypeBoolean(JsonNode value) throws ValidationException {
        if (!isTypeBoolean(value) && !value.isNull())
            throw new ValidationException("Expected 'boolean' attribute data type.");
    }

    public static void validateTypeNumber(JsonNode value) throws ValidationException {
        if (!isTypeNumber(value) && !value.isNull())
            throw new ValidationException("Expected 'number' attribute data type.");
    }

    public static void validateTypeString(JsonNode value) throws ValidationException {
        if (!isTypeString(value) && !value.isNull())
            throw new ValidationException("Expected 'string' attribute data type.");
    }

    public static void validateType(String expectedType, JsonNode value) throws ValidationException {
        switch (expectedType) {
            case "boolean":
                validateTypeBoolean(value);
                break;
            case "number":
                validateTypeNumber(value);
                break;
            case "string":
                validateTypeString(value);
                break;
            default:
                throw new ValidationException("'" + expectedType + " is not a recognized attribute data type.");
        }
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
        if (REGEX_EMPTY.matcher(value).matches())
            throw new ValidationException("'attributes' has an attribute with empty name.");
    }

    private void validateType(JsonNode value) throws ValidationException {
        if (!value.isTextual())
            throw new ValidationException("'attributes." + this.name + ".type' must be a string.");
        if (REGEX_EMPTY.matcher(value.textValue()).matches())
            throw new ValidationException("'attributes." + this.name + ".type'' must not be empty.");
        if (!VALID_TYPES.contains(value.textValue()))
            throw new ValidationException("'attributes." + this.name + ".type' has an unrecognized data type '" + value.textValue() + "'.");
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
