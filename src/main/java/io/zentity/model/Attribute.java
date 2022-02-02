/*
 * zentity
 * Copyright © 2018-2022 Dave Moore
 * https://zentity.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zentity.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Attribute {

    public static final Set<String> VALID_TYPES = new TreeSet<>(
            Arrays.asList("boolean", "date", "number", "string")
    );

    private final String name;
    private String[] nameFields;
    private Map<String, String> params = new TreeMap<>();
    private Double score;
    private String type = "string";
    private boolean validateRunnable = false;

    public Attribute(String name, JsonNode json) throws ValidationException, JsonProcessingException {
        validateName(name);
        this.name = name;
        this.nameFields = this.parseNameFields(name);
        this.deserialize(json);
    }

    public Attribute(String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.nameFields = this.parseNameFields(name);
        this.deserialize(json);
    }

    public Attribute(String name, JsonNode json, boolean validateRunnable) throws ValidationException, JsonProcessingException {
        validateName(name);
        this.name = name;
        this.nameFields = this.parseNameFields(name);
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public Attribute(String name, String json, boolean validateRunnable) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.nameFields = this.parseNameFields(name);
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public String name() {
        return this.name;
    }

    public String[] nameFields() {
        return this.nameFields;
    }

    public Map<String, String> params() {
        return this.params;
    }

    public Double score() {
        return this.score;
    }

    public String type() {
        return this.type;
    }

    public void score(JsonNode value) throws ValidationException {
        validateScore(value);
        this.score = value.doubleValue();
    }

    public void type(JsonNode value) throws ValidationException {
        validateType(value);
        this.type = value.textValue();
    }

    /**
     *
     * @param name The name of the attribute.
     * @return
     */
    private String[] parseNameFields(String name) throws ValidationException {
        String[] nameFields = Patterns.PERIOD.split(name, -1);
        this.validateNameFields(nameFields);
        return nameFields;
    }

    private void validateName(String value) throws ValidationException {
        Model.validateStrictName(value);
    }

    private void validateNameFields(String[] nameFields) throws ValidationException {
        for (String nameField : nameFields)
            Model.validateStrictName(nameField);
    }

    private void validateScore(JsonNode value) throws ValidationException {
        String errorMessage = "'attributes." + this.name + ".score' must be a floating point number in the range of 0.0 - 1.0. Integer values of 0 or 1 are acceptable.";
        if (!value.isNull() && !value.isNumber())
            throw new ValidationException(errorMessage);
        if (value.isNumber() && (value.floatValue() < 0.0 || value.floatValue() > 1.0))
            throw new ValidationException(errorMessage);
    }

    /**
     * Validate the value of "attributes".ATTRIBUTE_NAME."type".
     * Must be a non-empty string containing a recognized type.
     *
     * @param value The value of "attributes".ATTRIBUTE_NAME."type".
     * @throws ValidationException
     */
    private void validateType(JsonNode value) throws ValidationException {
        if (!value.isTextual())
            throw new ValidationException("'attributes." + this.name + ".type' must be a string.");
        if (Patterns.EMPTY_STRING.matcher(value.textValue()).matches())
            throw new ValidationException("'attributes." + this.name + ".type'' must not be empty.");
        if (!VALID_TYPES.contains(value.textValue()))
            throw new ValidationException("'attributes." + this.name + ".type' has an unrecognized type '" + value.textValue() + "'.");
    }

    /**
     * Validate the value of "attributes".ATTRIBUTE_NAME."params".
     * Must be an object.
     *
     * @param value The value of "attributes".ATTRIBUTE_NAME."params".
     * @throws ValidationException
     */
    private void validateParams(JsonNode value) throws ValidationException {
        if (!value.isObject())
            throw new ValidationException("'attributes." + this.name + ".params' must be an object.");
    }

    /**
     * Validate the value of "attributes".ATTRIBUTE_NAME.
     * Must be an object.
     *
     * @param object The value of "attributes".ATTRIBUTE_NAME.
     * @throws ValidationException
     */
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
     * @throws JsonProcessingException
     */
    public void deserialize(JsonNode json) throws ValidationException, JsonProcessingException {
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
                case "params":
                    // Set any params that were specified in the input, with the values serialized as strings.
                    if (!value.isObject())
                        throw new ValidationException("'attributes." + this.name + ".params' must be an object.");
                    Iterator<Map.Entry<String, JsonNode>> paramsNode = value.fields();
                    while (paramsNode.hasNext()) {
                        Map.Entry<String, JsonNode> paramNode = paramsNode.next();
                        String paramField = paramNode.getKey();
                        JsonNode paramValue = paramNode.getValue();
                        if (paramValue.isObject() || paramValue.isArray())
                            this.params().put(paramField, Json.MAPPER.writeValueAsString(paramValue));
                        else if (paramValue.isNull())
                            this.params().put(paramField, "null");
                        else
                            this.params().put(paramField, paramValue.asText());
                    }
                    break;
                case "score":
                    this.score(value);
                    break;
                default:
                    throw new ValidationException("'attributes." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
