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

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class IndexField {

    public static final Set<String> REQUIRED_FIELDS = new TreeSet<>(
            Arrays.asList("attribute")
    );

    private final String index;
    private final String name;
    private String[] path;
    private String attribute;
    private String matcher;
    private Double quality;
    private boolean validateRunnable = false;

    public IndexField(String index, String name, JsonNode json) throws ValidationException {
        validateName(name);
        this.index = index;
        this.name = name;
        this.nameToPath(name);
        this.deserialize(json);
    }

    public IndexField(String index, String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.index = index;
        this.name = name;
        this.nameToPath(name);
        this.deserialize(json);
    }

    public IndexField(String index, String name, JsonNode json, boolean validateRunnable) throws ValidationException {
        validateName(name);
        this.index = index;
        this.name = name;
        this.validateRunnable = validateRunnable;
        this.nameToPath(name);
        this.deserialize(json);
    }

    public IndexField(String index, String name, String json, boolean validateRunnable) throws ValidationException, IOException {
        validateName(name);
        this.index = index;
        this.name = name;
        this.validateRunnable = validateRunnable;
        this.nameToPath(name);
        this.deserialize(json);
    }

    public String index() {
        return this.index;
    }

    public String name() {
        return this.name;
    }

    public String[] path() {
        return this.path;
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
        this.matcher = value.textValue();
    }

    private void nameToPath(String name) {
        this.path = Patterns.PERIOD.split(name);
    }

    public Double quality() {
        return this.quality;
    }

    public void quality(JsonNode value) throws ValidationException {
        validateQuality(value);
        this.quality = value.doubleValue();
    }

    private void validateName(String value) throws ValidationException {
        if (Patterns.EMPTY_STRING.matcher(value).matches())
            throw new ValidationException("'indices." + this.index + "' has a field with an empty name.");
    }

    private void validateAttribute(JsonNode value) throws ValidationException {
        if (!value.isTextual())
            throw new ValidationException("'indices." + this.index + ".fields." + this.name + ".attribute' must be a string.");
        if (Patterns.EMPTY_STRING.matcher(value.textValue()).matches())
            throw new ValidationException("'indices." + this.index + ".fields." + this.name + ".attribute' must not be empty.");
    }

    private void validateMatcher(JsonNode value) throws ValidationException {
        if (!value.isNull() && !value.isTextual())
            throw new ValidationException("'indices." + this.index + "." + this.name + ".matcher' must be a string.");
        if (value.isTextual() && Patterns.EMPTY_STRING.matcher(value.textValue()).matches())
            throw new ValidationException("'indices." + this.index + ".fields." + this.name + ".matcher' must not be empty.");
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'indices." + this.index + ".fields." + this.name + "' must be an object.");
    }

    private void validateQuality(JsonNode value) throws ValidationException {
        String errorMessage = "'indices." + this.index + ".fields." + this.name + ".quality' must be a floating point number in the range of 0.0 - 1.0. Integer values of 0 or 1 are acceptable.";
        if (!value.isNull() && !value.isNumber())
            throw new ValidationException(errorMessage);
        if (value.isNumber() && (value.floatValue() < 0.0 || value.floatValue() > 1.0))
            throw new ValidationException(errorMessage);
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
                throw new ValidationException("'indices." + this.index + ".fields." + this.name + "' is missing required field '" + field + "'.");

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
                case "quality":
                    this.quality(value);
                    break;
                default:
                    throw new ValidationException("'indices." + this.index + ".fields." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
