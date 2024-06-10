/*
 * zentity
 * Copyright © 2018-2024 Dave Moore
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
package io.zentity.model.relation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.model.ValidationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.zentity.model.Validation.validateStrictName;

public class Model {

    public static final Set<String> REQUIRED_FIELDS = new TreeSet<>(
            Arrays.asList("index", "a", "b")
    );
    public static final Set<String> VALID_RELATION_DIRECTIONS = new TreeSet<>(
            Arrays.asList("a>b", "a<b", "a<>b", "")
    );

    private String index;
    private String type;
    private String direction;
    private String a;
    private String b;

    public Model(JsonNode json) throws ValidationException, JsonProcessingException {
        this.deserialize(json);
    }

    public Model(String json) throws ValidationException, IOException {
        this.deserialize(json);
    }

    public String index() { return this.index; }

    public String type() { return this.type; }

    public String direction() { return this.direction; }

    public String a() { return this.a; }

    public String b() { return this.b; }

    private void index(String name) throws ValidationException {
        validateStrictName(name);
        this.index = name;
    }

    private void type(String type) throws ValidationException {
        validateStrictName(type);
        this.type = type;
    }

    private void direction(String direction) throws ValidationException {
        this.direction = normalizeDirection(direction);
    }

    private void a(String entityType) throws ValidationException {
        validateStrictName(entityType);
        this.a = entityType;
    }

    private void b(String entityType) throws ValidationException {
        validateStrictName(entityType);
        this.b = entityType;
    }

    /**
     * Validate a top-level field of the relation model.
     *
     * @param json  JSON object.
     * @param field Field name.
     * @throws ValidationException
     */
    private void validateField(JsonNode json, String field) throws ValidationException {
        JsonNode value = json.get(field);
        if (REQUIRED_FIELDS.contains(field) && !value.isTextual())
            throw new ValidationException("'" + field + "' must be a string.");
        else if (!value.isTextual() && !value.isNull())
            throw new ValidationException("'" + field + "' must be a string, null, or omitted.");
    }

    /**
     * Validate that a given direction value is one of the allowed values.
     *
     * @param direction The direction value.
     * @throws ValidationException
     */
    public static void validateDirection(String direction) throws ValidationException {
        if (!VALID_RELATION_DIRECTIONS.contains(direction))
            throw new ValidationException("A relation direction must be one of: \"a>b\", \"a<b\", \"a<>b\"");
    }

    /**
     * Normalize a given direction to the allowed characters of a direction value,
     * then validate the normalized direction value.
     *
     * @param direction An input value for a direction.
     * @return The normalized value for the direction.
     * @throws ValidationException
     */
    public static String normalizeDirection(String direction) throws ValidationException {
        String normalized = direction.toLowerCase().replaceAll("[^ab<>]", "");
        validateDirection(normalized);
        return normalized;
    }

    public void deserialize(JsonNode json) throws ValidationException {
        if (!json.isObject())
            throw new ValidationException("Relation model must be an object.");

        // Validate the existence of required fields.
        for (String field : REQUIRED_FIELDS)
            if (!json.has(field))
                throw new ValidationException("Relation model is missing required field '" + field + "'.");

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            validateField(json, fieldName);
            JsonNode value = field.getValue();
            switch (fieldName) {
                case "index":
                    this.index(value.asText());
                    break;
                case "type":
                    if (!value.isNull() && !value.asText().equals(""))
                        this.type(value.asText());
                    break;
                case "direction":
                    if (!value.isNull() && !value.asText().equals(""))
                        this.direction(value.asText());
                    break;
                case "a":
                    this.a(value.asText());
                    break;
                case "b":
                    this.b(value.asText());
                    break;
                default:
                    throw new ValidationException("'" + fieldName + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
