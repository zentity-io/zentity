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
import java.util.regex.Pattern;

public class Matcher {

    public static final Set<String> REQUIRED_FIELDS = new TreeSet<>(
            Arrays.asList("clause")
    );

    private final String name;
    private String clause;
    private Map<String, String> params = new TreeMap<>();
    private Double quality;
    private boolean validateRunnable = false;
    private Map<String, Pattern> variables = new TreeMap<>();

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

    public Matcher(String name, JsonNode json, boolean validateRunnable) throws ValidationException, JsonProcessingException {
        validateName(name);
        this.name = name;
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public Matcher(String name, String json, boolean validateRunnable) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    /**
     * Extract the names of the variables expressed in a clause as {{ variable }}.
     * These will be used when populating the matcher clause to prevent redundant regular expression replacements.
     *
     * @param clause Clause serialized as a string.
     * @return
     */
    public static Map<String, Pattern> parseVariables(String clause) {
        java.util.regex.Matcher m = Patterns.VARIABLE.matcher(clause);
        Map<String, Pattern> variables = new TreeMap<>();
        while (m.find()) {
            String variable = m.group(1);
            Pattern pattern = Pattern.compile("\\{\\{\\s*(" + Pattern.quote(variable) + ")\\s*}}");
            variables.put(variable, pattern);
        }
        return variables;
    }

    public String name() {
        return this.name;
    }

    public String clause() {
        return this.clause;
    }

    public Map<String, String> params() {
        return this.params;
    }

    public Double quality() {
        return this.quality;
    }

    public Map<String, Pattern> variables() {
        return this.variables;
    }

    public void clause(JsonNode value) throws ValidationException, JsonProcessingException {
        validateClause(value);
        this.clause = Json.MAPPER.writeValueAsString(value);
        this.variables = parseVariables(this.clause);
    }

    public void quality(JsonNode value) throws ValidationException {
        validateQuality(value);
        this.quality = value.doubleValue();
    }

    private void validateName(String value) throws ValidationException {
        Model.validateStrictName(value);
    }

    private void validateClause(JsonNode value) throws ValidationException {
        if (!value.isObject())
            throw new ValidationException("'matchers." + this.name + ".clause' must be an object.");
        if (value.size() == 0)
            throw new ValidationException("'matchers." + this.name + ".clause' must not be empty.");
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'matchers." + this.name + "' must be an object.");
        if (this.validateRunnable) {
            if (object.size() == 0) {
                // Clarifying "in the entity model" because this exception likely will appear only for resolution requests,
                // and the user might think that the message is referring to the input instead of the entity model.
                throw new ValidationException("'matchers." + this.name + "' must not be empty in the entity model.");
            }
        }
    }

    private void validateQuality(JsonNode value) throws ValidationException {
        String errorMessage = "'matchers." + this.name + ".quality' must be a floating point number in the range of 0.0 - 1.0. Integer values of 0 or 1 are acceptable.";
        if (!value.isNull() && !value.isNumber())
            throw new ValidationException(errorMessage);
        if (value.isNumber() && (value.floatValue() < 0.0 || value.floatValue() > 1.0))
            throw new ValidationException(errorMessage);
    }

    /**
     * Deserialize, validate, and hold the state of a matcher object of an entity model.
     * Expected structure of the json variable:
     * <pre>
     * {
     *   "clause": MATCHER_CLAUSE,
     *   "params": MATCHER_PARAMS,
     *   "quality": MATCHER_QUALITY
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
                case "params":
                    // Set any params that were specified in the input, with the values serialized as strings.
                    if (value.isNull())
                        break;
                    if (!value.isObject())
                        throw new ValidationException("'matchers." + this.name + ".params' must be an object.");
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
                case "quality":
                    this.quality(value);
                    break;
                default:
                    throw new ValidationException("'matchers." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
