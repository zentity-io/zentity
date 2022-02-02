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
import java.util.TreeMap;
import java.util.TreeSet;

public class Index {

    public static final Set<String> REQUIRED_FIELDS = new TreeSet<>(
            Arrays.asList("fields")
    );

    private final String name;
    private Map<String, IndexField> fields;
    private Map<String, Map<String, IndexField>> attributeIndexFieldsMap = new TreeMap<>();
    private boolean validateRunnable = false;

    public Index(String name, JsonNode json) throws ValidationException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public Index(String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public Index(String name, JsonNode json, boolean validateRunnable) throws ValidationException {
        validateName(name);
        this.name = name;
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public Index(String name, String json, boolean validateRunnable) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public String name() {
        return this.name;
    }

    public Map<String, Map<String, IndexField>> attributeIndexFieldsMap() {
        return this.attributeIndexFieldsMap;
    }

    public Map<String, IndexField> fields() {
        return this.fields;
    }

    public void fields(JsonNode value) throws ValidationException {
        validateFields(value);
        Map<String, IndexField> fields = new TreeMap<>();
        Iterator<Map.Entry<String, JsonNode>> children = value.fields();
        while (children.hasNext()) {
            Map.Entry<String, JsonNode> child = children.next();
            String fieldName = child.getKey();
            JsonNode fieldObject = child.getValue();
            validateField(fieldName, fieldObject);
            fields.put(fieldName, new IndexField(this.name, fieldName, fieldObject));
        }
        this.fields = fields;
        this.rebuildAttributeIndexFieldsMap();
    }

    private void validateName(String value) throws ValidationException {
        if (Patterns.EMPTY_STRING.matcher(value).matches())
            throw new ValidationException("'indices' has an index with an empty name.");
    }

    private void validateField(String fieldName, JsonNode fieldObject) throws ValidationException {
        if (fieldName.equals(""))
            throw new ValidationException("'indices." + this.name + ".fields' has a field with an empty name.");
    }

    private void validateFields(JsonNode value) throws ValidationException {
        if (!value.isObject())
            throw new ValidationException("'indices." + this.name + ".fields' must be an object.");
        if (this.validateRunnable) {
            if (value.size() == 0) {
                // Clarifying "in the entity model" because this exception likely will appear only for resolution requests,
                // and the user might think that the message is referring to the input instead of the entity model.
                throw new ValidationException("'indices." + this.name + ".fields' must not be empty in the entity model.");
            }
        }
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'indices." + this.name + "' must be an object.");
        if (this.validateRunnable) {
            if (object.size() == 0) {
                // Clarifying "in the entity model" because this exception likely will appear only for resolution requests,
                // and the user might think that the message is referring to the input instead of the entity model.
                throw new ValidationException("'indices." + this.name + "' must not be empty in the entity model.");
            }
        }
    }

    /**
     * Create a reverse index of attribute names to index fields for faster lookup of index fields by attributes
     * during a resolution job.
     */
    private void rebuildAttributeIndexFieldsMap() {
        this.attributeIndexFieldsMap = new TreeMap<>();
        for (String indexFieldName : this.fields().keySet()) {
            String attributeName = this.fields().get(indexFieldName).attribute();
            if (!this.attributeIndexFieldsMap.containsKey(attributeName))
                this.attributeIndexFieldsMap.put(attributeName, new TreeMap<>());
            if (!this.attributeIndexFieldsMap.get(attributeName).containsKey(indexFieldName))
                this.attributeIndexFieldsMap.get(attributeName).put(indexFieldName, this.fields.get(indexFieldName));
        }
    }

    /**
     * Deserialize, validate, and hold the state of an index object of an entity model.
     * Expected structure of the json variable:
     * <pre>
     * {
     *   "fields": {
     *     INDEX_FIELD_NAME: INDEX_FIELD_OBJECT
     *     ...
     *   }
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
                throw new ValidationException("'indices." + this.name + "' is missing required field '" + field + "'.");

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();
            switch (name) {
                case "fields":
                    this.fields(value);
                    break;
                default:
                    throw new ValidationException("'indices." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
