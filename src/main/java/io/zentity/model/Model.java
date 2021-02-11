package io.zentity.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;

import java.io.IOException;
import java.util.*;

public class Model {

    public static final Set<String> REQUIRED_FIELDS = new TreeSet<>(
            Arrays.asList("attributes", "resolvers", "matchers", "indices")
    );

    private Map<String, Attribute> attributes = new TreeMap<>();
    private Map<String, Index> indices = new TreeMap<>();
    private Map<String, Matcher> matchers = new TreeMap<>();
    private Map<String, Resolver> resolvers = new TreeMap<>();
    private boolean validateRunnable = false;

    public Model(JsonNode json) throws ValidationException, JsonProcessingException {
        this.deserialize(json);
    }

    public Model(String json) throws ValidationException, IOException {
        this.deserialize(json);
    }

    public Model(JsonNode json, boolean validateRunnable) throws ValidationException, JsonProcessingException {
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public Model(String json, boolean validateRunnable) throws ValidationException, IOException {
        this.validateRunnable = validateRunnable;
        this.deserialize(json);
    }

    public Map<String, Attribute> attributes() {
        return this.attributes;
    }

    public Map<String, Index> indices() {
        return this.indices;
    }

    public Map<String, Matcher> matchers() {
        return this.matchers;
    }

    public Map<String, Resolver> resolvers() {
        return this.resolvers;
    }

    /**
     * Validate a top-level field of the entity model.
     *
     * @param json  JSON object.
     * @param field Field name.
     * @throws ValidationException
     */
    private void validateField(JsonNode json, String field) throws ValidationException {
        if (!json.get(field).isObject())
            throw new ValidationException("'" + field + "' must be an object.");
        if (this.validateRunnable) {
            if (json.get(field).size() == 0) {
                // Clarifying "in the entity model" because this exception likely will appear only for resolution requests,
                // and the user might think that the message is referring to the input instead of the entity model.
                throw new ValidationException("'" + field + "' must not be empty in the entity model.");
            }
        }
    }

    /**
     * Validate the object of a top-level field of the entity model.
     *
     * @param field  Field name.
     * @param object JSON object.
     * @throws ValidationException
     */
    private void validateObject(String field, JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'" + field + "' must be an object.");
        if (this.validateRunnable) {
            if (object.size() == 0) {
                // Clarifying "in the entity model" because this exception likely will appear only for resolution requests,
                // and the user might think that the message is referring to the input instead of the entity model.
                throw new ValidationException("'" + field + "' must not be empty in the entity model.");
            }
        }

    }

    public void deserialize(JsonNode json) throws ValidationException, JsonProcessingException {
        if (!json.isObject())
            throw new ValidationException("Entity model must be an object.");

        // Validate the existence of required fields.
        for (String field : REQUIRED_FIELDS)
            if (!json.has(field))
                throw new ValidationException("Entity model is missing required field '" + field + "'.");

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldObject = field.getValue();
            validateField(json, fieldName);
            validateObject(fieldName, fieldObject);
            Iterator<Map.Entry<String, JsonNode>> children = json.get(fieldName).fields();
            while (children.hasNext()) {
                Map.Entry<String, JsonNode> child = children.next();
                String name = child.getKey();
                JsonNode object = child.getValue();
                switch (fieldName) {
                    case "attributes":
                        this.attributes.put(name, new Attribute(name, object, this.validateRunnable));
                        break;
                    case "indices":
                        this.indices.put(name, new Index(name, object, this.validateRunnable));
                        break;
                    case "matchers":
                        this.matchers.put(name, new Matcher(name, object, this.validateRunnable));
                        break;
                    case "resolvers":
                        this.resolvers.put(name, new Resolver(name, object, this.validateRunnable));
                        break;
                    default:
                        throw new ValidationException("'" + fieldName + "' is not a recognized field.");
                }
            }

        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
