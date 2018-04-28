package io.zentity.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Model {

    private Map<String, Attribute> attributes = new TreeMap<>();
    private Map<String, Index> indices = new TreeMap<>();
    private Map<String, Matcher> matchers = new TreeMap<>();
    private Map<String, Resolver> resolvers = new TreeMap<>();

    public Model(JsonNode json) throws ValidationException, JsonProcessingException {
        this.deserialize(json);
    }

    public Model(String json) throws ValidationException, IOException {
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
        if (json.get(field).size() == 0)
            throw new ValidationException("'" + field + "' must not be empty.");
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
        if (object.size() == 0)
            throw new ValidationException("'" + field + "' is empty.");

    }

    public void deserialize(JsonNode json) throws ValidationException, JsonProcessingException {
        if (!json.isObject())
            throw new ValidationException("Entity model must be an object.");
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
                        this.attributes.put(name, new Attribute(name, object));
                        break;
                    case "indices":
                        this.indices.put(name, new Index(name, object));
                        break;
                    case "matchers":
                        this.matchers.put(name, new Matcher(name, object));
                        break;
                    case "resolvers":
                        this.resolvers.put(name, new Resolver(name, object));
                        break;
                    default:
                        throw new ValidationException("'" + fieldName + "' is not a recognized field.");
                }
            }

        }
        if (this.attributes.size() == 0)
            throw new ValidationException("'attributes' is missing.");
        if (this.resolvers.size() == 0)
            throw new ValidationException("'resolvers' is missing.");
        if (this.matchers.size() == 0)
            throw new ValidationException("'matchers' is missing.");
        if (this.indices.size() == 0)
            throw new ValidationException("'indices' is missing.");
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
