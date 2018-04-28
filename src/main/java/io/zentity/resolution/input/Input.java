package io.zentity.resolution.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.scope.Scope;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Input {

    private Map<String, Attribute> attributes = new TreeMap<>();
    private String entityType;
    private Model model;
    private Scope scope = new Scope();

    public Input(JsonNode json, Model model) throws ValidationException, IOException {
        this.model = model;
        this.deserialize(json);
    }

    public Input(String json, Model model) throws ValidationException, IOException {
        this.model = model;
        this.deserialize(json);
    }

    public Input(JsonNode json, String entityType) throws ValidationException, IOException {
        this.entityType = parseEntityType(entityType);
        this.deserialize(json);
    }

    public Input(String json, String entityType) throws ValidationException, IOException {
        this.entityType = parseEntityType(entityType);
        this.deserialize(json);
    }

    /**
     * Exclude indices from an entity model, while retaining all the others.
     *
     * @param model   The entity model.
     * @param indices Names of indices from "scope.exclude.indices" to exclude in the entity model.
     * @return Updated entity model.
     * @throws ValidationException
     */
    public static Model excludeIndices(Model model, Set<String> indices) throws ValidationException {
        if (!indices.isEmpty()) {
            for (String index : indices) {
                if (index == null || index.equals(""))
                    continue;
                if (!model.indices().containsKey(index))
                    throw new ValidationException("'" + index + "' is not in the 'indices' field of the entity model.");
                model.indices().remove(index);
            }
        }
        return model;
    }

    /**
     * Include indices in an entity model, while excluding all the others.
     *
     * @param model   The entity model.
     * @param indices Names of indices from "scope.include.indices" to include in the entity model.
     * @return Updated entity model.
     * @throws ValidationException
     */
    public static Model includeIndices(Model model, Set<String> indices) throws ValidationException {
        if (!indices.isEmpty()) {
            for (String index : indices) {
                if (index == null || index.equals(""))
                    continue;
                if (!model.indices().containsKey(index))
                    throw new ValidationException("'" + index + "' is not in the 'indices' field of the entity model.");
            }
            model.indices().keySet().retainAll(indices);
        }
        return model;
    }

    /**
     * Exclude resolvers from an entity model, while retaining all the others.
     *
     * @param model     The entity model.
     * @param resolvers Names of resolvers from "scope.exclude.resolvers" to exclude in the entity model.
     * @return Updated entity model.
     * @throws ValidationException
     */
    public static Model excludeResolvers(Model model, Set<String> resolvers) throws ValidationException {
        if (!resolvers.isEmpty()) {
            for (String resolver : resolvers) {
                if (resolver == null || resolver.equals(""))
                    continue;
                if (!model.resolvers().containsKey(resolver))
                    throw new ValidationException("'" + resolver + "' is not in the 'resolvers' field of the entity model.");
                model.resolvers().remove(resolver);
            }
        }
        return model;
    }

    /**
     * Include resolvers in an entity model, while excluding all the others.
     *
     * @param model     The entity model.
     * @param resolvers Names of resolvers from "scope.include.resolvers" to include in the entity model.
     * @return Updated entity model.
     * @throws ValidationException
     */
    public static Model includeResolvers(Model model, Set<String> resolvers) throws ValidationException {
        if (!resolvers.isEmpty()) {
            for (String resolver : resolvers) {
                if (resolver == null || resolver.equals(""))
                    continue;
                if (!model.resolvers().containsKey(resolver))
                    throw new ValidationException("'" + resolver + "' is not in the 'resolvers' field of the entity model.");
            }
            model.resolvers().keySet().retainAll(resolvers);
        }
        return model;
    }

    /**
     * Parse and validate the "attributes" field of the request body.
     *
     * @param requestBody The request body.
     * @param model       The entity model.
     * @return The parsed "attributes" field from the request body.
     * @throws ValidationException
     * @throws JsonProcessingException
     */
    public static Map<String, Attribute> parseAttributes(JsonNode requestBody, Model model) throws ValidationException, JsonProcessingException {
        if (!requestBody.has("attributes"))
            throw new ValidationException("The 'attributes' field is missing from the request body.");
        if (requestBody.get("attributes").size() == 0)
            throw new ValidationException("The 'attributes' field of the request body must not be empty.");
        JsonNode attributes = requestBody.get("attributes");
        Map<String, Attribute> attributesObj = new TreeMap<>();
        Iterator<String> attributeFields = attributes.fieldNames();
        while (attributeFields.hasNext()) {
            String attributeName = attributeFields.next();

            // Validate that the attribute exists in the entity model.
            if (!model.attributes().containsKey(attributeName))
                throw new ValidationException("'attributes." + attributeName + "' is not defined in the entity model.");

            // Parse the attribute values.
            String attributeType = model.attributes().get(attributeName).type();
            attributesObj.put(attributeName, new Attribute(attributeName, attributeType, attributes.get(attributeName)));
        }
        return attributesObj;
    }

    /**
     * Parse and validate the entity model from the 'model' field of the request body.
     *
     * @param requestBody The request body.
     * @return The parsed "model" field from the request body, or an object from ".zentity-models" index.
     * @throws IOException
     * @throws ValidationException
     */
    public static Model parseEntityModel(JsonNode requestBody) throws IOException, ValidationException {
        if (!requestBody.has("model"))
            throw new ValidationException("The 'model' field is missing from the request body while 'entity_type' is undefined.");
        JsonNode model = requestBody.get("model");
        if (!model.isObject())
            throw new ValidationException("Entity model must be an object.");
        return new Model(model.toString());
    }

    /**
     * Parse and validate the entity type.
     *
     * @param entityType
     */
    private String parseEntityType(String entityType) {
        if (entityType == null || Patterns.EMPTY_STRING.matcher(entityType).matches())
            return null;
        return entityType;
    }

    /**
     * Validate a top-level field of the input.
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
     * Validate the object of a top-level field of the input.
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

    public Map<String, Attribute> attributes() {
        return this.attributes;
    }

    public Model model() {
        return this.model;
    }

    public Scope scope() {
        return this.scope;
    }

    public void deserialize(JsonNode json) throws ValidationException, IOException {
        if (!json.isObject())
            throw new ValidationException("Input must be an object.");

        // Parse and validate the "model" field of the request body, or the entity model stored in the index.
        if (this.model == null) {
            if (this.entityType == null || !json.has("model"))
                throw new ValidationException("You must specify either an entity type or an entity model.");
            this.model = parseEntityModel(json.get("model"));
        } else if (this.entityType != null) {
            throw new ValidationException("You must specify either an entity type or an entity model, not both.");
        }

        // Parse and validate the "attributes" field of the request body.
        this.attributes = parseAttributes(json, this.model);

        // Parse and validate the "scope" field of the request body.
        if (json.has("scope")) {
            this.scope.deserialize(json.get("scope"), this.model);

            // Parse and validate the "scope"."include" field of the request body.
            if (this.scope.include() != null) {

                // Remove any resolvers entity model that do not appear in "scope.include.resolvers".
                if (!this.scope.include().resolvers().isEmpty())
                    this.model = includeResolvers(this.model, this.scope.include().resolvers());

                // Remove any indices entity model that do not appear in "scope.include.indices".
                if (!this.scope.include().indices().isEmpty())
                    this.model = includeIndices(this.model, this.scope.include().indices());
            }

            // Parse and validate the "scope"."exclude" field of the request body.
            if (this.scope.exclude() != null) {

                // Intersect the "indices" field of the entity model with "scope.exclude.indices".
                if (!this.scope.exclude().indices().isEmpty())
                    this.model = excludeIndices(this.model, this.scope.exclude().indices());

                // Intersect the "resolvers" field of the entity model with "scope.exclude.resolvers".
                if (!this.scope.exclude().resolvers().isEmpty())
                    this.model = excludeResolvers(this.model, this.scope.exclude().resolvers());
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
