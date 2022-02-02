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
package io.zentity.resolution.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.Index;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.scope.Scope;

import java.io.IOException;
import java.util.*;

public class Input {

    private Map<String, Attribute> attributes = new TreeMap<>();
    private Map<String, Set<String>> ids = new TreeMap<>();
    private Model model;
    private Scope scope = new Scope();
    private Set<Term> terms = new TreeSet<>();

    public Input(JsonNode json, Model model) throws ValidationException, IOException {
        this.model = model;
        this.deserialize(json);
    }

    public Input(String json, Model model) throws ValidationException, IOException {
        this.model = model;
        this.deserialize(json);
    }

    public Input(JsonNode json) throws ValidationException, IOException {
        this.deserialize(json);
    }

    public Input(String json) throws ValidationException, IOException {
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
     * Parse and validate the "ids" field of the request body.
     *
     * @param requestBody The request body.
     * @param model       The entity model.
     * @return The parsed "ids" field from the request body.
     * @throws ValidationException
     */
    public static Map<String, Set<String>> parseIds(JsonNode requestBody, Model model) throws ValidationException {
        Map<String, Set<String>> idsObj = new TreeMap<>();
        if (!requestBody.has("ids") || requestBody.get("ids").size() == 0)
            return idsObj;
        JsonNode ids = requestBody.get("ids");
        Iterator<Map.Entry<String, JsonNode>> indices = ids.fields();
        while (indices.hasNext()) {
            Map.Entry<String, JsonNode> index = indices.next();
            String indexName = index.getKey();
            JsonNode idsValues = index.getValue();

            // Validate that the index exists in the entity model.
            if (!model.indices().containsKey(indexName))
                throw new ValidationException("'ids." + indexName + "' is not defined in the entity model.");

            // Parse the id values.
            idsObj.put(indexName, new TreeSet<>());
            if (!idsValues.isNull() && !idsValues.isArray())
                throw new ValidationException("'ids." + indexName + "' must be an array.");
            Iterator<JsonNode> idsNode = idsValues.elements();
            while (idsNode.hasNext()) {
                JsonNode idNode = idsNode.next();
                if (!idNode.isTextual())
                    throw new ValidationException("'ids." + indexName + "' must be an array of strings.");
                String id = idNode.asText();
                if (Patterns.EMPTY_STRING.matcher(id).matches())
                    throw new ValidationException("'ids." + indexName + "' must be an array of non-empty strings.");
                idsObj.get(indexName).add(Json.quoteString(id));
            }
        }
        return idsObj;
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
        Map<String, Attribute> attributesObj = new TreeMap<>();
        if (!requestBody.has("attributes") || requestBody.get("attributes").size() == 0)
            return attributesObj;
        JsonNode attributes = requestBody.get("attributes");
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
     * Parse and validate the "terms" field of the request body.
     *
     * @param requestBody The request body.
     * @return The parsed "terms" field from the request body.
     * @throws ValidationException
     */
    public static Set<Term> parseTerms(JsonNode requestBody) throws ValidationException {
        Set<Term> terms = new TreeSet<>();
        if (!requestBody.has("terms") || requestBody.get("terms").size() == 0)
            return terms;
        if (requestBody.get("terms").isArray()) {
            Iterator<JsonNode> termsNode = requestBody.get("terms").elements();
            while (termsNode.hasNext()) {
                JsonNode termNode = termsNode.next();
                if (!termNode.isTextual())
                    throw new ValidationException("'terms' must be an array of strings.");
                terms.add(new Term(termNode.asText()));
            }
        } else if (!requestBody.get("terms").isNull()) {
            throw new ValidationException("'terms' must be an object or an array of strings.");
        }
        return terms;
    }

    /**
     * Parse and validate the entity model from the 'model' field of the request body.
     * The model must be runnable, meaning there are no missing fields required for resolution.
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
        return new Model(model.toString(), true);
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
            throw new ValidationException("'" + field + "' must not be empty.");

    }

    public Map<String, Attribute> attributes() {
        return this.attributes;
    }

    public Map<String, Set<String>> ids() {
        return this.ids;
    }

    public Model model() {
        return this.model;
    }

    public Scope scope() {
        return this.scope;
    }

    public Set<Term> terms() {
        return this.terms;
    }

    public void deserialize(JsonNode json) throws ValidationException, IOException {
        if (!json.isObject())
            throw new ValidationException("Input must be an object.");

        // Validate recognized fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            switch (name) {
                case "attributes":
                case "ids":
                case "model":
                case "scope":
                case "terms":
                    break;
                default:
                    throw new ValidationException("'" + name + "' is not a recognized field.");
            }
        }

        // Parse and validate the "model" field of the request body, or the entity model stored in the index.
        if (this.model == null) {
            if (!json.has("model"))
                throw new ValidationException("You must specify either an entity type or an entity model.");
            this.model = parseEntityModel(json);
        } else if (json.has("model")) {
            throw new ValidationException("You must specify either an entity type or an entity model, not both.");
        }

        // Parse and validate the "attributes" field of the request body.
        this.attributes = parseAttributes(json, this.model);

        // Parse and validate the "terms" field of the request body.
        this.terms = parseTerms(json);

        // Parse and validate the "ids" field of the request body.
        this.ids = parseIds(json, this.model);

        // Ensure that either the "attributes" or "terms" or "ids" field exists and is valid.
        if (this.attributes().isEmpty() && this.terms.isEmpty() && this.ids.isEmpty())
            throw new ValidationException("The 'attributes', 'terms', and 'ids' fields are missing from the request body. At least one must exist.");

        // Parse and validate the "scope" field of the request body.
        if (json.has("scope")) {
            this.scope.deserialize(json.get("scope"), this.model);

            // Parse and validate the "scope"."include" field of the request body.
            if (this.scope.include() != null) {

                // Remove any resolvers of the entity model that do not appear in "scope.include.resolvers".
                if (!this.scope.include().resolvers().isEmpty())
                    this.model = includeResolvers(this.model, this.scope.include().resolvers());

                // Remove any indices of the entity model that do not appear in "scope.include.indices".
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

        // Validate that the attribute associated with each index field has any and all required params.
        // For example, 'date' attributes require the 'format' field to be specified in the matcher params,
        // the model attribute params, or the input attribute params so that the dates can be queried and returned
        // in a normalized fashion. Currently this only applies to 'date' attribute types.
        Set<String> paramsValidated = new TreeSet<>();
        for (String indexName : this.model.indices().keySet()) {
            Index index = this.model.indices().get(indexName);
            for (String attributeName : index.attributeIndexFieldsMap().keySet()) {
                if (paramsValidated.contains(attributeName))
                    continue;
                if (!this.model.attributes().containsKey(attributeName))
                    continue;
                switch (this.model.attributes().get(attributeName).type()) {
                    case "date":
                        // Check if the required params are defined in the input attribute.
                        Map<String, String> params = new TreeMap<>();
                        if (this.attributes.containsKey(attributeName))
                            params = this.attributes.get(attributeName).params();
                        if (!params.containsKey("format") || params.get("format").equals("null") || Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                            // Otherwise check if the required params are defined in the model attribute.
                            params = this.model.attributes().get(attributeName).params();
                            if (!params.containsKey("format") || params.get("format").equals("null") || Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                // Otherwise check if the required params are defined in the matcher associated with the index field.
                                for (String indexFieldName : index.attributeIndexFieldsMap().get(attributeName).keySet()) {
                                    String matcherName = index.attributeIndexFieldsMap().get(attributeName).get(indexFieldName).matcher();
                                    params = this.model.matchers().get(matcherName).params();
                                    if (!params.containsKey("format") || params.get("format").equals("null") || Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                        // If we've gotten this far, that means that the required params for this attribute type
                                        // haven't been specified in any valid places.
                                        throw new ValidationException("'attributes." + attributeName + "' is a 'date' which required a 'format' to be specified in the params.");
                                    }
                                }
                            }
                        }
                        break;
                    default:
                        break;
                }
                paramsValidated.add(attributeName);
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json));
    }

}
