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
package io.zentity.resolution.input.scope;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.Attribute;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public abstract class ScopeField {

    protected Map<String, Attribute> attributes = new TreeMap<>();
    protected Set<String> indices = new TreeSet<>();
    protected Set<String> resolvers = new TreeSet<>();

    public ScopeField() {
    }

    /**
     * Parse and validate the "scope.*.attributes" field of the request body or URL.
     *
     * @param scopeType       "exclude" or "include".
     * @param model           The entity model.
     * @param scopeAttributes The "attributes" object of "scope.exclude" or "scope.include".
     * @return Names and values of attributes to include in the entity model.
     * @throws ValidationException
     * @throws JsonProcessingException
     */
    public static Map<String, Attribute> parseAttributes(String scopeType, Model model, JsonNode scopeAttributes) throws ValidationException, JsonProcessingException {
        Map<String, Attribute> attributesObj = new TreeMap<>();
        if (scopeAttributes.isNull())
            return attributesObj;
        if (!scopeAttributes.isObject())
            throw new ValidationException("'scope." + scopeType + ".attributes' must be an object.");
        Iterator<Map.Entry<String, JsonNode>> attributeNodes = scopeAttributes.fields();
        while (attributeNodes.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributeNodes.next();
            String attributeName = attribute.getKey();

            // Validate that the attribute exists in the entity model.
            if (!model.attributes().containsKey(attributeName))
                throw new ValidationException("'" + attributeName + "' is not defined in the entity model.");

            // Parse the attribute values.
            String attributeType = model.attributes().get(attributeName).type();
            JsonNode valuesNode = scopeAttributes.get(attributeName);
            if (!valuesNode.isNull())
                attributesObj.put(attributeName, new Attribute(attributeName, attributeType, valuesNode));
        }
        return attributesObj;
    }

    /**
     * Parse and validate the "scope.*.indices" field of the request body or URL.
     *
     * @param scopeType    "include" or "exclude".
     * @param scopeIndices The "indices" object of "scope.exclude" or "scope.include".
     * @return Names of indices to include in the entity model.
     * @throws ValidationException
     */
    public static Set<String> parseIndices(String scopeType, JsonNode scopeIndices) throws ValidationException {
        Set<String> indices = new TreeSet<>();
        if (scopeIndices.isNull())
            return indices;
        if (scopeIndices.isTextual()) {
            if (scopeIndices.asText().equals(""))
                throw new ValidationException("'scope." + scopeType + ".indices' must not have non-empty strings.");
            String index = scopeIndices.asText();
            indices.add(index);
        } else if (scopeIndices.isArray()) {
            for (JsonNode indexNode : scopeIndices) {
                if (!indexNode.isTextual())
                    throw new ValidationException("'scope." + scopeType + ".indices' must be a string or an array of strings.");
                String index = indexNode.asText();
                if (index == null || index.equals(""))
                    throw new ValidationException("'scope." + scopeType + ".indices' must not have non-empty strings.");
                indices.add(index);
            }
        } else {
            throw new ValidationException("'scope." + scopeType + ".indices' must be a string or an array of strings.");
        }
        return indices;
    }

    /**
     * Parse and validate the "scope.*.resolvers" field of the request body or URL.
     *
     * @param scopeType      "include" or "exclude".
     * @param scopeResolvers The "resolvers" object of "scope.exclude" or "scope.include".
     * @return Names of resolvers to exclude from the entity model.
     * @throws ValidationException
     */
    public static Set<String> parseResolvers(String scopeType, JsonNode scopeResolvers) throws ValidationException {
        Set<String> resolvers = new TreeSet<>();
        if (scopeResolvers.isNull())
            return resolvers;
        if (scopeResolvers.isTextual()) {
            if (scopeResolvers.asText().equals(""))
                throw new ValidationException("'scope." + scopeType + ".resolvers' must not have non-empty strings.");
            String resolver = scopeResolvers.asText();
            resolvers.add(resolver);
        } else if (scopeResolvers.isArray()) {
            for (JsonNode resolverNode : scopeResolvers) {
                if (!resolverNode.isTextual())
                    throw new ValidationException("'scope." + scopeType + ".resolvers' must be a string or an array of strings.");
                String resolver = resolverNode.asText();
                if (resolver == null || resolver.equals(""))
                    throw new ValidationException("'scope." + scopeType + ".resolvers' must not have non-empty strings.");
                resolvers.add(resolver);
            }
        } else {
            throw new ValidationException("'scope." + scopeType + ".resolvers' must be a string or an array of strings.");
        }
        return resolvers;
    }

    public Map<String, Attribute> attributes() {
        return this.attributes;
    }

    public void attributes(Map<String, Attribute> attributes) {
        this.attributes = attributes;
    }

    public Set<String> indices() {
        return this.indices;
    }

    public void indices(Set<String> indices) {
        this.indices = indices;
    }

    public Set<String> resolvers() {
        return this.resolvers;
    }

    public void resolvers(Set<String> resolvers) {
        this.resolvers = resolvers;
    }

    public abstract void deserialize(JsonNode json, Model model) throws ValidationException, IOException;

    public void deserialize(String json, Model model) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json), model);
    }

}
