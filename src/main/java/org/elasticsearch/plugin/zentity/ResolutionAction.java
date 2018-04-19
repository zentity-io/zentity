package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zentity.model.Attribute;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.Job;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ResolutionAction extends BaseRestHandler {

    @Inject
    ResolutionAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "_zentity/resolution", this);
        controller.registerHandler(POST, "_zentity/resolution/{entity_type}", this);
    }

    /**
     * Parse the entity type from either the request body or URL.
     *
     * @param entityTypeFromUrl The value of the "entity_type" parameter from the URL.
     * @param requestBody       The request body.
     * @return The value of the "entity_type" from either the request body or URL.
     * @throws BadRequestException
     */
    public static String parseEntityType(String entityTypeFromUrl, JsonNode requestBody) throws BadRequestException {
        String entityType = null;
        if (entityTypeFromUrl == null || entityTypeFromUrl.equals("")) {
            if (requestBody.has("entity_type"))
                entityType = requestBody.get("entity_type").asText();
        } else {
            if (requestBody.has("entity_type"))
                throw new BadRequestException("'entity_type' must be specified in the request body or URL, but not both.");
            entityType = entityTypeFromUrl;
        }
        return entityType;
    }

    /**
     * Parse and validate the entity model from the 'model' field of the request body.
     *
     * @param requestBody The request body.
     * @return The parsed "model" field from the request body, or an object from ".zentity-models" index.
     * @throws BadRequestException
     * @throws IOException
     * @throws ValidationException
     */
    public static Model parseEntityModel(JsonNode requestBody) throws BadRequestException, IOException, ValidationException {
        if (!requestBody.has("model"))
            throw new BadRequestException("The 'model' field is missing from the request body while 'entity_type' is undefined.");
        JsonNode model = requestBody.get("model");
        if (!model.isObject())
            throw new BadRequestException("Entity model must be an object.");
        return new Model(model.toString());
    }

    /**
     * Retrieve, parse, and validate the entity model of a given type.
     * Otherwise expect the entity model to be provided in the 'model' field of the request body.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The parsed "model" field from the request body, or an object from ".zentity-models" index.
     * @throws NotFoundException
     * @throws IOException
     * @throws ValidationException
     */
    public static Model parseEntityModel(String entityType, NodeClient client) throws NotFoundException, IOException, ValidationException {
        String model = ModelsAction.getEntityModel(entityType, client).getSourceAsString();
        if (model == null || model.equals(""))
            throw new NotFoundException("Model not found for entity type '" + entityType + "'.");
        return new Model(model);
    }

    /**
     * Parse and validate the "scope.*.attributes" field of the request body or URL.
     *
     * @param scopeType       "exclude" or "include".
     * @param model           The entity model.
     * @param scopeAttributes The "attributes" object of "scope.exclude" or "scope.include".
     * @return Names and values of attributes to include in the entity model.
     * @throws BadRequestException
     * @throws ValidationException
     */
    public static Map<String, Set<Object>> parseScopeAttributes(String scopeType, Model model, JsonNode scopeAttributes) throws BadRequestException, ValidationException {
        Map<String, Set<Object>> attributes = new HashMap<>();
        if (scopeAttributes.isNull())
            return attributes;
        if (!scopeAttributes.isObject())
            throw new BadRequestException("'scope." + scopeType + ".attributes' must be an object.");
        Iterator<Map.Entry<String, JsonNode>> attributeNodes = scopeAttributes.fields();
        while (attributeNodes.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributeNodes.next();
            String attributeName = attribute.getKey();
            JsonNode attributeNode = attribute.getValue();
            if (!attributeNode.isArray()) {
                // Validate that the attribute exists in the entity model.
                if (!model.attributes().containsKey(attributeName))
                    throw new BadRequestException("'scope." + scopeType + ".attributes." + attributeName + "' is not defined in the entity model.");
                String attributeType = model.attributes().get(attributeName).type();
                try {
                    Attribute.validateType(attributeType, attributeNode);
                } catch (ValidationException e) {
                    throw new ValidationException("'scope." + scopeType + ".attributes." + attributeName + "' must be a " + attributeType + " data type.");
                }
                Object attributeValue = Attribute.convertType(attributeType, attributeNode);
                if (!attributes.containsKey(attributeName))
                    attributes.put(attributeName, new HashSet<>());
                attributes.get(attributeName).add(attributeValue);
            } else {
                for (JsonNode node : attributeNode) {
                    // Validate that the attribute exists in the entity model.
                    if (!model.attributes().containsKey(attributeName))
                        throw new BadRequestException("'scope." + scopeType + ".attributes." + attributeName + "' is not defined in the entity model.");
                    String attributeType = model.attributes().get(attributeName).type();
                    try {
                        Attribute.validateType(attributeType, node);
                    } catch (ValidationException e) {
                        throw new ValidationException("'scope." + scopeType + ".attributes." + attributeName + "' must be a " + attributeType + " data type.");
                    }
                    Object attributeValue = Attribute.convertType(attributeType, node);
                    if (!attributes.containsKey(attributeName))
                        attributes.put(attributeName, new HashSet<>());
                    attributes.get(attributeName).add(attributeValue);
                }
            }
        }
        return attributes;
    }

    /**
     * Parse and validate the "scope.exclude.attributes" field of the request body or URL.
     *
     * @param model           The entity model.
     * @param scopeAttributes The "attributes" object of "scope.exclude".
     * @return Names and values of attributes to exclude in the entity model.
     * @throws BadRequestException
     * @throws ValidationException
     */
    public static Map<String, Set<Object>> parseScopeExcludeAttributes(Model model, JsonNode scopeAttributes) throws BadRequestException, ValidationException {
        return parseScopeAttributes("exclude", model, scopeAttributes);
    }

    /**
     * Parse and validate the "scope.include.attributes" field of the request body or URL.
     *
     * @param model           The entity model.
     * @param scopeAttributes The "attributes" object of "scope.include".
     * @return Names and values of attributes to include in the entity model.
     * @throws BadRequestException
     * @throws ValidationException
     */
    public static Map<String, Set<Object>> parseScopeIncludeAttributes(Model model, JsonNode scopeAttributes) throws BadRequestException, ValidationException {
        return parseScopeAttributes("include", model, scopeAttributes);
    }

    /**
     * Parse and validate the "scope.*.indices" field of the request body or URL.
     *
     * @param scopeType    "include" or "exclude".
     * @param scopeIndices The "indices" object of "scope.exclude" or "scope.include".
     * @return Names of indices to include in the entity model.
     * @throws BadRequestException
     */
    public static Set<String> parseScopeIndices(String scopeType, JsonNode scopeIndices) throws BadRequestException {
        Set<String> indices = new HashSet<>();
        if (scopeIndices.isNull())
            return indices;
        if (scopeIndices.isTextual()) {
            if (scopeIndices.asText().equals(""))
                throw new BadRequestException("'scope." + scopeType + ".indices' must not have non-empty strings.");
            String index = scopeIndices.asText();
            indices.add(index);
        } else if (scopeIndices.isArray()) {
            for (JsonNode indexNode : scopeIndices) {
                if (!indexNode.isTextual())
                    throw new BadRequestException("'scope." + scopeType + ".indices' must be a string or an array of strings.");
                String index = indexNode.asText();
                if (index == null || index.equals(""))
                    throw new BadRequestException("'scope." + scopeType + ".indices' must not have non-empty strings.");
                indices.add(index);
            }
        } else {
            throw new BadRequestException("'scope." + scopeType + ".indices' must be a string or an array of strings.");
        }
        return indices;
    }

    /**
     * Parse and validate the "scope.exclude.indices" field of the request body or URL.
     *
     * @param scopeIndices The "indices" object of "scope.exclude".
     * @return Names of indices to exclude from the entity model.
     * @throws BadRequestException
     */
    public static Set<String> parseScopeExcludeIndices(JsonNode scopeIndices) throws BadRequestException {
        return parseScopeIndices("exclude", scopeIndices);
    }

    /**
     * Parse and validate the "scope.include.indices" field of the request body or URL.
     *
     * @param scopeIndices The "indices" object of "scope.include".
     * @return Names of indices to include in the entity model.
     * @throws BadRequestException
     */
    public static Set<String> parseScopeIncludeIndices(JsonNode scopeIndices) throws BadRequestException {
        return parseScopeIndices("include", scopeIndices);
    }

    /**
     * Exclude indices from an entity model, while retaining all the others.
     *
     * @param model   The entity model.
     * @param indices Names of indices from "scope.exclude.indices" to exclude in the entity model.
     * @return Updated entity model.
     */
    public static Model excludeIndices(Model model, Set<String> indices) throws BadRequestException {
        if (!indices.isEmpty()) {
            for (String index : indices) {
                if (index == null || index.equals(""))
                    continue;
                if (!model.indices().containsKey(index))
                    throw new BadRequestException("'" + index + "' is not in the 'indices' field of the entity model.");
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
     * @throws BadRequestException
     */
    public static Model includeIndices(Model model, Set<String> indices) throws BadRequestException {
        if (!indices.isEmpty()) {
            for (String index : indices) {
                if (index == null || index.equals(""))
                    continue;
                if (!model.indices().containsKey(index))
                    throw new BadRequestException("'" + index + "' is not in the 'indices' field of the entity model.");
            }
            model.indices().keySet().retainAll(indices);
        }
        return model;
    }

    /**
     * Parse and validate the "scope.*.resolvers" field of the request body or URL.
     *
     * @param scopeType      "include" or "exclude".
     * @param scopeResolvers The "resolvers" object of "scope.exclude" or "scope.include".
     * @return Names of resolvers to exclude from the entity model.
     * @throws BadRequestException
     */
    public static Set<String> parseScopeResolvers(String scopeType, JsonNode scopeResolvers) throws BadRequestException {
        Set<String> resolvers = new HashSet<>();
        if (scopeResolvers.isNull())
            return resolvers;
        if (scopeResolvers.isTextual()) {
            if (scopeResolvers.asText().equals(""))
                throw new BadRequestException("'scope." + scopeType + ".resolvers' must not have non-empty strings.");
            String resolver = scopeResolvers.asText();
            resolvers.add(resolver);
        } else if (scopeResolvers.isArray()) {
            for (JsonNode resolverNode : scopeResolvers) {
                if (!resolverNode.isTextual())
                    throw new BadRequestException("'scope." + scopeType + ".resolvers' must be a string or an array of strings.");
                String resolver = resolverNode.asText();
                if (resolver == null || resolver.equals(""))
                    throw new BadRequestException("'scope." + scopeType + ".resolvers' must not have non-empty strings.");
                resolvers.add(resolver);
            }
        } else {
            throw new BadRequestException("'scope." + scopeType + ".resolvers' must be a string or an array of strings.");
        }
        return resolvers;
    }

    /**
     * Parse and validate the "scope.exclude.resolvers" field of the request body or URL.
     *
     * @param scopeResolvers The "resolvers" object of "scope.exclude".
     * @return Names of resolvers to exclude from the entity model.
     * @throws BadRequestException
     */
    public static Set<String> parseScopeExcludeResolvers(JsonNode scopeResolvers) throws BadRequestException {
        return parseScopeResolvers("exclude", scopeResolvers);
    }

    /**
     * Parse and validate the "scope.include.resolvers" field of the request body or URL.
     *
     * @param scopeResolvers The "resolvers" object of "scope.include".
     * @return Names of resolvers to include in the entity model.
     * @throws BadRequestException
     */
    public static Set<String> parseScopeIncludeResolvers(JsonNode scopeResolvers) throws BadRequestException {
        return parseScopeResolvers("include", scopeResolvers);
    }

    /**
     * Exclude resolvers from an entity model, while retaining all the others.
     *
     * @param model     The entity model.
     * @param resolvers Names of resolvers from "scope.exclude.resolvers" to exclude in the entity model.
     * @return Updated entity model.
     */
    public static Model excludeResolvers(Model model, Set<String> resolvers) throws BadRequestException {
        if (!resolvers.isEmpty()) {
            for (String resolver : resolvers) {
                if (resolver == null || resolver.equals(""))
                    continue;
                if (!model.resolvers().containsKey(resolver))
                    throw new BadRequestException("'" + resolver + "' is not in the 'resolvers' field of the entity model.");
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
     * @throws BadRequestException
     */
    public static Model includeResolvers(Model model, Set<String> resolvers) throws BadRequestException {
        if (!resolvers.isEmpty()) {
            for (String resolver : resolvers) {
                if (resolver == null || resolver.equals(""))
                    continue;
                if (!model.resolvers().containsKey(resolver))
                    throw new BadRequestException("'" + resolver + "' is not in the 'resolvers' field of the entity model.");
            }
            model.resolvers().keySet().retainAll(resolvers);
        }
        return model;
    }

    /**
     * Parse and validate the "attributes" field of the request body.
     *
     * @param model       The entity model.
     * @param requestBody The request body.
     * @return The parsed "attributes" field from the request body.
     * @throws BadRequestException
     */
    public static Map<String, Set<Object>> parseAttributes(Model model, JsonNode requestBody) throws BadRequestException, ValidationException {
        if (!requestBody.has("attributes"))
            throw new BadRequestException("The 'attributes' field is missing from the request body.");
        if (requestBody.get("attributes").size() == 0)
            throw new BadRequestException("The 'attributes' field of the request body must not be empty.");
        JsonNode attributes = requestBody.get("attributes");
        Map<String, Set<Object>> attributesObj = new HashMap<>();
        Iterator<String> attributeFields = attributes.fieldNames();
        while (attributeFields.hasNext()) {
            String attributeName = attributeFields.next();

            // Validate that the attribute exists in the entity model.
            if (!model.attributes().containsKey(attributeName))
                throw new BadRequestException("'attributes." + attributeName + "' is not defined in the entity model.");
            attributesObj.put(attributeName, new HashSet<>());
            String attributeType = model.attributes().get(attributeName).type();

            if (attributes.get(attributeName).isObject())
                throw new BadRequestException("'attributes." + attributeName + "' must be a string or an array of values.");
            if (attributes.get(attributeName).isArray()) {
                for (JsonNode valueNode : attributes.get(attributeName)) {
                    try {
                        Attribute.validateType(attributeType, valueNode);
                    } catch (ValidationException e) {
                        throw new ValidationException("'attributes." + attributeName + "' must be a " + attributeType + " data type.");
                    }
                    Object value = Attribute.convertType(attributeType, valueNode);
                    attributesObj.get(attributeName).add(value);
                }
            } else {
                JsonNode valueNode = attributes.get(attributeName);
                try {
                    Attribute.validateType(attributeType, valueNode);
                } catch (ValidationException e) {
                    throw new ValidationException("'attributes." + attributeName + "' must be a " + attributeType + " data type.");
                }
                Object value = Attribute.convertType(attributeType, valueNode);
                attributesObj.get(attributeName).add(value);
            }
        }
        return attributesObj;
    }

    /**
     * Parse and validate the "scope.exclude" field of the request body.
     *
     * @param scopeExclude The "scope.exclude" object of the request body.
     * @throws BadRequestException
     * @throws ValidationException
     */
    public static void parseScopeExclude(JsonNode scopeExclude) throws BadRequestException, ValidationException {
        if (!scopeExclude.isNull() && !scopeExclude.isObject())
            throw new BadRequestException("'scope.exclude' must be an object.");
        Iterator<Map.Entry<String, JsonNode>> fields = scopeExclude.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            switch (fieldName) {
                case "attributes":
                    break;
                case "indices":
                    break;
                case "resolvers":
                    break;
                default:
                    throw new ValidationException("'scope.exclude." + fieldName + "' is not a recognized field.");
            }
        }
    }

    /**
     * Parse and validate the "scope.include" field of the request body.
     *
     * @param scopeInclude The "scope.include" object of the request body.
     * @throws BadRequestException
     * @throws ValidationException
     */
    public static void parseScopeInclude(JsonNode scopeInclude) throws BadRequestException, ValidationException {
        if (!scopeInclude.isNull() && !scopeInclude.isObject())
            throw new BadRequestException("'scope.include' must be an object.");
        Iterator<Map.Entry<String, JsonNode>> fields = scopeInclude.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            switch (fieldName) {
                case "attributes":
                    break;
                case "indices":
                    break;
                case "resolvers":
                    break;
                default:
                    throw new ValidationException("'scope.include." + fieldName + "' is not a recognized field.");
            }
        }
    }

    /**
     * Parse and validate the "scope" field of the request body.
     *
     * @param scope The "scope" object of the request body.
     * @throws BadRequestException
     * @throws ValidationException
     */
    public static void parseScope(JsonNode scope) throws BadRequestException, ValidationException {
        if (!scope.isNull() && !scope.isObject())
            throw new BadRequestException("'scope' must be an object.");
        Iterator<Map.Entry<String, JsonNode>> fields = scope.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            switch (fieldName) {
                case "exclude":
                    break;
                case "include":
                    break;
                default:
                    throw new ValidationException("'scope." + fieldName + "' is not a recognized field.");
            }
        }
    }

    @Override
    public String getName() {
        return "zentity_resolution_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        String body = restRequest.content().utf8ToString();

        // Parse the request params that will be passed to the job configuration
        String entityTypeFromUrl = restRequest.param("entity_type");
        Boolean includeAttributes = restRequest.paramAsBoolean("_attributes", Job.DEFAULT_INCLUDE_ATTRIBUTES);
        Boolean includeHits = restRequest.paramAsBoolean("hits", Job.DEFAULT_INCLUDE_HITS);
        Boolean includeQueries = restRequest.paramAsBoolean("queries", Job.DEFAULT_INCLUDE_QUERIES);
        Boolean includeSource = restRequest.paramAsBoolean("_source", Job.DEFAULT_INCLUDE_SOURCE);
        int maxDocsPerQuery = restRequest.paramAsInt("max_docs_per_query", Job.DEFAULT_MAX_DOCS_PER_QUERY);
        int maxHops = restRequest.paramAsInt("max_hops", Job.DEFAULT_MAX_HOPS);
        Boolean pretty = restRequest.paramAsBoolean("pretty", Job.DEFAULT_PRETTY);
        Boolean profile = restRequest.paramAsBoolean("profile", Job.DEFAULT_PROFILE);

        return channel -> {
            try {

                // Prepare the entity resolution job.
                Job job = new Job(client);
                job.includeAttributes(includeAttributes);
                job.includeHits(includeHits);
                job.includeQueries(includeQueries);
                job.includeSource(includeSource);
                job.maxDocsPerQuery(maxDocsPerQuery);
                job.maxHops(maxHops);
                job.pretty(pretty);
                job.profile(profile);

                // Parse the request body.
                if (body == null || body.equals(""))
                    throw new BadRequestException("Request body is missing.");
                ObjectMapper mapper = new ObjectMapper();
                JsonNode requestBody = mapper.readTree(body);

                // Parse the entity type.
                String entityType = parseEntityType(entityTypeFromUrl, requestBody);

                // Parse and validate the entity model.
                Model model;
                if (entityType == null || entityType.equals(""))
                    model = parseEntityModel(requestBody);
                else
                    model = parseEntityModel(entityType, client);

                // Parse and validate the input attributes.
                Map<String, Set<Object>> inputAttributesObj = parseAttributes(model, requestBody);
                job.inputAttributes(inputAttributesObj);

                // Validate the "scope" field of the request body.
                if (requestBody.has("scope"))
                    if (!requestBody.get("scope").isNull() && !requestBody.get("scope").isObject())
                        throw new BadRequestException("The 'scope' field of the request body must be an object.");

                // Parse and validate the "scope" field of the request body.
                if (requestBody.has("scope")) {
                    parseScope(requestBody.get("scope"));

                    // Parse and validate the "scope.include" field of the request body.
                    if (requestBody.get("scope").has("include")) {
                        parseScopeInclude(requestBody.get("scope").get("include"));

                        // Parse and validate the "scope.include.attributes" field of the request body.
                        if (requestBody.get("scope").get("include").has("attributes")) {
                            Map<String, Set<Object>> scopeIncludeAttributes = parseScopeIncludeAttributes(model, requestBody.get("scope").get("include").get("attributes"));
                            job.scopeIncludeAttributes(scopeIncludeAttributes);
                        }

                        // Parse and validate the "scope.include.resolvers" field of the request body.
                        if (requestBody.get("scope").get("include").has("resolvers")) {
                            Set<String> scopeIncludeResolvers = parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
                            // Remove any resolvers entity model that do not appear in "scope.include.resolvers".
                            model = includeResolvers(model, scopeIncludeResolvers);
                        }

                        // Parse and validate the "scope.include.resolvers" field of the request body.
                        if (requestBody.get("scope").get("include").has("indices")) {
                            Set<String> scopeIncludeIndices = parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
                            // Remove any indices entity model that do not appear in "scope.include.indices".
                            model = includeIndices(model, scopeIncludeIndices);
                        }
                    }

                    // Parse and validate the "scope.exclude" field of the request body.
                    if (requestBody.get("scope").has("exclude")) {
                        parseScopeExclude(requestBody.get("scope").get("exclude"));

                        // Parse and validate the "scope.exclude.attributes" field of the request body.
                        if (requestBody.get("scope").get("exclude").has("attributes")) {
                            Map<String, Set<Object>> scopeExcludeAttributes = parseScopeExcludeAttributes(model, requestBody.get("scope").get("exclude").get("attributes"));
                            job.scopeExcludeAttributes(scopeExcludeAttributes);
                        }

                        // Parse and validate the "scope.exclude.resolvers" field of the request body.
                        if (requestBody.get("scope").get("exclude").has("resolvers")) {
                            Set<String> scopeExcludeResolvers = parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
                            // Intersect the "resolvers" field of the entity model with "scope.exclude.resolvers".
                            model = excludeResolvers(model, scopeExcludeResolvers);
                        }

                        // Parse and validate the "scope.exclude.resolvers" field of the request body.
                        if (requestBody.get("scope").get("exclude").has("indices")) {
                            Set<String> scopeExcludeIndices = parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
                            // Intersect the "resolvers" field of the entity model with "scope.exclude.indices".
                            model = excludeIndices(model, scopeExcludeIndices);
                        }

                    }
                }
                if (model.resolvers().isEmpty())
                    throw new BadRequestException("No resolvers have been provided for the entity resolution job.");
                if (model.indices().isEmpty())
                    throw new BadRequestException("No indices have been provided for the entity resolution job.");

                // Run the entity resolution job.
                job.model(model);
                String response = job.run();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response));

            } catch (BadRequestException | ValidationException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (NotFoundException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_FOUND, e));
            }
        };
    }
}
