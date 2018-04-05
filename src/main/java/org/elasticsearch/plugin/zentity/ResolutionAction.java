package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
     * Parse and validate the "scope.indices" field of the request body or URL.
     *
     * @param requestBody The request body.
     * @return Names of indices to filter from the "scope.indices" object of the entity model.
     * @throws BadRequestException
     */
    public static HashSet<String> parseScopeIndices(JsonNode requestBody) throws BadRequestException {
        HashSet<String> indicesScopeSet = new HashSet<>();
        if (!requestBody.has("scope") || requestBody.get("scope").isNull())
            return indicesScopeSet;
        if (!requestBody.get("scope").isObject())
            throw new BadRequestException("The 'scope' field of the request body must be an object.");
        if (!requestBody.get("scope").has("indices"))
            return indicesScopeSet;
        if (requestBody.get("scope").get("indices").isTextual()) {
            String index = requestBody.get("scope").get("indices").asText();
            indicesScopeSet.add(index);
        } else if (requestBody.get("scope").get("indices").isArray()) {
            for (JsonNode indexNode : requestBody.get("scope").get("indices")) {
                if (!indexNode.isTextual())
                    throw new BadRequestException("'scope.indices' must be a string or an array of strings.");
                String index = indexNode.asText();
                if (index == null || index.equals(""))
                    throw new BadRequestException("'scope.indices' must be have non-empty strings.");
                indicesScopeSet.add(index);
            }
        } else {
            throw new BadRequestException("'scope.indices' must be a string or an array of strings.");
        }
        return indicesScopeSet;
    }

    /**
     * Filter indices based on what the client wants to use.
     *
     * @param model        The entity model.
     * @param scopeIndices Names of indices to filter from the "indices" object.
     * @return Filtered "indices" object.
     * @throws BadRequestException
     */
    public static Model filterIndices(Model model, Set<String> scopeIndices) throws BadRequestException {
        if (!scopeIndices.isEmpty()) {
            for (String index : scopeIndices) {
                if (index == null || index.equals(""))
                    continue;
                if (!model.indices().containsKey(index))
                    throw new BadRequestException("'" + index + "' is not in the 'indices' field.");
            }
            model.indices().keySet().retainAll(scopeIndices);
        }
        return model;
    }

    /**
     * Parse and validate the "scope.resolvers" field of the request body or URL.
     *
     * @param requestBody The request body.
     * @return Names of resolvers to filter from the "scope.resolvers" object of the entity model.
     * @throws BadRequestException
     */
    public static HashSet<String> parseScopeResolvers(JsonNode requestBody) throws BadRequestException {
        HashSet<String> resolversScopeSet = new HashSet<>();
        if (!requestBody.has("scope") || requestBody.get("scope").isNull())
            return resolversScopeSet;
        if (!requestBody.get("scope").isObject())
            throw new BadRequestException("The 'scope' field of the request body must be an object.");
        if (!requestBody.get("scope").has("resolvers"))
            return resolversScopeSet;
        if (requestBody.get("scope").get("resolvers").isTextual()) {
            String resolver = requestBody.get("scope").get("resolvers").asText();
            resolversScopeSet.add(resolver);
        } else if (requestBody.get("scope").get("resolvers").isArray()) {
            for (JsonNode resolverNode : requestBody.get("scope").get("resolvers")) {
                if (!resolverNode.isTextual())
                    throw new BadRequestException("'scope.resolvers' must be a string or an array of strings.");
                String resolver = resolverNode.asText();
                if (resolver == null || resolver.equals(""))
                    throw new BadRequestException("'scope.resolvers' must have non-empty strings.");
                resolversScopeSet.add(resolver);
            }
        } else {
            throw new BadRequestException("'scope.resolvers' must be a string or an array of strings.");
        }
        return resolversScopeSet;
    }

    /**
     * Filter resolvers based on what the client wants to use.
     *
     * @param model          The entity model.
     * @param scopeResolvers Names of resolvers to filter from the "resolvers" object.
     * @return Filtered "resolvers" object.
     * @throws BadRequestException
     */
    public static Model filterResolvers(Model model, Set<String> scopeResolvers) throws BadRequestException {
        if (!scopeResolvers.isEmpty()) {
            for (String resolver : scopeResolvers) {
                if (resolver == null || resolver.equals(""))
                    continue;
                if (!model.resolvers().containsKey(resolver))
                    throw new BadRequestException("'" + resolver + "' is not in the 'resolvers' field.");
            }
            model.resolvers().keySet().retainAll(scopeResolvers);
        }
        return model;
    }

    /**
     * Parse and validate the "attributes" field of the request body.
     *
     * @param requestBody The request body.
     * @return The parsed "attributes" field from the request body.
     * @throws BadRequestException
     */
    public static HashMap<String, HashSet<Object>> parseAttributes(JsonNode requestBody) throws BadRequestException {
        if (!requestBody.has("attributes"))
            throw new BadRequestException("The 'attributes' field is missing from the request body.");
        if (requestBody.get("attributes").size() == 0)
            throw new BadRequestException("The 'attributes' field of the request body must not be empty.");
        JsonNode attributes = requestBody.get("attributes");
        HashMap<String, HashSet<Object>> attributesObj = new HashMap<>();
        Iterator<String> attributeFields = attributes.fieldNames();
        while (attributeFields.hasNext()) {
            String attribute = attributeFields.next();
            attributesObj.put(attribute, new HashSet<>());
            if (attributes.get(attribute).isObject())
                throw new BadRequestException("'attributes." + attribute + "' must be a string or an array of values.");
            if (attributes.get(attribute).isArray()) {
                for (JsonNode valueNode : attributes.get(attribute)) {
                    if (valueNode.isObject() || valueNode.isArray())
                        throw new BadRequestException("'attributes." + attribute + "' must be a string or an array of values.");
                    Object value;
                    if (valueNode.isBoolean())
                        value = valueNode.booleanValue();
                    else if (valueNode.isDouble())
                        value = valueNode.doubleValue();
                    else if (valueNode.isFloat())
                        value = valueNode.floatValue();
                    else if (valueNode.isInt())
                        value = valueNode.intValue();
                    else if (valueNode.isLong())
                        value = valueNode.longValue();
                    else if (valueNode.isShort())
                        value = valueNode.shortValue();
                    else if (valueNode.isNull())
                        value = "";
                    else
                        value = valueNode.asText();
                    attributesObj.get(attribute).add(value);
                }
            } else {
                attributesObj.get(attribute).add(attributes.get(attribute).asText());
            }
        }
        return attributesObj;
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

                // Parse and validate the input attributes.
                HashMap<String, HashSet<Object>> inputAttributesObj = parseAttributes(requestBody);
                job.inputAttributes(inputAttributesObj);

                // Parse the entity type.
                String entityType = parseEntityType(entityTypeFromUrl, requestBody);

                // Parse and validate the entity model.
                Model model;
                if (entityType == null || entityType.equals(""))
                    model = parseEntityModel(requestBody);
                else
                    model = parseEntityModel(entityType, client);

                // Validate the "scope" field of the request body.
                if (requestBody.has("scope"))
                    if (!requestBody.get("scope").isNull() && !requestBody.get("scope").isObject())
                        throw new BadRequestException("The 'scope' field of the request body must be an object.");

                // Parse and validate the "scope.resolvers" field of the request body.
                if (requestBody.has("scope")) {
                    if (requestBody.get("scope").has("resolvers")) {
                        // Parse and validate the "scope.resolvers" field of the request body.
                        HashSet<String> scopeResolvers = parseScopeResolvers(requestBody);
                        // Intersect the "resolvers" field of the entity model with the "scope.resolvers" field.
                        model = filterResolvers(model, scopeResolvers);
                    }
                }
                if (model.resolvers().isEmpty())
                    throw new BadRequestException("No resolvers have been provided for the entity resolution job.");

                // Parse and validate the "scope.indices" field of the request body.
                if (requestBody.has("scope")) {
                    if (requestBody.get("scope").has("indices")) {
                        // Parse and validate the "scope.indices" field of the request body.
                        HashSet<String> scopeIndices = parseScopeIndices(requestBody);
                        // Intersect the "indices" field of the entity model with the "scope.indices" field.
                        model = filterIndices(model, scopeIndices);
                    }
                }
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
