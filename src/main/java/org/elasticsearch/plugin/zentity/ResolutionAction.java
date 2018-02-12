package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zentity.resolution.Job;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.omg.DynamicAny.NameDynAnyPair;

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
     * Parse the request body.
     *
     * @param requestBody The request body.
     * @return The parsed request body.
     * @throws BadRequestException
     * @throws IOException
     */
    public static JsonNode parseRequestBody(String requestBody) throws BadRequestException, IOException {
        if (requestBody == null || requestBody.equals(""))
            throw new BadRequestException("Request body is missing.");
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(requestBody);
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
     * Parse and validate the entity model.
     * Retrieve the entity model if an entity type was given.
     * Otherwise expect the entity model to be provided in the 'model' field of the request body.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The parsed "model" field from the request body, or an object from ".zentity-models" index.
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws IOException
     */
    public static JsonNode parseEntityModel(String entityType, JsonNode requestBody, NodeClient client) throws BadRequestException, NotFoundException, IOException {
        JsonNode entityModel;
        if (entityType == null || entityType.equals("")) {
            if (!requestBody.has("model"))
                throw new BadRequestException("The 'model' field is missing from the request body while 'entity_type' is undefined.");
            entityModel = requestBody.get("model");
        } else {
            String entityModelSource = ModelsAction.getEntityModel(entityType, client).getSourceAsString();
            if (entityModelSource == null || entityModelSource.equals(""))
                throw new NotFoundException("Model not found for entity type '" + entityType + "'.");
            ObjectMapper mapper = new ObjectMapper();
            entityModel = mapper.readTree(entityModelSource);
        }
        if (!entityModel.isObject())
            throw new BadRequestException("Entity model must be an object.");
        return entityModel;
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
            throw new BadRequestException("'attributes' field is missing from the request body.");
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
                        value = valueNode.isDouble();
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
        int maxDocsPerQuery = restRequest.paramAsInt("max_docs_per_hop", Job.DEFAULT_MAX_DOCS_PER_QUERY);
        int maxHops = restRequest.paramAsInt("max_hops", Job.DEFAULT_MAX_HOPS);
        Boolean pretty = restRequest.paramAsBoolean("pretty", Job.DEFAULT_PRETTY);
        Boolean profile = restRequest.paramAsBoolean("profile", Job.DEFAULT_PROFILE);

        // Parse other request params
        String indicesFilterFromUrl = restRequest.param("filter_indices", "");
        String resolversFilterFromUrl = restRequest.param("filter_resolvers", "");

        return channel -> {
            try {

                // Prepare the entity resolution job.
                Job job = new Job(client);
                job.setIncludeAttributes(includeAttributes);
                job.setIncludeHits(includeHits);
                job.setIncludeQueries(includeQueries);
                job.setIncludeSource(includeSource);
                job.setMaxDocsPerQuery(maxDocsPerQuery);
                job.setMaxHops(maxHops);
                job.setPretty(pretty);
                job.setProfile(profile);

                // Parse the request body.
                if (body == null || body.equals(""))
                    throw new BadRequestException("Request body is missing.");
                ObjectMapper mapper = new ObjectMapper();
                JsonNode requestBody = mapper.readTree(body);

                // Parse and validate the input attributes.
                HashMap<String, HashSet<Object>> inputAttributesObj = parseAttributes(requestBody);
                job.setInputAttributes(inputAttributesObj);

                // Parse the entity type.
                String entityType = parseEntityType(entityTypeFromUrl, requestBody);

                // Parse and validate the entity model.
                JsonNode entityModel = parseEntityModel(entityType, requestBody, client);

                // Parse and validate the "attributes" field of the entity model.
                HashMap<String, HashMap<String, String>> attributeObj = ModelsAction.parseAttributes(entityModel);
                if (attributeObj.isEmpty())
                    throw new BadRequestException("No attributes have been provided for the entity resolution job.");
                job.setAttributes(attributeObj);

                // Parse and validate the "resolvers" field of the entity model.
                HashMap<String, ArrayList<String>> resolversObj = ModelsAction.parseResolvers(entityModel);
                // Parse and validate the "filter_resolvers" field of the request body or URL.
                HashSet<String> resolversFilter = ModelsAction.parseResolversFilter(resolversFilterFromUrl, requestBody);
                // Intersect the "resolvers" field of the entity model with the "filter_resolvers" field.
                resolversObj = ModelsAction.filterResolvers(resolversObj, resolversFilter);
                if (resolversObj.isEmpty())
                    throw new BadRequestException("No resolvers have been provided for the entity resolution job.");
                job.setResolvers(resolversObj);

                // Parse and validate the "indices" field of the entity model.
                HashMap<String, HashMap<String, String>> indicesObj = ModelsAction.parseIndices(entityModel);
                // Parse and validate the "filter_indices" field of the request body or URL.
                HashSet<String> indicesFilter = ModelsAction.parseIndicesFilter(indicesFilterFromUrl, requestBody);
                // Intersect the "indices" field of the entity model with the "filter_indices" field.
                indicesObj = ModelsAction.filterIndices(indicesObj, indicesFilter);
                if (indicesObj.isEmpty())
                    throw new BadRequestException("No indices have been provided for the entity resolution job.");
                job.setIndices(indicesObj);

                // Run the entity resolution job.
                String response = job.run();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response));

            } catch (BadRequestException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (NotFoundException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_FOUND, e));
            }
        };
    }
}
