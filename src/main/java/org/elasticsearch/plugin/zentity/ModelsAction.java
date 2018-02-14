package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.*;

import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.*;

public class ModelsAction extends BaseRestHandler {

    public static final String INDEX = ".zentity-models";
    private static final Pattern INVALID_CHARS = Pattern.compile("\\.");

    @Inject
    public ModelsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "_zentity/models", this);
        controller.registerHandler(GET, "_zentity/models/{entity_type}", this);
        controller.registerHandler(POST, "_zentity/models/{entity_type}", this);
        controller.registerHandler(PUT, "_zentity/models/{entity_type}", this);
        controller.registerHandler(DELETE, "_zentity/models/{entity_type}", this);
    }

    public static void createIndex(NodeClient client) {
        client.admin().indices().prepareCreate(INDEX)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                )
                .addMapping("doc",
                        "{\n" +
                                "  \"doc\": {\n" +
                                "    \"dynamic\": \"strict\",\n" +
                                "    \"properties\": {\n" +
                                "      \"attributes\": {\n" +
                                "        \"type\": \"object\",\n" +
                                "        \"enabled\": false\n" +
                                "      },\n" +
                                "      \"indices\": {\n" +
                                "        \"type\": \"object\",\n" +
                                "        \"enabled\": false\n" +
                                "      },\n" +
                                "      \"resolvers\": {\n" +
                                "        \"type\": \"object\",\n" +
                                "        \"enabled\": false\n" +
                                "      }\n" +
                                "    }\n" +
                                "  }\n" +
                                "}",
                        XContentType.JSON
                )
                .get();
    }

    /**
     * Check if the .zentity-models index exists, and if it doesn't, then create it.
     *
     * @param client The client that will communicate with Elasticsearch.
     */
    public static void ensureIndex(NodeClient client) {
        IndicesExistsRequestBuilder request = client.admin().indices().prepareExists(INDEX);
        IndicesExistsResponse response = request.get();
        if (!response.isExists())
            createIndex(client);
    }

    /**
     * Retrieve all entity models.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    public static SearchResponse getEntityModels(NodeClient client) {
        SearchRequestBuilder request = client.prepareSearch(INDEX);
        request.setSize(10000);
        try {
            return request.get();
        } catch (IndexNotFoundException e) {
            createIndex(client);
            return request.get();
        }
    }

    /**
     * Retrieve one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    public static GetResponse getEntityModel(String entityType, NodeClient client) {
        GetRequestBuilder request = client.prepareGet(INDEX, "doc", entityType);
        try {
            return request.get();
        } catch (IndexNotFoundException e) {
            createIndex(client);
            return request.get();
        }
    }

    /**
     * Index one entity model by its type.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    public static IndexResponse indexEntityModel(String entityType, String requestBody, NodeClient client) {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX, "doc", entityType);
        request.setSource(requestBody, XContentType.JSON).setRefreshPolicy("wait_for");
        return request.get();
    }

    /**
     * Update one entity model by its type. Supports partial updates.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    public static UpdateResponse updateEntityModel(String entityType, String requestBody, NodeClient client) {
        ensureIndex(client);
        UpdateRequestBuilder request = client.prepareUpdate(INDEX, "doc", entityType);
        request.setDoc(requestBody, XContentType.JSON).setDocAsUpsert(true).setRefreshPolicy("wait_for");
        return request.get();
    }

    /**
     * Delete one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    private static DeleteResponse deleteEntityModel(String entityType, NodeClient client) {
        DeleteRequestBuilder request = client.prepareDelete(INDEX, "doc", entityType);
        request.setRefreshPolicy("wait_for");
        try {
            return request.get();
        } catch (IndexNotFoundException e) {
            createIndex(client);
            return request.get();
        }
    }

    /**
     * Parse and validate the "attributes" field of an entity model.
     *
     * @param entityModel The entity model.
     * @return Parsed "attributes" object from the entity model.
     * @throws BadRequestException
     * @throws JsonProcessingException
     */
    public static HashMap<String, HashMap<String, String>> parseAttributes(JsonNode entityModel) throws BadRequestException, JsonProcessingException {
        if (!entityModel.has("attributes"))
            throw new BadRequestException("The 'attributes' field is missing from the entity model.");
        if (!entityModel.get("attributes").isObject())
            throw new BadRequestException("The 'attributes' field of the entity model  must be an object.");
        if (entityModel.get("attributes").size() == 0)
            throw new BadRequestException("The 'attributes' field of the entity model must not be empty.");
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, HashMap<String, String>> attributesObj = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> attributes = entityModel.get("attributes").fields();
        while (attributes.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributes.next();
            if (INVALID_CHARS.matcher(attribute.getKey()).find())
                throw new BadRequestException("'attributes." + attribute + "'must not have periods in the field name.");
            if (!attribute.getValue().isObject())
                throw new BadRequestException("'attributes." + attribute + "' must be an object.");
            if (attribute.getValue().size() == 0)
                throw new BadRequestException("'attributes." + attribute + "' is empty.");
            attributesObj.put(attribute.getKey(), new HashMap<>());
            Iterator<Map.Entry<String, JsonNode>> matchers = entityModel.get("attributes").get(attribute.getKey()).fields();
            while (matchers.hasNext()) {
                Map.Entry<String, JsonNode> matcher = matchers.next();
                if (INVALID_CHARS.matcher(matcher.getKey()).find())
                    throw new BadRequestException("'attributes." + matcher + "' must not have periods in the field name.");
                if (!attribute.getValue().isObject())
                    throw new BadRequestException("'attributes." + attribute + "." + matcher + "' must be an object.");
                if (attribute.getValue().size() == 0)
                    throw new BadRequestException("'attributes." + attribute + "." + matcher + "' is empty.");
                attributesObj.get(attribute.getKey()).put(matcher.getKey(), mapper.writeValueAsString(matcher.getValue()));
            }
        }
        return attributesObj;
    }

    /**
     * Parse and validate the "indices" field of an entity model.
     *
     * @param entityModel The entity model.
     * @return Parsed "indices" object from the entity model.
     * @throws BadRequestException
     */
    public static HashMap<String, HashMap<String, String>> parseIndices(JsonNode entityModel) throws BadRequestException {
        if (!entityModel.has("indices"))
            throw new BadRequestException("The 'indices' field is missing from the entity model.");
        if (!entityModel.get("indices").isObject())
            throw new BadRequestException("The 'indices' field of the entity model must be an object.");
        if (entityModel.get("indices").size() == 0)
            throw new BadRequestException("The 'indices' field of the entity model must not be empty.");
        HashMap<String, HashMap<String, String>> indicesObj = new HashMap<>();
        Iterator<String> indices = entityModel.get("indices").fieldNames();
        while (indices.hasNext()) {
            String index = indices.next();
            indicesObj.put(index, new HashMap<>());
            if (!entityModel.get("indices").get(index).isObject())
                throw new BadRequestException("'indices." + index + "' must be an object.");
            if (entityModel.get("indices").get(index).size() == 0)
                throw new BadRequestException("'indices." + index + "' is empty.");
            Iterator<String> modelFields = entityModel.get("indices").get(index).fieldNames();
            while (modelFields.hasNext()) {
                String modelField = modelFields.next();
                JsonNode indexFieldNode = entityModel.get("indices").get(index).get(modelField);
                if (indexFieldNode.isObject() || indexFieldNode.isArray())
                    throw new BadRequestException("'indices." + index + "." + modelField + "' must be a string.");
                String indexField = indexFieldNode.asText();
                if (indexField == null || indexField.equals(""))
                    throw new BadRequestException("'indices." + index + "." + modelField + "' is empty.");
                indicesObj.get(index).put(modelField, indexField);
            }
        }
        return indicesObj;
    }

    /**
     * Parse and validate the "indices" field of the request body or URL.
     *
     * @param indicesFilterFromUrl The value of the "indices" parameter of the URL.
     * @param requestBody          The request body.
     * @return Names of indices to filter from the "indices" object of the entity model.
     * @throws BadRequestException
     */
    public static HashSet<String> parseIndicesFilter(String indicesFilterFromUrl, JsonNode requestBody) throws BadRequestException {
        HashSet<String> indicesFilter = new HashSet<>();
        if (indicesFilterFromUrl != null && !indicesFilterFromUrl.equals("")) {
            for (String index : indicesFilterFromUrl.split(",")) {
                if (index == null || index.equals(""))
                    continue;
                indicesFilter.add(index);
            }
        } else if (requestBody.has("indices")) {
            if (!requestBody.get("indices").isArray())
                throw new BadRequestException("The 'indices' field of the request body must be an array of strings.");
            for (JsonNode indexNode : requestBody.get("indices")) {
                String index = indexNode.asText();
                if (index == null || index.equals(""))
                    continue;
                indicesFilter.add(index);
            }
        }
        return indicesFilter;
    }

    /**
     * Filter indices based on what the client wants to use.
     *
     * @param indicesObj    The parsed "indices" object of an entity model.
     * @param indicesFilter Names of indices to filter from the "indices" object.
     * @return Filtered "indices" object.
     * @throws BadRequestException
     */
    public static HashMap<String, HashMap<String, String>> filterIndices(HashMap<String, HashMap<String, String>> indicesObj, Set<String> indicesFilter) throws BadRequestException {
        if (!indicesFilter.isEmpty()) {
            for (String index : indicesFilter) {
                if (index == null || index.equals(""))
                    continue;
                if (!indicesObj.containsKey(index))
                    throw new BadRequestException("'" + index + "' is not in the 'indices' field of the entity model.");
            }
            indicesObj.keySet().retainAll(indicesFilter);
        }
        return indicesObj;
    }

    /**
     * Parse and validate the "resolvers" field of an entity model.
     *
     * @param entityModel The entity model.
     * @return Parsed "resolvers" object from the entity model.
     * @throws BadRequestException
     */
    public static HashMap<String, ArrayList<String>> parseResolvers(JsonNode entityModel) throws BadRequestException {
        if (!entityModel.has("resolvers"))
            throw new BadRequestException("The 'resolvers' field is missing from the entity model.");
        if (!entityModel.get("resolvers").isObject())
            throw new BadRequestException("The 'resolvers' field of the entity model  must be an object.");
        if (entityModel.get("resolvers").size() == 0)
            throw new BadRequestException("The 'resolvers' field of the entity model  must not be empty.");
        HashMap<String, ArrayList<String>> resolversObj = new HashMap<>();
        Iterator<String> resolvers = entityModel.get("resolvers").fieldNames();
        while (resolvers.hasNext()) {
            String resolver = resolvers.next();
            if (INVALID_CHARS.matcher(resolver).find())
                throw new BadRequestException("'resolver." + resolver + "' must not have periods in the field name.");
            resolversObj.put(resolver, new ArrayList<>());
            if (!entityModel.get("resolvers").get(resolver).isArray())
                throw new BadRequestException("'resolver." + resolver + "' must be an array of strings.");
            if (entityModel.get("resolvers").get(resolver).size() == 0)
                throw new BadRequestException("'resolver." + resolver + "' is empty.");
            for (JsonNode attributeNode : entityModel.get("resolvers").get(resolver)) {
                if (attributeNode.isArray() || attributeNode.isObject())
                    throw new BadRequestException("'resolver." + resolver + "' must be an array of strings.");
                String attributeName = attributeNode.asText();
                if (attributeName == null || attributeName.equals(""))
                    throw new BadRequestException("'resolver." + resolver + "' must be an array of strings.");
                resolversObj.get(resolver).add(attributeName);
            }
        }
        return resolversObj;
    }

    /**
     * Parse and validate the "resolvers" field of the request body or URL.
     *
     * @param resolversFilterFromUrl The value of the "resolvers" parameter of the URL.
     * @param requestBody            The request body.
     * @return Names of resolvers to filter from the "resolvers" object of the entity model.
     * @throws BadRequestException
     */
    public static HashSet<String> parseResolversFilter(String resolversFilterFromUrl, JsonNode requestBody) throws BadRequestException {
        HashSet<String> resolversFilter = new HashSet<>();
        if (resolversFilterFromUrl != null && !resolversFilterFromUrl.equals("")) {
            for (String resolver : resolversFilterFromUrl.split(",")) {
                if (resolver == null || resolver.equals(""))
                    continue;
                resolversFilter.add(resolver);
            }
        } else if (requestBody.has("resolvers")) {
            if (!requestBody.get("resolvers").isArray())
                throw new BadRequestException("The 'resolvers' field of the request body must be an array of strings.");
            for (JsonNode resolverNode : requestBody.get("resolvers")) {
                String resolver = resolverNode.asText();
                if (resolver == null || resolver.equals(""))
                    continue;
                resolversFilter.add(resolver);
            }
        }
        return resolversFilter;
    }

    /**
     * Filter resolvers based on what the client wants to use.
     *
     * @param resolversObj    The parsed "resolvers" object of an entity model.
     * @param resolversFilter Names of resolvers to filter from the "resolvers" object.
     * @return Filtered "resolvers" object.
     * @throws BadRequestException
     */
    public static HashMap<String, ArrayList<String>> filterResolvers(HashMap<String, ArrayList<String>> resolversObj, Set<String> resolversFilter) throws BadRequestException {
        if (!resolversFilter.isEmpty()) {
            for (String resolver : resolversFilter) {
                if (resolver == null || resolver.equals(""))
                    continue;
                if (!resolversObj.containsKey(resolver))
                    throw new BadRequestException("'" + resolver + "' is not in the 'resolvers' field of the entity model.");
            }
            resolversObj.keySet().retainAll(resolversFilter);
        }
        return resolversObj;
    }

    @Override
    public String getName() {
        return "zentity_models_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        String entityType = restRequest.param("entity_type");
        Method method = restRequest.method();
        String requestBody = restRequest.content().utf8ToString();

        return channel -> {
            try {

                if (method == GET && (entityType == null || entityType.equals(""))) {
                    // GET _zentity/models
                    SearchResponse response = getEntityModels(client);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response.toString()));

                } else if (method == GET && !entityType.equals("")) {
                    // GET _zentity/models/{entity_type}
                    GetResponse response = getEntityModel(entityType, client);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response.toString()));

                } else if (method == POST && !entityType.equals("")) {
                    // POST _zentity/models/{entity_type}
                    if (requestBody == null || requestBody.equals(""))
                        throw new BadRequestException("Request body cannot be empty when indexing an entity model.");
                    IndexResponse response = indexEntityModel(entityType, requestBody, client);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response.toString()));

                } else if (method == PUT && !entityType.equals("")) {
                    // PUT _zentity/models/{entity_type}
                    if (requestBody == null || requestBody.equals(""))
                        throw new BadRequestException("Request body cannot be empty when updating an entity model.");
                    UpdateResponse response = updateEntityModel(entityType, requestBody, client);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response.toString()));

                } else if (method == DELETE && !entityType.equals("")) {
                    // DELETE _zentity/models/{entity_type}
                    DeleteResponse response = deleteEntityModel(entityType, client);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response.toString()));

                } else {
                    throw new NotImplementedException("Method and endpoint not implemented.");
                }

            } catch (BadRequestException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (NotImplementedException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_IMPLEMENTED, e));
            }
        };
    }
}
