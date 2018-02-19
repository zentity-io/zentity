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
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.*;

import java.io.IOException;
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

    public static boolean hasInvalidChars(String value) {
        return INVALID_CHARS.matcher(value).find();
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
     * Index one entity model by its type. Return error if an entity model already exists for that entity type.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    public static IndexResponse indexEntityModel(String entityType, String requestBody, NodeClient client) {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX, "doc", entityType);
        request.setSource(requestBody, XContentType.JSON).setCreate(true).setRefreshPolicy("wait_for");
        return request.get();
    }

    /**
     * Update one entity model by its type. Does not support partial updates.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    public static IndexResponse updateEntityModel(String entityType, String requestBody, NodeClient client) {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX, "doc", entityType);
        request.setSource(requestBody, XContentType.JSON).setCreate(false).setRefreshPolicy("wait_for");
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
            throw new BadRequestException("The 'attributes' field of the entity model must be an object.");
        if (entityModel.get("attributes").size() == 0)
            throw new BadRequestException("The 'attributes' field of the entity model must not be empty.");
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, HashMap<String, String>> attributesObj = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> attributes = entityModel.get("attributes").fields();
        while (attributes.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributes.next();
            if (attribute.getKey().equals(""))
                throw new BadRequestException("The 'attributes' field of the entity model has an attribute with an empty name.");
            if (hasInvalidChars(attribute.getKey()))
                throw new BadRequestException("'attributes." + attribute.getKey() + "' must not have periods in its name.");
            if (!attribute.getValue().isObject())
                throw new BadRequestException("'attributes." + attribute.getKey() + "' must be an object.");
            if (attribute.getValue().size() == 0)
                throw new BadRequestException("'attributes." + attribute.getKey() + "' is empty.");
            attributesObj.put(attribute.getKey(), new HashMap<>());
            Iterator<Map.Entry<String, JsonNode>> matchers = entityModel.get("attributes").get(attribute.getKey()).fields();
            while (matchers.hasNext()) {
                Map.Entry<String, JsonNode> matcher = matchers.next();
                if (matcher.getKey().equals(""))
                    throw new BadRequestException("'attributes." + attribute.getKey() + "' has a matcher with an empty name.");
                if (hasInvalidChars(matcher.getKey()))
                    throw new BadRequestException("'attributes." + attribute.getKey() + "." + matcher.getKey() + "' must not have periods in its name.");
                if (!matcher.getValue().isObject())
                    throw new BadRequestException("'attributes." + attribute.getKey() + "." + matcher.getKey() + "' must be an object.");
                if (matcher.getValue().size() == 0)
                    throw new BadRequestException("'attributes." + attribute.getKey() + "." + matcher.getKey() + "' is empty.");
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
            if (index.equals(""))
                throw new BadRequestException("The 'indices' field of the entity model has an index with an empty name.");
            if (!entityModel.get("indices").get(index).isObject())
                throw new BadRequestException("'indices." + index + "' must be an object.");
            if (entityModel.get("indices").get(index).size() == 0)
                throw new BadRequestException("'indices." + index + "' is empty.");
            Iterator<String> modelFields = entityModel.get("indices").get(index).fieldNames();
            while (modelFields.hasNext()) {
                String modelField = modelFields.next();
                JsonNode indexFieldNode = entityModel.get("indices").get(index).get(modelField);
                if (modelField.equals(""))
                    throw new BadRequestException("'indices." + index + "' has an attribute matcher with an empty name.");
                if (modelField.split("\\.").length != 2)
                    throw new BadRequestException("'indices." + index + "." + modelField + "' must be formatted as 'ATTRIBUTE_NAME'.'MATCHER_NAME'.");
                if (!indexFieldNode.isTextual())
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
            throw new BadRequestException("The 'resolvers' field of the entity model must be an object.");
        if (entityModel.get("resolvers").size() == 0)
            throw new BadRequestException("The 'resolvers' field of the entity model must not be empty.");
        HashMap<String, ArrayList<String>> resolversObj = new HashMap<>();
        Iterator<String> resolvers = entityModel.get("resolvers").fieldNames();
        while (resolvers.hasNext()) {
            String resolver = resolvers.next();
            if (resolver.equals(""))
                throw new BadRequestException("The 'resolvers' field of the entity model has a resolver with an empty name.");
            if (hasInvalidChars(resolver))
                throw new BadRequestException("'resolver." + resolver + "' must not have periods in its name.");
            resolversObj.put(resolver, new ArrayList<>());
            if (!entityModel.get("resolvers").get(resolver).isArray())
                throw new BadRequestException("'resolver." + resolver + "' must be an array of strings.");
            if (entityModel.get("resolvers").get(resolver).size() == 0)
                throw new BadRequestException("'resolver." + resolver + "' is empty.");
            for (JsonNode attributeNode : entityModel.get("resolvers").get(resolver)) {
                if (!attributeNode.isTextual())
                    throw new BadRequestException("'resolver." + resolver + "' must be an array of strings.");
                String attributeName = attributeNode.asText();
                if (attributeName == null || attributeName.equals(""))
                    throw new BadRequestException("'resolver." + resolver + "' must be an array of strings.");
                if (hasInvalidChars(attributeName))
                    throw new BadRequestException("'resolver." + resolver + "' must not have periods in its values.");
                resolversObj.get(resolver).add(attributeName);
            }
        }
        return resolversObj;
    }

    /**
     * Parse and validate the entity model.
     *
     * @param model The entity model.
     * @return The parsed "model" field from the request body, or an object from ".zentity-models" index.
     * @throws BadRequestException
     * @throws IOException
     */
    public static JsonNode parseEntityModel(String model) throws BadRequestException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode entityModel = mapper.readTree(model);
        if (!entityModel.isObject())
            throw new BadRequestException("Entity model must be an object.");

        // Parse and validate the "attributes" field of the entity model.
        HashMap<String, HashMap<String, String>> attributeObj = parseAttributes(entityModel);
        if (attributeObj.isEmpty())
            throw new BadRequestException("No attributes have been provided for the entity model.");

        // Parse and validate the "resolvers" field of the entity model.
        HashMap<String, ArrayList<String>> resolversObj = parseResolvers(entityModel);
        if (resolversObj.isEmpty())
            throw new BadRequestException("No resolvers have been provided for the entity model.");

        // Parse and validate the "indices" field of the entity model.
        HashMap<String, HashMap<String, String>> indicesObj = parseIndices(entityModel);
        if (indicesObj.isEmpty())
            throw new BadRequestException("No indices have been provided for the entity model.");
        return entityModel;
    }

    @Override
    public String getName() {
        return "zentity_models_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        String entityType = restRequest.param("entity_type");
        Boolean pretty = restRequest.paramAsBoolean("pretty", false);
        Method method = restRequest.method();
        String requestBody = restRequest.content().utf8ToString();

        return channel -> {
            try {

                // Validate input
                if (method == POST || method == PUT) {

                    // Parse the request body.
                    if (requestBody == null || requestBody.equals(""))
                        throw new BadRequestException("Request body is missing.");

                    // Parse and validate the entity model.
                    JsonNode entityModel = parseEntityModel(requestBody);
                }

                // Handle request
                if (method == GET && (entityType == null || entityType.equals(""))) {
                    // GET _zentity/models
                    SearchResponse response = getEntityModels(client);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

                } else if (method == GET && !entityType.equals("")) {
                    // GET _zentity/models/{entity_type}
                    GetResponse response = getEntityModel(entityType, client);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

                } else if (method == POST && !entityType.equals("")) {
                    // POST _zentity/models/{entity_type}
                    if (requestBody == null || requestBody.equals(""))
                        throw new BadRequestException("Request body cannot be empty when indexing an entity model.");
                    IndexResponse response = indexEntityModel(entityType, requestBody, client);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

                } else if (method == PUT && !entityType.equals("")) {
                    // PUT _zentity/models/{entity_type}
                    if (requestBody == null || requestBody.equals(""))
                        throw new BadRequestException("Request body cannot be empty when updating an entity model.");
                    IndexResponse response = updateEntityModel(entityType, requestBody, client);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

                } else if (method == DELETE && !entityType.equals("")) {
                    // DELETE _zentity/models/{entity_type}
                    DeleteResponse response = deleteEntityModel(entityType, client);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

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
