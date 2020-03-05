package org.elasticsearch.plugin.zentity;

import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import org.elasticsearch.ElasticsearchSecurityException;
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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ModelsAction extends BaseRestHandler {

    public static final String INDEX_NAME = ".zentity-models";

    @Inject
    public ModelsAction(RestController controller) {
        controller.registerHandler(GET, "_zentity/models", this);
        controller.registerHandler(GET, "_zentity/models/{entity_type}", this);
        controller.registerHandler(POST, "_zentity/models/{entity_type}", this);
        controller.registerHandler(PUT, "_zentity/models/{entity_type}", this);
        controller.registerHandler(DELETE, "_zentity/models/{entity_type}", this);
    }

    /**
     * Check if the .zentity-models index exists, and if it doesn't, then create it.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @throws ForbiddenException
     */
    public static void ensureIndex(NodeClient client) throws ForbiddenException {
        try {
            IndicesExistsRequestBuilder request = client.admin().indices().prepareExists(INDEX_NAME);
            IndicesExistsResponse response = request.get();
            if (!response.isExists())
                SetupAction.createIndex(client);
        } catch (ElasticsearchSecurityException se) {
            throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
        }
    }

    /**
     * Retrieve all entity models.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException
     */
    public static SearchResponse getEntityModels(NodeClient client) throws ForbiddenException {
        SearchRequestBuilder request = client.prepareSearch(INDEX_NAME);
        request.setSize(10000);
        try {
            return request.get();
        } catch (IndexNotFoundException e) {
            try {
                SetupAction.createIndex(client);
            } catch (ElasticsearchSecurityException se) {
                throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
            }
            return request.get();
        }
    }

    /**
     * Retrieve one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException
     */
    public static GetResponse getEntityModel(String entityType, NodeClient client) throws ForbiddenException {
        GetRequestBuilder request = client.prepareGet(INDEX_NAME, "doc", entityType);
        try {
            return request.get();
        } catch (IndexNotFoundException e) {
            try {
                SetupAction.createIndex(client);
            } catch (ElasticsearchSecurityException se) {
                throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
            }
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
     * @throws ForbiddenException
     */
    public static IndexResponse indexEntityModel(String entityType, String requestBody, NodeClient client) throws ForbiddenException {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX_NAME, "doc", entityType);
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
     * @throws ForbiddenException
     */
    public static IndexResponse updateEntityModel(String entityType, String requestBody, NodeClient client) throws ForbiddenException {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX_NAME, "doc", entityType);
        request.setSource(requestBody, XContentType.JSON).setCreate(false).setRefreshPolicy("wait_for");
        return request.get();
    }

    /**
     * Delete one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException
     */
    private static DeleteResponse deleteEntityModel(String entityType, NodeClient client) throws ForbiddenException {
        DeleteRequestBuilder request = client.prepareDelete(INDEX_NAME, "doc", entityType);
        request.setRefreshPolicy("wait_for");
        try {
            return request.get();
        } catch (IndexNotFoundException e) {
            try {
                SetupAction.createIndex(client);
            } catch (ElasticsearchSecurityException se) {
                throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
            }
            return request.get();
        }
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
                        throw new ValidationException("Request body is missing.");

                    // Parse and validate the entity model.
                    new Model(requestBody);
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
                    if (requestBody.equals(""))
                        throw new ValidationException("Request body cannot be empty when indexing an entity model.");
                    IndexResponse response = indexEntityModel(entityType, requestBody, client);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

                } else if (method == PUT && !entityType.equals("")) {
                    // PUT _zentity/models/{entity_type}
                    if (requestBody.equals(""))
                        throw new ValidationException("Request body cannot be empty when updating an entity model.");
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

            } catch (ValidationException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (ForbiddenException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.FORBIDDEN, e));
            } catch (NotImplementedException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_IMPLEMENTED, e));
            }
        };
    }
}
