package org.elasticsearch.plugin.zentity;

import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ModelsAction extends BaseRestHandler {

    public static final String INDEX_NAME = ".zentity-models";

    @Override
    public List<Route> routes() {
        return List.of(
                new Route(GET, "_zentity/models"),
                new Route(GET, "_zentity/models/{entity_type}"),
                new Route(POST, "_zentity/models/{entity_type}"),
                new Route(PUT, "_zentity/models/{entity_type}"),
                new Route(DELETE, "_zentity/models/{entity_type}")
        );
    }

    /**
     * Check if the .zentity-models index exists, and if it doesn't, then create it.
     *
     * @param client    The client that will communicate with Elasticsearch.
     * @param onSuccess The action to perform after the index creation request completes.
     */
    public static void ensureIndex(NodeClient client, ActionListener<ActionResponse> onSuccess) {

        // Check if the .zentity-model index exists.
        client.admin().indices().prepareExists(INDEX_NAME).execute(new ActionListener<>() {

            @Override
            public void onResponse(IndicesExistsResponse response) {
                if (!response.isExists()) {

                    // Index does not exist. Create it.
                    SetupAction.createIndex(client, new ActionListener<>() {

                        @Override
                        public void onResponse(CreateIndexResponse response) {
                            onSuccess.onResponse(response);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // TODO: Test if other exceptions are being swallowed
                        }
                    });
                } else {

                    // Index exists.
                    onSuccess.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ElasticsearchSecurityException) {
                    // TODO: Test handling of ElasticsearchSecurityException
                    throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                }
                // TODO: Test if other exceptions are being swallowed
            }
        });
    }

    /**
     * Retrieve all entity models.
     *
     * @param client    The client that will communicate with Elasticsearch.
     * @param onSuccess The action to perform after the search request completes.
     */
    public static void getEntityModels(NodeClient client, ActionListener<SearchResponse> onSuccess) {

        // Retrieve all entity models from the .zentity-models index.
        client.prepareSearch(INDEX_NAME).setSize(10000).execute(new ActionListener<>() {

            @Override
            public void onResponse(SearchResponse response) {
                // Successfully retrieved the entity models. Invoke the listener.
                onSuccess.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {

                // .zentity-models index does not exist. Create it.
                if (e instanceof IndexNotFoundException) {
                    // TODO: Test handling of IndexNotFoundException
                    SetupAction.createIndex(client, new ActionListener<>() {

                        @Override
                        public void onResponse(CreateIndexResponse createIndexResponse) {
                            // Try to retrieve entity models once more, and then invoke the listener.
                            client.prepareSearch(INDEX_NAME).setSize(10000).execute(onSuccess);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // TODO: Test if other exceptions are being swallowed
                        }
                    });
                } else if (e instanceof ElasticsearchSecurityException) {
                    // TODO: Test handling of ElasticsearchSecurityException
                    throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                }
                // TODO: Test if other exceptions are being swallowed
            }
        });
    }

    /**
     * Retrieve one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @param onSuccess  The action to perform after the get request completes.
     */
    public static void getEntityModel(String entityType, NodeClient client, ActionListener<GetResponse> onSuccess) {

        // Retrieve one entity model from the .zentity-models index.
        client.prepareGet(INDEX_NAME, "doc", entityType).execute(new ActionListener<>() {

            @Override
            public void onResponse(GetResponse response) {
                // Successfully retrieved the entity model. Invoke the listener.
                onSuccess.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {

                // .zentity-models index does not exist. Create it.
                if (e instanceof IndexNotFoundException) {
                    // TODO: Test handling of IndexNotFoundException
                    SetupAction.createIndex(client, new ActionListener<>() {

                        @Override
                        public void onResponse(CreateIndexResponse createIndexResponse) {
                            // Try to retrieve entity model once more, and then invoke the listener.
                            client.prepareGet(INDEX_NAME, "doc", entityType).execute(onSuccess);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // TODO: Test if other exceptions are being swallowed
                        }
                    });
                } else if (e instanceof ElasticsearchSecurityException) {
                    // TODO: Test handling of ElasticsearchSecurityException
                    throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                }
                // TODO: Test if other exceptions are being swallowed
            }
        });
    }

    /**
     * Index one entity model by its type. Return error if an entity model already exists for that entity type.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @param onSuccess   The action to perform after indexing the entity model.
     */
    public static void indexEntityModel(String entityType, String requestBody, NodeClient client, ActionListener<IndexResponse> onSuccess) {
        ensureIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(ActionResponse actionResponse) {
                client.prepareIndex(INDEX_NAME, "doc", entityType)
                        .setSource(requestBody, XContentType.JSON)
                        .setCreate(true)
                        .setRefreshPolicy("wait_for")
                        .execute(onSuccess);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ElasticsearchSecurityException) {
                    // TODO: Test handling of ElasticsearchSecurityException
                    throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                }
                // TODO: Test if other exceptions are being swallowed
            }
        });
    }

    /**
     * Update one entity model by its type. Does not support partial updates.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @param onSuccess   The action to perform after updating the entity model.
     */
    public static void updateEntityModel(String entityType, String requestBody, NodeClient client, ActionListener<IndexResponse> onSuccess) {
        ensureIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(ActionResponse actionResponse) {
                client.prepareIndex(INDEX_NAME, "doc", entityType)
                        .setSource(requestBody, XContentType.JSON)
                        .setCreate(false)
                        .setRefreshPolicy("wait_for")
                        .execute(onSuccess);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ElasticsearchSecurityException) {
                    // TODO: Test handling of ElasticsearchSecurityException
                    throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                }
                // TODO: Test if other exceptions are being swallowed
            }
        });
    }

    /**
     * Delete one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @param onSuccess  The action to perform after deleting the entity model.
     */
    private static void deleteEntityModel(String entityType, NodeClient client, ActionListener<DeleteResponse> onSuccess) {
        ensureIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(ActionResponse actionResponse) {
                client.prepareDelete(INDEX_NAME, "doc", entityType)
                        .setRefreshPolicy("wait_for")
                        .execute(onSuccess);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ElasticsearchSecurityException) {
                    // TODO: Test handling of ElasticsearchSecurityException
                    throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                }
                // TODO: Test if other exceptions are being swallowed
            }
        });
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
                    getEntityModels(client, new RestActionListener<>(channel) {

                        @Override
                        protected void processResponse(SearchResponse response) throws Exception {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
                        }
                    });

                } else if (method == GET && !entityType.equals("")) {

                    // GET _zentity/models/{entity_type}
                    getEntityModel(entityType, client, new RestActionListener<>(channel) {

                        @Override
                        protected void processResponse(GetResponse response) throws Exception {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
                        }
                    });

                } else if (method == POST && !entityType.equals("")) {

                    // POST _zentity/models/{entity_type}
                    if (requestBody.equals(""))
                        throw new ValidationException("Request body cannot be empty when indexing an entity model.");
                    indexEntityModel(entityType, requestBody, client, new RestActionListener<>(channel) {

                        @Override
                        protected void processResponse(IndexResponse response) throws Exception {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
                        }
                    });

                } else if (method == PUT && !entityType.equals("")) {

                    // PUT _zentity/models/{entity_type}
                    if (requestBody.equals(""))
                        throw new ValidationException("Request body cannot be empty when updating an entity model.");
                    updateEntityModel(entityType, requestBody, client, new RestActionListener<>(channel) {

                        @Override
                        protected void processResponse(IndexResponse response) throws Exception {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
                        }
                    });

                } else if (method == DELETE && !entityType.equals("")) {

                    // DELETE _zentity/models/{entity_type}
                    deleteEntityModel(entityType, client, new RestActionListener<>(channel) {

                        @Override
                        protected void processResponse(DeleteResponse response) throws Exception {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            content = response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
                        }
                    });

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
