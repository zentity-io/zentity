package org.elasticsearch.plugin.zentity;

import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.rest.RestRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ModelsAction extends BaseRestHandler {


    private static final Logger logger = LogManager.getLogger(ModelsAction.class);
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
     * Create the .zentity-models index.
     *
     * @param client     The client that will communicate with Elasticsearch.
     * @param onComplete The action to perform after the index creation request completes.
     */
    public static void createIndex(NodeClient client, ActionListener<ActionResponse> onComplete) {
        SetupAction.createIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(CreateIndexResponse response) {
                try {

                    // The index was created successfully.
                    onComplete.onResponse(response);
                } catch (Exception e) {

                    // An unexpected error occurred when returning the response.
                    onComplete.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when creating the index.
                if (e instanceof ElasticsearchSecurityException) {

                    // The error was a security exception.
                    // Log the error message as it was received from Elasticsearch.
                    logger.debug(e.getMessage());

                    // Return a more descriptive error message for the user.
                    onComplete.onFailure(new ForbiddenException("The '" + INDEX_NAME + "' index does not exist and cannot be created. This action requires the 'create_index' privilege for the '" + INDEX_NAME + "' index. An authorized user can create the '" + INDEX_NAME + "' index using the Setup API: POST _zentity/_setup"));
                } else {

                    // The error was unexpected.
                    onComplete.onFailure(e);
                }
            }
        });
    }

    /**
     * Check if the .zentity-models index exists, and if it doesn't, then create it.
     *
     * @param client     The client that will communicate with Elasticsearch.
     * @param onComplete The action to perform after the index creation request completes.
     */
    public static void ensureIndex(NodeClient client, ActionListener<ActionResponse> onComplete) {

        // Check if the .zentity-model index exists.
        client.admin().indices().prepareExists(INDEX_NAME).execute(new ActionListener<>() {

            @Override
            public void onResponse(IndicesExistsResponse response) {
                if (!response.isExists()) {

                    // The index does not exist. Create it.
                    createIndex(client, new ActionListener<>() {

                        @Override
                        public void onResponse(ActionResponse response) {

                            // Successfully created the index.
                            onComplete.onResponse(response);
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when creating the index.
                            onComplete.onFailure(e);
                        }
                    });
                } else {

                    // The index already exists.
                    onComplete.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when checking if the index exists.
                if (e instanceof ElasticsearchSecurityException) {

                    // The error was a security exception.
                    // Log the error message as it was received from Elasticsearch.
                    logger.debug(e.getMessage());

                    // Return a more descriptive error message for the user.
                    onComplete.onFailure(new ForbiddenException("Unable to verify if the '" + INDEX_NAME + "' index exists. This action requires the 'view_index_metadata' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                } else {

                    // The error was unexpected.
                    onComplete.onFailure(e);
                }
            }
        });
    }

    /**
     * Retrieve all entity models.
     *
     * @param client     The client that will communicate with Elasticsearch.
     * @param onComplete The action to perform after the search request completes.
     */
    public static void getEntityModels(NodeClient client, ActionListener<SearchResponse> onComplete) {

        // Retrieve entity models from the .zentity-models index.
        client.prepareSearch(INDEX_NAME).setSize(10000).execute(new ActionListener<>() {

            @Override
            public void onResponse(SearchResponse response) {

                // Successfully retrieved the entity models.
                onComplete.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when retrieving the entity models.
                if (e instanceof IndexNotFoundException) {

                    // The .zentity-models index does not exist.
                    // Log the error message as it was received from Elasticsearch.
                    logger.debug(e.getMessage());

                    // Attempt to create the .zentity-models index.
                    createIndex(client, new ActionListener<>() {

                        @Override
                        public void onResponse(ActionResponse response) {

                            // Successfully created the .zentity-models index.
                            // Attempt to retrieve the entity models again.
                            getEntityModels(client, onComplete);
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when creating the .zentity-models index.
                            onComplete.onFailure(e);
                        }
                    });

                } else if (e.getClass() == ElasticsearchSecurityException.class) {

                    // The error was a security exception.
                    // Log the error message as it was received from Elasticsearch.
                    logger.debug(e.getMessage());

                    // Return a more descriptive error message for the user.
                    onComplete.onFailure(new ForbiddenException("Unable to retrieve the entity models. This action requires the 'read' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));

                } else {

                    // The error was unexpected.
                    onComplete.onFailure(e);
                }
            }
        });
    }

    /**
     * Retrieve one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @param onComplete The action to perform after the get request completes.
     */
    public static void getEntityModel(String entityType, NodeClient client, ActionListener<GetResponse> onComplete) {

        // Retrieve one entity model from the .zentity-models index.
        client.prepareGet(INDEX_NAME, "doc", entityType).execute(new ActionListener<>() {

            @Override
            public void onResponse(GetResponse response) {

                // Successfully retrieved the entity model.
                onComplete.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when retrieving the entity model.
                if (e instanceof IndexNotFoundException) {

                    // The .zentity-models index does not exist.
                    // Log the error message as it was received from Elasticsearch.
                    logger.debug(e.getMessage());

                    // Attempt to create the .zentity-models index.
                    createIndex(client, new ActionListener<>() {

                        @Override
                        public void onResponse(ActionResponse response) {

                            // Successfully created the .zentity-models index.
                            // Attempt to retrieve the entity model again.
                            getEntityModel(entityType, client, onComplete);
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when creating the .zentity-models index.
                            onComplete.onFailure(e);
                        }
                    });

                } else if (e.getClass() == ElasticsearchSecurityException.class) {

                    // The error was a security exception.
                    // Log the error message as it was received from Elasticsearch.
                    logger.debug(e.getMessage());

                    // Return a more descriptive error message for the user.
                    onComplete.onFailure(new ForbiddenException("Unable to retrieve the entity model. This action requires the 'read' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));

                } else {

                    // The error was unexpected.
                    onComplete.onFailure(e);
                }
            }
        });
    }

    /**
     * Index one entity model by its type. Return error if an entity model already exists for that entity type.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @param onComplete  The action to perform after indexing the entity model.
     */
    public static void indexEntityModel(String entityType, String requestBody, NodeClient client, ActionListener<IndexResponse> onComplete) throws ValidationException {
        Model.validateStrictName(entityType);
        ensureIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(ActionResponse actionResponse) {
                try {

                    // Index the entity model.
                    client.prepareIndex(INDEX_NAME, "doc", entityType)
                            .setSource(requestBody, XContentType.JSON)
                            .setCreate(true)
                            .setRefreshPolicy("wait_for")
                            .execute(onComplete);
                } catch (Exception e) {

                    // An error occurred when indexing the entity model.
                    onComplete.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when ensuring that the '.zentity-models' index existed.
                onComplete.onFailure(e);
            }
        });
    }

    /**
     * Update one entity model by its type. Does not support partial updates.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @param onComplete  The action to perform after updating the entity model.
     */
    public static void updateEntityModel(String entityType, String requestBody, NodeClient client, ActionListener<IndexResponse> onComplete) throws ValidationException {
        Model.validateStrictName(entityType);
        ensureIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(ActionResponse actionResponse) {
                try {

                    // Update the entity model.
                    client.prepareIndex(INDEX_NAME, "doc", entityType)
                            .setSource(requestBody, XContentType.JSON)
                            .setCreate(false)
                            .setRefreshPolicy("wait_for")
                            .execute(onComplete);
                } catch (Exception e) {

                    // An error occurred when updating the entity model.
                    onComplete.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when ensuring that the '.zentity-models' index existed.
                onComplete.onFailure(e);
            }
        });
    }

    /**
     * Delete one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @param onComplete The action to perform after deleting the entity model.
     */
    public static void deleteEntityModel(String entityType, NodeClient client, ActionListener<DeleteResponse> onComplete) {
        ensureIndex(client, new ActionListener<>() {

            @Override
            public void onResponse(ActionResponse actionResponse) {
                try {

                    // Delete the entity model.
                    client.prepareDelete(INDEX_NAME, "doc", entityType)
                            .setRefreshPolicy("wait_for")
                            .execute(onComplete);
                } catch (Exception e) {

                    // An error occurred when deleting the entity model.
                    onComplete.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {

                // An error occurred when ensuring that the '.zentity-models' index existed.
                onComplete.onFailure(e);
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
                    if (requestBody == null || requestBody.equals("")) {
                        if (method == POST)
                            throw new ValidationException("Request body cannot be empty when indexing an entity model.");
                        else
                            throw new ValidationException("Request body cannot be empty when updating an entity model.");
                    }

                    // Parse and validate the entity model.
                    new Model(requestBody);
                }

                // Handle request
                if (method == GET && (entityType == null || entityType.equals(""))) {

                    // GET _zentity/models
                    getEntityModels(client, new ActionListener<>() {

                        @Override
                        public void onResponse(SearchResponse response) {
                            try {

                                // The entity models were retrieved. Send the response.
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                ZentityPlugin.sendResponse(channel, content);
                            } catch (Exception e) {

                                // An error occurred when sending the response.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when retrieving the entity models.
                            ZentityPlugin.sendResponseError(channel, logger, e);
                        }
                    });

                } else if (method == GET) {

                    // GET _zentity/models/{entity_type}
                    getEntityModel(entityType, client, new ActionListener<>() {

                        @Override
                        public void onResponse(GetResponse response) {
                            try {

                                // The entity model was retrieved. Send the response.
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                ZentityPlugin.sendResponse(channel, content);
                            } catch (Exception e) {

                                // An error occurred when sending the response.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // An error occurred when retrieving the entity model.
                            ZentityPlugin.sendResponseError(channel, logger, e);
                        }
                    });

                } else if (method == POST && !entityType.equals("")) {

                    // POST _zentity/models/{entity_type}
                    indexEntityModel(entityType, requestBody, client, new ActionListener<>() {

                        @Override
                        public void onResponse(IndexResponse response) {
                            try {

                                // The entity model was indexed. Send the response.
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                ZentityPlugin.sendResponse(channel, content);
                            } catch (Exception e) {

                                // An error occurred when sending the response.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when indexing the entity model.
                            if (e.getClass() == ElasticsearchSecurityException.class) {

                                // The error was a security exception.
                                // Log the error message as it was received from Elasticsearch.
                                logger.debug(e.getMessage());

                                // Return a more descriptive error message for the user.
                                ZentityPlugin.sendResponseError(channel, logger, new ForbiddenException("Unable to index the entity model. This action requires the 'write' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                            } else {

                                // The error was unexpected.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }
                    });

                } else if (method == PUT && !entityType.equals("")) {

                    // PUT _zentity/models/{entity_type}
                    updateEntityModel(entityType, requestBody, client, new ActionListener<>() {

                        @Override
                        public void onResponse(IndexResponse response) {
                            try {

                                // The entity model was updated. Send the response.
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                ZentityPlugin.sendResponse(channel, content);
                            } catch (Exception e) {

                                // An error occurred when sending the response.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when updating the entity model.
                            if (e.getClass() == ElasticsearchSecurityException.class) {

                                // The error was a security exception.
                                // Log the error message as it was received from Elasticsearch.
                                logger.debug(e.getMessage());

                                // Return a more descriptive error message for the user.
                                ZentityPlugin.sendResponseError(channel, logger, new ForbiddenException("Unable to update the entity model. This action requires the 'write' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                            } else {

                                // The error was unexpected.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }
                    });

                } else if (method == DELETE && !entityType.equals("")) {

                    // DELETE _zentity/models/{entity_type}
                    deleteEntityModel(entityType, client, new ActionListener<>() {

                        @Override
                        public void onResponse(DeleteResponse response) {
                            try {

                                // The entity model was deleted. Send the response.
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                ZentityPlugin.sendResponse(channel, content);
                            } catch (Exception e) {

                                // An error occurred when sending the response.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {

                            // An error occurred when deleting the entity model.
                            if (e.getClass() == ElasticsearchSecurityException.class) {

                                // The error was a security exception.
                                // Log the error message as it was received from Elasticsearch.
                                logger.debug(e.getMessage());

                                // Return a more descriptive error message for the user.
                                ZentityPlugin.sendResponseError(channel, logger, new ForbiddenException("Unable to delete the entity model. This action requires the 'write' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                            } else {

                                // The error was unexpected.
                                ZentityPlugin.sendResponseError(channel, logger, e);
                            }
                        }
                    });

                } else {
                    throw new NotImplementedException("Method and endpoint not implemented.");
                }

            } catch (Exception e) {
                ZentityPlugin.sendResponseError(channel, logger, e);
            }
        };
    }
}
