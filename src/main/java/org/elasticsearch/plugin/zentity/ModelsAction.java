package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.AsyncCollectionRunner;
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.Job;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ModelsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(ModelsAction.class);
    private static final int MAX_CONCURRENT_OPERATIONS_PER_REQUEST = BulkAction.MAX_CONCURRENT_OPERATIONS_PER_REQUEST;

    public static final String INDEX_NAME = ".zentity-models";

    // All parameters known to the request
    private static final String PARAM_ENTITY_TYPE = "entity_type";
    private static final String PARAM_PRETTY = "pretty";

    // Default parameter values
    public static final boolean DEFAULT_PRETTY = false;

    @Override
    public List<Route> routes() {
        return List.of(

                // Single operations
                new Route(GET, "_zentity/models"),
                new Route(GET, "_zentity/models/{entity_type}"),
                new Route(POST, "_zentity/models/{entity_type}"),   // Bulkable
                new Route(PUT, "_zentity/models/{entity_type}"),    // Bulkable
                new Route(DELETE, "_zentity/models/{entity_type}"), // Bulkable

                // Bulk operations
                new Route(POST, "_zentity/models/_bulk"),
                new Route(POST, "_zentity/models/{entity_type}/_bulk")
        );
    }

    @Override
    public String getName() {
        return "zentity_models_action";
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

    static void runOperation(NodeClient client, Method method, String body, Map<String, String> params, Map<String, String> reqParams, ActionListener<XContentBuilder> onComplete) throws NotImplementedException, ValidationException, IOException {
        final String entityType = ParamsUtil.optString(ModelsAction.PARAM_ENTITY_TYPE, null, params, reqParams);
        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, DEFAULT_PRETTY, reqParams, emptyMap());

        switch (method) {

            case GET:
                if (entityType == null || entityType.equals("")) {

                    // GET _zentity/models
                    // An error occurred when retrieving the entity models.
                    getEntityModels(client, ActionListener.wrap(

                            // Success
                            (SearchResponse response) -> {
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                onComplete.onResponse(content);
                            },

                            // Failure
                            onComplete::onFailure // An error occurred when retrieving the entity models.
                    ));
                } else {

                    // GET _zentity/models/{entity_type}
                    // An error occurred when retrieving the entity models.
                    getEntityModel(entityType, client, ActionListener.wrap(

                            // Success
                            (GetResponse response) -> {
                                XContentBuilder content = XContentFactory.jsonBuilder();
                                if (pretty)
                                    content.prettyPrint();
                                response.toXContent(content, ToXContent.EMPTY_PARAMS);
                                onComplete.onResponse(content);
                            },

                            // Failure
                            onComplete::onFailure // An error occurred when retrieving the entity model.
                    ));
                }
                break;

            case POST:

                // POST _zentity/models/{entity_type}
                if (body == null || body.equals(""))
                   throw new ValidationException("Request body cannot be empty when indexing an entity model.");
                new Model(body);

                indexEntityModel(entityType, body, client, ActionListener.wrap(

                        // Success
                        (IndexResponse response) -> {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            onComplete.onResponse(content);
                        },

                        // Failure
                        (Exception e) -> {

                            // An error occurred when indexing the entity model.
                            if (e.getClass() == ElasticsearchSecurityException.class) {

                                // The error was a security exception.
                                // Log the error message as it was received from Elasticsearch.
                                logger.debug(e.getMessage());

                                // Return a more descriptive error message for the user.
                                onComplete.onFailure(new ForbiddenException("Unable to index the entity model. This action requires the 'write' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                            } else {

                                // The error was unexpected.
                                onComplete.onFailure(e);
                            }
                        }));
                break;

            case PUT:

                // PUT _zentity/models/{entity_type}
                if (body == null || body.equals(""))
                    throw new ValidationException("Request body cannot be empty when updating an entity model.");
                new Model(body);

                updateEntityModel(entityType, body, client, ActionListener.wrap(

                        // Success
                        (IndexResponse response) -> {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            onComplete.onResponse(content);
                        },

                        // Failure
                        (Exception e) -> {

                            // An error occurred when updating the entity model.
                            if (e.getClass() == ElasticsearchSecurityException.class) {

                                // The error was a security exception.
                                // Log the error message as it was received from Elasticsearch.
                                logger.debug(e.getMessage());

                                // Return a more descriptive error message for the user.
                                onComplete.onFailure(new ForbiddenException("Unable to update the entity model. This action requires the 'write' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                            } else {

                                // The error was unexpected.
                                onComplete.onFailure(e);
                            }
                        }));
                break;

            case DELETE:

                // DELETE _zentity/models/{entity_type}
                deleteEntityModel(entityType, client, ActionListener.wrap(

                        // Success
                        (DeleteResponse response) -> {
                            XContentBuilder content = XContentFactory.jsonBuilder();
                            if (pretty)
                                content.prettyPrint();
                            response.toXContent(content, ToXContent.EMPTY_PARAMS);
                            onComplete.onResponse(content);
                        },

                        // Failure
                        (Exception e) -> {

                            // An error occurred when deleting the entity model.
                            if (e.getClass() == ElasticsearchSecurityException.class) {

                                // The error was a security exception.
                                // Log the error message as it was received from Elasticsearch.
                                logger.debug(e.getMessage());

                                // Return a more descriptive error message for the user.
                                onComplete.onFailure(new ForbiddenException("Unable to delete the entity model. This action requires the 'write' privilege for the '" + INDEX_NAME + "' index. Your role does not have this privilege."));
                            } else {

                                // The error was unexpected.
                                onComplete.onFailure(e);
                            }
                        }));
                break;

            default:
                throw new NotImplementedException("Method and endpoint not implemented.");

        }
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        Method method = restRequest.method();
        String body = restRequest.content().utf8ToString();

        // Read all possible parameters into a map so that the handler knows we've consumed them
        // and all other unknowns will be thrown as unrecognized
        Map<String, String> reqParams = ParamsUtil.readAll(
            restRequest,
            PARAM_ENTITY_TYPE,
            PARAM_PRETTY
        );

        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, DEFAULT_PRETTY, reqParams, emptyMap());

        return channel -> {
            Consumer<Exception> errorHandler = (e) -> ZentityPlugin.sendResponseError(channel, logger, e);
            try {
                boolean isBulkRequest = restRequest.path().endsWith("_bulk");
                if (isBulkRequest) {

                    // Run bulk operations
                    List<Tuple<String, String>> entries = BulkAction.splitBulkEntries(body);
                    runBulk(client, entries, reqParams, ActionListener.wrap(
                        (bulkResult) -> {
                            String json = BulkAction.bulkResultToJson(bulkResult);
                            if (pretty)
                                json = Json.pretty(json);
                            ZentityPlugin.sendResponse(channel, json);
                        },
                        errorHandler
                    ));
                } else {

                    // Run single operation
                    runOperation(client, method, body, reqParams, emptyMap(), ActionListener.wrap(
                        (content) -> {
                            ZentityPlugin.sendResponse(channel, content);
                        },
                        errorHandler
                    ));
                }

            } catch (Exception e) {
                errorHandler.accept(e);
            }
        };
    }

    /**
     * Create a single result containing the error.
     *
     * @param delegate The delegated action listener to run after creating the single error result.
     * @param action   The bulk action ("create", "update", "delete").
     * @param e        The exception object.
     */
    static void delegateFailure(ActionListener<BulkAction.SingleResult> delegate, String action, Exception e) {
        try {
            delegate.onResponse(new BulkAction.SingleResult("{\"" + action + "\":{\"error\":{" + Job.serializeException(e, true) + "}}}", true));
        } catch (Exception ee) {
            // An error occurred when preparing or sending the response.
            delegate.onFailure(ee);
        }
    }

    /**
     * @param client The node client.
     * @param entries The bulk tuple entries: <String actionAndParams, String entityModel>.
     * @param reqParams The parameters map for the entire request. Overridden by any params from entries.
     * @param listener The listener for completion results.
     */
    static void executeBulk(NodeClient client, List<Tuple<String, String>> entries, Map<String, String> reqParams, ActionListener<Collection<BulkAction.SingleResult>> listener) {
        BiConsumer<Tuple<String, String>, ActionListener<BulkAction.SingleResult>> operationRunner = (tuple, delegate) -> {
            String actionAndParams = tuple.v1();
            String entityModel = tuple.v2();
            String action = "action";
            String params = "";
            try {
                Iterator<Map.Entry<String, JsonNode>> fields = Json.MAPPER.readTree(actionAndParams).fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String name = field.getKey();
                    JsonNode value = field.getValue();
                    switch (name) {
                        case "create":
                        case "update":
                        case "delete":
                            if (!action.equals("action"))
                                throw new ValidationException("Each bulk operation must have only one action and payload.");
                            action = name;
                            params = Json.ORDERED_MAPPER.writeValueAsString(value);
                            break;
                        default:
                            throw new ValidationException("'" + name + "' is not a recognized action for bulk model management.");
                    }
                }

                // Associate the bulk action with an HTTP request method for a single operation.
                final Method method;
                switch (action) {
                    case "create":
                        method = POST;
                        break;
                    case "update":
                        method = PUT;
                        break;
                    case "delete":
                        method = DELETE;
                        break;
                    default:
                        method = GET;
                        break;
                }
                final String finalAction = action;
                final Map<String, String> finalParams = Json.toStringMap(params);
                runOperation(client, method, entityModel, finalParams, reqParams, ActionListener.wrap(
                        (xContentBuilder) -> delegate.onResponse(new BulkAction.SingleResult("{\"" + finalAction + "\":" + Strings.toString(xContentBuilder) + "}", false)),
                        (e) -> delegateFailure(delegate, finalAction, e)
                ));
            } catch (Exception e) {
                delegateFailure(delegate, action, e);
            }
        };

        // Treat all failures as fatal and fail the request as quickly as possible.
        // Operations that have handleable errors should attempt to complete normally with a structured response.
        AsyncCollectionRunner<Tuple<String, String>, BulkAction.SingleResult> collectionRunner
                = new AsyncCollectionRunner<>(entries, operationRunner, MAX_CONCURRENT_OPERATIONS_PER_REQUEST, true);

        collectionRunner.run(listener);
    }

    /**
     * Run a collection of operations concurrently.
     *
     * Expected syntax is NDJSON:
     *
     * ACTION_AND_PARAMS
     * ENTITY_MODEL
     * ...
     *
     * - ACTION: An object key being one of "create", "update", or "delete". Required for all operations.
     * - PARAMS: An object of URL params, including "entity_type" from the URL path. Required for all operations.
     * - ENTITY_MODEL: The entity model. Required for "create" and "update" operations.
     *
     * Examples:
     *
     * { "create": { "entity_type": "person" }}
     * { "attributes": { ... }, "resolvers": { ... }, "matchers": { ... }, "indices": { ... }}
     * { "update": { "entity_type": "person" }}
     * { "attributes": { ... }, "resolvers": { ... }, "matchers": { ... }, "indices": { ... }}
     * { "delete": { "entity_type": "person" }}
     * {}
     *
     *
     *
     * @param client The node client.
     * @param entries The bulk tuple entries: <String actionAndParams, String entityModel>.
     * @param reqParams The parameters map for the entire request. Overridden by any params from entries.
     * @param onComplete The listener for completion results.
     */
    static void runBulk(NodeClient client, List<Tuple<String, String>> entries, Map<String, String> reqParams, ActionListener<BulkAction.BulkResult> onComplete) {
        final long startTime = System.nanoTime();

        executeBulk(client, entries, reqParams, ActionListener.delegateFailure(
            onComplete,
            (ignored, results) -> {
                List<String> items = results.stream().map((res) -> res.response).collect(Collectors.toList());
                boolean errors = results.stream().anyMatch((res) -> res.failed);
                long took = Duration.ofNanos(System.nanoTime() - startTime).toMillis();
                onComplete.onResponse(new BulkAction.BulkResult(items, errors, took));
            }
        ));
    }

}