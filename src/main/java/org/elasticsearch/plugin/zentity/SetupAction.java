package org.elasticsearch.plugin.zentity;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class SetupAction extends BaseRestHandler {

    public static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    public static final int DEFAULT_NUMBER_OF_REPLICAS = 1;
    public static final String INDEX_MAPPING = "{\n" +
            "  \"dynamic\": \"strict\",\n" +
            "  \"properties\": {\n" +
            "    \"attributes\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"enabled\": false\n" +
            "    },\n" +
            "    \"resolvers\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"enabled\": false\n" +
            "    },\n" +
            "    \"matchers\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"enabled\": false\n" +
            "    },\n" +
            "    \"indices\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"enabled\": false\n" +
            "    }\n" +
            "  }\n" +
            "}";
    public static final String INDEX_MAPPING_ELASTICSEARCH_6 = "{\n" +
            "  \"doc\": " + INDEX_MAPPING + "\n" +
            "}";

    @Inject
    public SetupAction(RestController controller) {
        controller.registerHandler(this);
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, "_zentity/_setup"));
    }

    /**
     * Create the .zentity-models index.
     *
     * @param client           The client that will communicate with Elasticsearch.
     * @param numberOfShards   The value of index.number_of_shards.
     * @param numberOfReplicas The value of index.number_of_replicas.
     * @return
     */
    public static CreateIndexResponse createIndex(NodeClient client, int numberOfShards, int numberOfReplicas) {
        // Elasticsearch 7.0.0+ removes mapping types
        Properties props = ZentityPlugin.properties();
        if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
            return client.admin().indices().prepareCreate(ModelsAction.INDEX_NAME)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", numberOfShards)
                        .put("index.number_of_replicas", numberOfReplicas)
                )
                .addMapping("doc", INDEX_MAPPING, XContentType.JSON)
                .get();
        } else {
            return client.admin().indices().prepareCreate(ModelsAction.INDEX_NAME)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", numberOfShards)
                        .put("index.number_of_replicas", numberOfReplicas)
                )
                .addMapping("doc", INDEX_MAPPING_ELASTICSEARCH_6, XContentType.JSON)
                .get();
        }
    }

    /**
     * Create the .zentity-models index using the default index settings.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @return
     */
    public static CreateIndexResponse createIndex(NodeClient client) {
        return createIndex(client, DEFAULT_NUMBER_OF_SHARDS, DEFAULT_NUMBER_OF_REPLICAS);
    }

    @Override
    public String getName() {
        return "zentity_setup_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        Boolean pretty = restRequest.paramAsBoolean("pretty", false);
        int numberOfShards = restRequest.paramAsInt("number_of_shards", 1);
        int numberOfReplicas = restRequest.paramAsInt("number_of_replicas", 1);
        Method method = restRequest.method();

        return channel -> {
            try {
                if (method == POST) {

                    createIndex(client, numberOfShards, numberOfReplicas);
                    XContentBuilder content = XContentFactory.jsonBuilder();
                    if (pretty)
                        content.prettyPrint();
                    content.startObject().field("acknowledged", true).endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));

                } else {
                    throw new NotImplementedException("Method and endpoint not implemented.");
                }
            } catch (NotImplementedException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_IMPLEMENTED, e));
            }
        };
    }
}
