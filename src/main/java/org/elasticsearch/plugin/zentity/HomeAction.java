package org.elasticsearch.plugin.zentity;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Properties;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class HomeAction extends BaseRestHandler {

    @Inject
    public HomeAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "_zentity", this);
    }

    @Override
    public String getName() {
        return "zentity_plugin_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        Properties props = ZentityPlugin.properties();
        Boolean pretty = restRequest.paramAsBoolean("pretty", false);

        return channel -> {
            XContentBuilder content = XContentFactory.jsonBuilder();
            if (pretty)
                content.prettyPrint();
            content.startObject();
            content.field("name", props.getProperty("name"));
            content.field("description", props.getProperty("description"));
            content.field("website", props.getProperty("zentity.website"));
            content.startObject("version");
            content.field("zentity", props.getProperty("zentity.version"));
            content.field("elasticsearch", props.getProperty("elasticsearch.version"));
            content.endObject();
            content.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
        };
    }
}
