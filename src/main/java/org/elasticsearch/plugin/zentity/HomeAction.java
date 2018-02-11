package org.elasticsearch.plugin.zentity;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.util.Properties;

import static org.elasticsearch.rest.RestRequest.Method.GET;

class BadRequestException extends Exception {
    BadRequestException(String message) {
        super(message);
    }
}

class NotFoundException extends Exception {
    NotFoundException(String message) {
        super(message);
    }
}

class NotImplementedException extends Exception {
    NotImplementedException(String message) {
        super(message);
    }
}

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

        return channel -> {
            String response = "{\n" +
                    "  \"name\": \"" + props.getProperty("name") + "\",\n" +
                    "  \"description\": \"" + props.getProperty("description") + "\",\n" +
                    "  \"version\": \"" + props.getProperty("version") + "\",\n" +
                    "  \"author\": \"" + props.getProperty("author") + "\",\n" +
                    "  \"website\": \"" + props.getProperty("website") + "\"\n" +
                    "}";
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response));
        };
    }
}
