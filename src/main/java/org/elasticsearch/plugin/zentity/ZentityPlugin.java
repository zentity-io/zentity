package org.elasticsearch.plugin.zentity;

import io.zentity.common.Json;
import io.zentity.model.ValidationException;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

class NotFoundException extends Exception {
    public NotFoundException(String message) {
        super(message);
    }
}

class NotImplementedException extends Exception {
    NotImplementedException(String message) {
        super(message);
    }
}

class ForbiddenException extends ElasticsearchSecurityException {
    public ForbiddenException(String message) {
        super(message);
    }
}

public class ZentityPlugin extends Plugin implements ActionPlugin {

    private static final Properties properties = new Properties();

    public ZentityPlugin() throws IOException {
        Properties zentityProperties = new Properties();
        Properties pluginDescriptorProperties = new Properties();
        InputStream zentityStream = this.getClass().getResourceAsStream("/zentity.properties");
        InputStream pluginDescriptorStream = this.getClass().getResourceAsStream("/plugin-descriptor.properties");
        zentityProperties.load(zentityStream);
        pluginDescriptorProperties.load(pluginDescriptorStream);
        properties.putAll(zentityProperties);
        properties.putAll(pluginDescriptorProperties);
    }

    public static Properties properties() {
        return properties;
    }

    public String version() {
        return properties.getProperty("version");
    }

    @Override
    public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new HomeAction(),
                new ModelsAction(),
                new ResolutionAction(),
                new SetupAction()
        );
    }

    /**
     * Return an error response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The rest channel to return the response through.
     * @param e       The exception object to process and return.
     */
    protected static void sendResponseError(RestChannel channel, Logger logger, Exception e) {
        try {

            // Handle known types of errors.
            if (e instanceof ForbiddenException) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.FORBIDDEN, e));
            } else if (e instanceof ValidationException) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } else if (e instanceof NotFoundException) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_FOUND, e));
            } else if (e instanceof NotImplementedException) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_IMPLEMENTED, e));
            } else if (e instanceof ElasticsearchException) {
                // Any other ElasticsearchException which has its own status code.
                channel.sendResponse(new BytesRestResponse(channel, ((ElasticsearchException) e).status(), e));
            } else {
                // Log the stack trace for unexpected types of errors.
                logger.catching(e);
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
            }
        } catch (Exception ee) {

            // Unexpected error when processing the exception object.
            // Since BytesRestResponse throws IOException, build this response object as a string
            // to handle any IOException that find its way here.
            logger.catching(ee);
            String message = "{\"error\":{\"root_cause\":[{\"type\":\"exception\",\"reason\":" + Json.quoteString(ee.getMessage()) + "}],\"type\":\"exception\",\"reason\":" + Json.quoteString(ee.getMessage()) + "},\"status\":500}";
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
        }
    }

    /**
     * Return a response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The rest channel to return the response through.
     * @param content The content to process and return.
     */
    protected static void sendResponse(RestChannel channel, RestStatus statusCode, XContentBuilder content) {
        channel.sendResponse(new BytesRestResponse(statusCode, content));
    }

    /**
     * Return a response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The rest channel to return the response through.
     * @param content The content to process and return.
     */
    protected static void sendResponse(RestChannel channel, XContentBuilder content) {
        sendResponse(channel, RestStatus.OK, content);
    }
}