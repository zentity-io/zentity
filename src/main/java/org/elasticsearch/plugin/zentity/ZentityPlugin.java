/*
 * zentity
 * Copyright © 2018-2026 Dave Moore
 * https://zentity.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.plugin.zentity;

import io.zentity.common.Json;
import io.zentity.model.ValidationException;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
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

class BadRequestException extends ElasticsearchStatusException {
    public BadRequestException(String message) {
        this(message, null);
    }

    public BadRequestException(String message, Throwable cause) {
        super(message, RestStatus.BAD_REQUEST, cause);
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
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature) {
        return Arrays.asList(
                new HomeAction(),
                new ModelsAction(),
                new ResolutionAction(),
                new SetupAction()
        );
    }

    /**
     * A function that builds the {@link RestChannel} consumer for a request once its body (if any)
     * is known as a UTF-8 string.
     */
    @FunctionalInterface
    protected interface RestBodyHandler {
        void accept(RestChannel channel, String body) throws Exception;
    }

    /**
     * Build the {@link org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer} for a request,
     * reading its body as a UTF-8 string regardless of whether Elasticsearch delivered it fully
     * buffered or as a stream of chunks.
     *
     * As of Elasticsearch 9.0.0, the HTTP layer no longer aggregates request bodies whose path
     * ends in "/_bulk" (see Netty4HttpServerTransport's aggregator predicate). Such bodies arrive
     * as a {@link HttpBody.Stream}, and {@link RestRequest#content()} cannot be called on them.
     * Elasticsearch's own {@code BaseRestHandler} requires handlers to either consume a fully
     * buffered body synchronously inside {@code prepareRequest} (otherwise it rejects the request
     * with "does not support having a body"), or, for streamed bodies, return a
     * {@link org.elasticsearch.rest.BaseRestHandler.RequestBodyChunkConsumer} that it drives itself
     * via {@link HttpBody.Stream#setHandler}.
     *
     * This method must therefore be called from within {@code prepareRequest} (not deferred into
     * the returned consumer), so that a fully-buffered body is consumed before this method returns,
     * matching what {@code BaseRestHandler.handleRequest} asserts. Elasticsearch 9.1.0+ and all 8.x
     * versions always deliver fully-buffered bodies, so the streamed branch only engages on 9.0.x.
     * Both {@link RestRequest#isFullContent()} and {@link RestRequest#contentStream()} are available
     * on every Elasticsearch version zentity supports (verified on 8.19.6 through 9.5.5).
     *
     * @param restRequest The incoming request.
     * @param logger      The logger to use if an error occurs while handling the last chunk of a
     *                    streamed body (there is no other caller on the stack at that point to
     *                    report it through).
     * @param bodyHandler The action to perform with the channel and the fully-read request body.
     * @return The consumer to return from {@code prepareRequest}.
     */
    protected static BaseRestHandler.RequestBodyChunkConsumer prepareRequestBody(RestRequest restRequest, Logger logger, RestBodyHandler bodyHandler) {
        if (restRequest.isFullContent()) {

            // The body was delivered fully-buffered. Consume it now (inside prepareRequest, as
            // Elasticsearch requires) and run the handler once the channel is available.
            // handleChunk() is irrelevant here: Elasticsearch only wires and calls it when
            // RestRequest#isStreamedContent() is true, which is never the case on this branch.
            String body = restRequest.content().utf8ToString();
            return new BaseRestHandler.RequestBodyChunkConsumer() {
                @Override
                public void accept(RestChannel channel) throws Exception {
                    bodyHandler.accept(channel, body);
                }

                @Override
                public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
                    throw new IllegalStateException("handleChunk() should never be called for a fully-buffered request body.");
                }
            };
        }

        // The body was delivered as a stream of chunks. Elasticsearch drives this consumer itself:
        // it wires HttpBody.Stream#setHandler to call handleChunk(...) and then invokes accept(channel)
        // to kick things off, mirroring Elasticsearch's own RestBulkAction.ChunkHandler.
        return new BaseRestHandler.RequestBodyChunkConsumer() {

            private final List<ReleasableBytesReference> chunks = new ArrayList<>();

            @Override
            public void accept(RestChannel channel) {
                restRequest.contentStream().next();
            }

            @Override
            public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
                chunks.add(chunk);
                if (!isLast) {
                    restRequest.contentStream().next();
                    return;
                }
                try {
                    String body = CompositeBytesReference.of(chunks.toArray(new ReleasableBytesReference[0])).utf8ToString();
                    bodyHandler.accept(channel, body);
                } catch (Exception e) {
                    sendResponseError(channel, logger, e);
                } finally {
                    Releasables.close(chunks);
                }
            }
        };
    }

    /**
     * Return an error response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The REST channel to return the response through.
     * @param e       The exception object to process and return.
     */
    protected static void sendResponseError(RestChannel channel, Logger logger, Exception e) {
        try {

            // Handle known types of errors.
            if (e instanceof ForbiddenException) {
                channel.sendResponse(new RestResponse(channel, RestStatus.FORBIDDEN, e));
            } else if (e instanceof ValidationException) {
                channel.sendResponse(new RestResponse(channel, RestStatus.BAD_REQUEST, e));
            } else if (e instanceof NotFoundException) {
                channel.sendResponse(new RestResponse(channel, RestStatus.NOT_FOUND, e));
            } else if (e instanceof NotImplementedException) {
                channel.sendResponse(new RestResponse(channel, RestStatus.NOT_IMPLEMENTED, e));
            } else if (e instanceof ElasticsearchException) {
                // Any other ElasticsearchException which has its own status code.
                channel.sendResponse(new RestResponse(channel, ((ElasticsearchException) e).status(), e));
            } else {
                // Log the stack trace for unexpected types of errors.
                logger.catching(e);
                channel.sendResponse(new RestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
            }
        } catch (Exception ee) {

            // Unexpected error when processing the exception object.
            // Since BytesRestResponse throws IOException, build this response object as a string
            // to handle any IOException that find its way here.
            logger.catching(ee);
            String message = "{\"error\":{\"root_cause\":[{\"type\":\"exception\",\"reason\":" + Json.quoteString(ee.getMessage()) + "}],\"type\":\"exception\",\"reason\":" + Json.quoteString(ee.getMessage()) + "},\"status\":500}";
            channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
        }
    }

    /**
     * Return a response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The REST channel to return the response through.
     * @param content The content to process and return.
     */
    protected static void sendResponse(RestChannel channel, RestStatus statusCode, XContentBuilder content) {
        channel.sendResponse(new RestResponse(statusCode, content));
    }

    /**
     * Return a response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The REST channel to return the response through.
     * @param content The content to process and return.
     */
    protected static void sendResponse(RestChannel channel, XContentBuilder content) {
        sendResponse(channel, RestStatus.OK, Strings.toString(content));
    }

    /**
     * Return a response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The REST channel to return the response through.
     * @param json    The JSON string to process and return.
     */
    protected static void sendResponse(RestChannel channel, RestStatus statusCode, String json) {
        channel.sendResponse(new RestResponse(statusCode, "application/json", json));
    }

    /**
     * Return a response through a RestChannel.
     * This method is used by the action classes in org.elasticsearch.plugin.zentity.
     *
     * @param channel The REST channel to return the response through.
     * @param json    The JSON string to process and return.
     */
    protected static void sendResponse(RestChannel channel, String json) {
        sendResponse(channel, RestStatus.OK, json);
    }
}
