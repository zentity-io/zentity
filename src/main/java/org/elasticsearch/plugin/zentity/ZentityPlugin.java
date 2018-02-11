package org.elasticsearch.plugin.zentity;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class ZentityPlugin extends Plugin implements ActionPlugin {

    public static final String INDEX = ".entity-models";
    private static final Properties properties = new Properties();

    public ZentityPlugin() throws IOException {
        InputStream resourceStream = this.getClass().getResourceAsStream("/plugin-descriptor.properties");
        properties.load(resourceStream);
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
                                "        \"dynamic\": true\n" +
                                "      },\n" +
                                "      \"indices\": {\n" +
                                "        \"type\": \"object\",\n" +
                                "        \"dynamic\": true\n" +
                                "      },\n" +
                                "      \"resolvers\": {\n" +
                                "        \"type\": \"object\",\n" +
                                "        \"dynamic\": true\n" +
                                "      }\n" +
                                "    }\n" +
                                "  }\n" +
                                "}",
                        XContentType.JSON
                )
                .get();
    }

    public static Properties properties() {
        return properties;
    }

    public String name() {
        return properties.getProperty("name");
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
        List<RestHandler> handlers = new ArrayList<RestHandler>() {{
            new HomeAction(settings, restController);
            new ModelsAction(settings, restController);
            new ResolutionAction(settings, restController);
        }};
        return handlers;
    }
}