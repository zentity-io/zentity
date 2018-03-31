package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.resolution.AbstractITCase;
import org.elasticsearch.client.Response;

import java.util.Iterator;
import java.util.Map;

public class ZentityPluginIT extends AbstractITCase {

    public void testPluginIsLoaded() throws Exception {
        Response response = client.performRequest("GET", "_nodes/plugins");
        JsonNode json = mapper.readTree(response.getEntity().getContent());
        Iterator<Map.Entry<String, JsonNode>> nodes = json.get("nodes").fields();
        while (nodes.hasNext()) {
            Map.Entry<String, JsonNode> entry = nodes.next();
            JsonNode node = entry.getValue();
            boolean pluginFound = false;
            for (JsonNode plugin : node.get("plugins")) {
                String pluginName = plugin.get("name").textValue();
                if (pluginName.equals("zentity")) {
                    pluginFound = true;
                    break;
                }
            }
            assertTrue(pluginFound);
        }
    }
}