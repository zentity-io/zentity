package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class HomeActionIT extends AbstractITCase {

    @Test
    public void testHomeAction() throws Exception {

        // Get plugin properties
        Properties props = new Properties();
        Properties zentityProperties = new Properties();
        Properties pluginDescriptorProperties = new Properties();
        InputStream zentityStream = ZentityPlugin.class.getResourceAsStream("/zentity.properties");
        InputStream pluginDescriptorStream = ZentityPlugin.class.getResourceAsStream("/plugin-descriptor.properties");
        zentityProperties.load(zentityStream);
        pluginDescriptorProperties.load(pluginDescriptorStream);
        props.putAll(zentityProperties);
        props.putAll(pluginDescriptorProperties);

        // Verify if the plugin properties match the output of GET _zentity
        Response response = client().performRequest(new Request("GET", "_zentity"));
        JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
        assertEquals(json.get("name").asText(), props.getProperty("name"));
        assertEquals(json.get("description").asText(), props.getProperty("description"));
        assertEquals(json.get("website").asText(), props.getProperty("zentity.website"));
        assertEquals(json.get("version").get("zentity").asText(), props.getProperty("zentity.version"));
        assertEquals(json.get("version").get("elasticsearch").asText(), props.getProperty("elasticsearch.version"));
    }

}
