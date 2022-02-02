/*
 * zentity
 * Copyright © 2018-2022 Dave Moore
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

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class HomeActionIT extends AbstractIT {

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
