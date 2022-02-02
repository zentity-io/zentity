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

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class ZentityPluginIT extends AbstractIT {

    @Test
    public void testPluginIsLoaded() throws Exception {
        Response response = client().performRequest(new Request("GET", "_nodes/plugins"));
        JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
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