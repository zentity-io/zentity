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
import org.elasticsearch.client.ResponseException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SetupActionIT extends AbstractIT {

    public static void destroyTestResources() throws Exception {
        try {
            client().performRequest(new Request("DELETE", ModelsAction.INDEX_NAME));
        } catch (ResponseException e) {
            // Destroy the test index if it already exists, otherwise continue.
        }
    }

    @Test
    public void testSetupDefault() throws Exception {
        destroyTestResources();
        try {

            // Run setup with default settings
            Response setupResponse = client().performRequest(new Request("POST", "_zentity/_setup"));
            JsonNode setupJson = Json.MAPPER.readTree(setupResponse.getEntity().getContent());

            // The response should be { "acknowledged": true }
            JsonNode acknowledged = setupJson.get("acknowledged");
            assertTrue(acknowledged.isBoolean() && acknowledged.asBoolean());

            // Get the index settings and mapping
            Response getIndexResponse = client().performRequest(new Request("GET", ModelsAction.INDEX_NAME));
            JsonNode getIndexJson = Json.MAPPER.readTree(getIndexResponse.getEntity().getContent());

            // Verify if the mapping matches the default mapping
            JsonNode mappingJson = getIndexJson.get(ModelsAction.INDEX_NAME).get("mappings");
            assertEquals(mappingJson.get("dynamic").asText(), "strict");
            assertEquals(mappingJson.get("properties").get("attributes").get("type").asText(), "object");
            assertFalse(mappingJson.get("properties").get("attributes").get("enabled").booleanValue());
            assertEquals(mappingJson.get("properties").get("resolvers").get("type").asText(), "object");
            assertFalse(mappingJson.get("properties").get("resolvers").get("enabled").booleanValue());
            assertEquals(mappingJson.get("properties").get("matchers").get("type").asText(), "object");
            assertFalse(mappingJson.get("properties").get("matchers").get("enabled").booleanValue());
            assertEquals(mappingJson.get("properties").get("indices").get("type").asText(), "object");
            assertFalse(mappingJson.get("properties").get("indices").get("enabled").booleanValue());

            // Verify if the settings match the default settings
            JsonNode settingsJson = getIndexJson.get(ModelsAction.INDEX_NAME).get("settings");
            assertEquals(settingsJson.get("index").get("number_of_shards").asText(), Integer.toString(SetupAction.DEFAULT_NUMBER_OF_SHARDS));
            assertEquals(settingsJson.get("index").get("number_of_replicas").asText(), Integer.toString(SetupAction.DEFAULT_NUMBER_OF_REPLICAS));

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testSetupCustom() throws Exception {
        destroyTestResources();
        try {

            // Run setup with custom settings
            Response setupResponse = client().performRequest(new Request("POST", "_zentity/_setup?number_of_shards=2&number_of_replicas=2"));
            JsonNode setupJson = Json.MAPPER.readTree(setupResponse.getEntity().getContent());

            // The response should be { "acknowledged": true }
            JsonNode acknowledged = setupJson.get("acknowledged");
            assertTrue(acknowledged.isBoolean() && acknowledged.asBoolean());

            // Get the index settings and mapping
            Response getIndexResponse = client().performRequest(new Request("GET", ModelsAction.INDEX_NAME));
            JsonNode getIndexJson = Json.MAPPER.readTree(getIndexResponse.getEntity().getContent());

            // Verify if the settings match the default settings
            JsonNode settingsJson = getIndexJson.get(ModelsAction.INDEX_NAME).get("settings");
            assertEquals(settingsJson.get("index").get("number_of_shards").asText(), "2");
            assertEquals(settingsJson.get("index").get("number_of_replicas").asText(), "2");

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testSetupDeconflict() throws Exception {
        destroyTestResources();
        try {

            // Run setup with default settings
            Response setupDefaultResponse = client().performRequest(new Request("POST", "_zentity/_setup"));
            JsonNode setupDefaultJson = Json.MAPPER.readTree(setupDefaultResponse.getEntity().getContent());

            // The response should be { "acknowledged": true }
            JsonNode acknowledged = setupDefaultJson.get("acknowledged");
            assertTrue(acknowledged.isBoolean() && acknowledged.asBoolean());

            // Run setup again with custom settings
            try {
                client().performRequest(new Request("POST", "_zentity/_setup?number_of_shards=2&number_of_replicas=2"));
            } catch (ResponseException e) {

                // The response should be an error
                JsonNode setupCustomJson = Json.MAPPER.readTree(e.getResponse().getEntity().getContent());
                assertEquals(e.getResponse().getStatusLine().getStatusCode(), 400);
                assertEquals(setupCustomJson.get("error").get("type").asText(), "resource_already_exists_exception");
            }

            // Get the index settings and mapping
            Response getIndexResponse = client().performRequest(new Request("GET", ModelsAction.INDEX_NAME));
            JsonNode getIndexJson = Json.MAPPER.readTree(getIndexResponse.getEntity().getContent());

            // Verify if the settings match the default settings and not the custom settings
            JsonNode settingsJson = getIndexJson.get(ModelsAction.INDEX_NAME).get("settings");
            assertEquals(settingsJson.get("index").get("number_of_shards").asText(), Integer.toString(SetupAction.DEFAULT_NUMBER_OF_SHARDS));
            assertEquals(settingsJson.get("index").get("number_of_replicas").asText(), Integer.toString(SetupAction.DEFAULT_NUMBER_OF_REPLICAS));

        } finally {
            destroyTestResources();
        }
    }
}