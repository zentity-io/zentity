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
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.zentity.common.Json;
import io.zentity.model.*;
import joptsimple.internal.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModelsActionIT extends AbstractIT {

    public static final ContentType NDJSON_TYPE = ContentType.create("application/x-ndjson", Consts.UTF_8);

    public static void destroyTestResources() throws Exception {
        try {
            client().performRequest(new Request("DELETE", ModelsAction.INDEX_NAME));
        } catch (ResponseException e) {
            // Destroy the test index if it already exists, otherwise continue.
        }
    }

    public static void createAndValidateEntityModel(String httpMethod) throws Exception {

        // Create the entity model
        String entityModel = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(ModelTest.VALID_OBJECT));
        Request postRequest = new Request(httpMethod, "_zentity/models/zentity_test_entity_valid");
        postRequest.setEntity(new ByteArrayEntity(entityModel.getBytes(), ContentType.APPLICATION_JSON));
        Response postResponse = client().performRequest(postRequest);

        // Validate the response
        JsonNode postResponseJson = Json.MAPPER.readTree(postResponse.getEntity().getContent());
        assertEquals(postResponse.getStatusLine().getStatusCode(), 200);
        assertEquals(postResponseJson.get("result").asText(), "created");

        // Retrieve the created entity model
        Request getRequest = new Request("GET", "_zentity/models/zentity_test_entity_valid");
        Response getResponse = client().performRequest(getRequest);

        // Validate the created entity model
        JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
        assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
        assertEquals(Json.ORDERED_MAPPER.writeValueAsString(getResponseJson.get("_source")), entityModel);
    }

    public static byte[] readFile(String filename) throws IOException {
        InputStream stream = ModelsActionIT.class.getResourceAsStream("/" + filename);
        return IOUtils.toByteArray(stream);
    }

    @Test
    public void testPostModel() throws Exception {
        destroyTestResources();
        try {
            createAndValidateEntityModel("POST");
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testPostModelConflict() throws Exception {
        destroyTestResources();
        try {
            JsonNode responseJson = Json.MAPPER.readTree("{}");
            try {
                createAndValidateEntityModel("POST");
                createAndValidateEntityModel("POST");
            } catch (ResponseException e) {

                // Expect a version conflict and validate if it was thrown
                responseJson = Json.MAPPER.readTree(e.getResponse().getEntity().getContent());
            }
            assertEquals(responseJson.get("error").get("type").asText(), "version_conflict_engine_exception");
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testPutModel() throws Exception {
        destroyTestResources();
        try {
            createAndValidateEntityModel("PUT");
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testPutModelUpdate() throws Exception {
        destroyTestResources();
        try {
            createAndValidateEntityModel("POST");

            // Create the new entity model
            String entityModel2 = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree("{\n" +
                    "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                    "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                    "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                    "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                    "}"));
            Request postRequest2 = new Request("PUT", "_zentity/models/zentity_test_entity_valid");
            postRequest2.setEntity(new ByteArrayEntity(entityModel2.getBytes(), ContentType.APPLICATION_JSON));
            Response postResponse2 = client().performRequest(postRequest2);

            // Validate the response
            JsonNode postResponseJson2 = Json.MAPPER.readTree(postResponse2.getEntity().getContent());
            assertEquals(postResponse2.getStatusLine().getStatusCode(), 200);
            assertEquals(postResponseJson2.get("result").asText(), "updated");

            // Retrieve the updated entity model
            Request getRequest2 = new Request("GET", "_zentity/models/zentity_test_entity_valid");
            Response getResponse2 = client().performRequest(getRequest2);

            // Validate the updated entity model
            JsonNode getResponseJson2 = Json.MAPPER.readTree(getResponse2.getEntity().getContent());
            assertEquals(getResponse2.getStatusLine().getStatusCode(), 200);
            assertEquals(Json.ORDERED_MAPPER.writeValueAsString(getResponseJson2.get("_source")), entityModel2);

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testDeleteModel() throws Exception {
        destroyTestResources();
        try {
            createAndValidateEntityModel("POST");

            // Delete the created entity model
            Request deleteRequest = new Request("DELETE", "_zentity/models/zentity_test_entity_valid");
            Response deleteResponse = client().performRequest(deleteRequest);

            // Validate the response
            JsonNode deleteResponseJson = Json.MAPPER.readTree(deleteResponse.getEntity().getContent());
            assertEquals(deleteResponse.getStatusLine().getStatusCode(), 200);
            assertEquals(deleteResponseJson.get("result").asText(), "deleted");

            // Retrieve the deleted entity model
            Request getRequest = new Request("GET", "_zentity/models/zentity_test_entity_valid");
            Response getResponse = client().performRequest(getRequest);

            // Validate the response
            JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
            assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
            assertFalse(getResponseJson.get("found").booleanValue());

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testDeleteModelNotFound() throws Exception {
        destroyTestResources();
        try {

            // Delete a non-existent entity model
            Request deleteRequest = new Request("DELETE", "_zentity/models/zentity_test_entity_not_found");
            Response deleteResponse = client().performRequest(deleteRequest);

            // Validate the response
            JsonNode deleteResponseJson = Json.MAPPER.readTree(deleteResponse.getEntity().getContent());
            assertEquals(deleteResponse.getStatusLine().getStatusCode(), 200);
            assertEquals(deleteResponseJson.get("result").asText(), "not_found");

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testGetModelNotFound() throws Exception {
        destroyTestResources();
        try {

            // Retrieve a non-existent entity model
            Request getRequest = new Request("GET", "_zentity/models/zentity_test_entity_not_found");
            Response getResponse = client().performRequest(getRequest);

            // Validate the response
            JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
            assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
            assertFalse(getResponseJson.get("found").booleanValue());

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testGetModels() throws Exception {
        destroyTestResources();
        try {
            createAndValidateEntityModel("POST");

            // Retrieve all entity models
            Request getRequest = new Request("GET", "_zentity/models");
            Response getResponse = client().performRequest(getRequest);

            // Validate the response
            JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
            assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
            assertEquals(getResponseJson.get("hits").get("total").get("value").intValue(), 1);
            JsonNode hit = getResponseJson.get("hits").get("hits").elements().next();
            assertEquals(hit.get("_id").asText(), "zentity_test_entity_valid");
            String actualSource = Json.ORDERED_MAPPER.writeValueAsString(hit.get("_source"));
            String expectedSource = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(ModelTest.VALID_OBJECT));
            assertEquals(actualSource, expectedSource);

        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testCannotCreateInvalidEntityType() throws Exception {
        destroyTestResources();
        ByteArrayEntity testEntityModelA = new ByteArrayEntity(readFile("TestEntityModelA.json"), ContentType.APPLICATION_JSON);
        Request request = new Request("POST", "_zentity/models/_anInvalidType");
        request.setEntity(testEntityModelA);

        try {
            client().performRequest(request);
            fail("expected failure");
        } catch (ResponseException ex) {
            Response response = ex.getResponse();
            assertEquals(400, response.getStatusLine().getStatusCode());

            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

            assertEquals(400, json.get("status").asInt());

            assertTrue("response has error field", json.has("error"));
            JsonNode errorJson = json.get("error");

            assertTrue("error has type field", errorJson.has("type"));
            assertEquals("validation_exception", errorJson.get("type").textValue());

            assertTrue("error has reason field", errorJson.has("reason"));
            assertTrue(errorJson.get("reason").textValue().contains("Invalid name [_anInvalidType]"));
        } finally {
            destroyTestResources();
        }
    }

    ////  Bulk actions  ////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testBulkCreate() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityTypeA = "zentity_test_entity_a";
            String entityTypeB = "zentity_test_entity_b";
            String entityModelA =  "{\"attributes\":{\"a\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            String entityModelB =  "{\"attributes\":{\"b\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{\"entity_type\":\"" + entityTypeA + "\"}}",
                    entityModelA,
                    "{\"create\":{\"entity_type\":\"" + entityTypeB + "\"}}",
                    entityModelB
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check shape
            assertTrue(json.isObject());
            assertTrue(json.has("errors"));
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.has("took"));
            assertTrue(json.get("took").isNumber());
            assertTrue(json.has("items"));
            assertTrue(json.get("items").isArray());

            // check the values
            assertTrue(json.get("took").asLong() >= 0);
            assertFalse(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            int size = items.size();
            assertEquals(2, size);

            // check the values of each item
            for (int i = 0; i < size; i++) {

                // validate the action
                JsonNode item = items.get(i);
                assertTrue(item.has("create"));
                assertFalse(item.has("update"));
                assertFalse(item.has("delete"));
                assertFalse(item.has("action"));

                // validate the result
                JsonNode result = item.get("create");
                assertEquals(ModelsAction.INDEX_NAME, result.get("_index").asText());
                assertEquals("created", result.get("result").asText());
                assertTrue(result.has("_type"));
                assertTrue(result.has("_version"));
                assertTrue(result.has("_shards"));
                assertTrue(result.has("_seq_no"));
                assertTrue(result.has("_primary_term"));
                assertFalse(result.has("error"));

                // items should be returned in the order they were processed
                String _id = "";
                String model = "";
                if (i == 0) {
                    _id = entityTypeA;
                    model = entityModelA;
                } else if (i == 1) {
                    _id = entityTypeB;
                    model = entityModelB;
                }
                assertEquals(_id, result.get("_id").asText());

                // ensure the entity model is properly reflected in the '.zentity-models' index
                Request getRequest = new Request("GET", "_zentity/models/" + _id);
                Response getResponse = client().performRequest(getRequest);
                JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
                assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
                assertTrue(getResponseJson.get("found").booleanValue());
                model = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(model));
                assertEquals(model, Json.ORDERED_MAPPER.writeValueAsString(getResponseJson.get("_source")));
            }
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testBulkCreateConflict() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityType = "zentity_test_entity_a";
            String entityModel =  "{\"attributes\":{\"a\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{\"entity_type\":\"" + entityType + "\"}}",
                    entityModel,
                    "{\"create\":{\"entity_type\":\"" + entityType + "\"}}",
                    entityModel
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check shape
            assertTrue(json.isObject());
            assertTrue(json.has("errors"));
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.has("took"));
            assertTrue(json.get("took").isNumber());
            assertTrue(json.has("items"));
            assertTrue(json.get("items").isArray());

            // check the values
            assertTrue(json.get("took").asLong() >= 0);
            assertTrue(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            int size = items.size();
            assertEquals(2, size);

            // check the values of each item
            for (int i = 0; i < size; i++) {

                // validate the action
                JsonNode item = items.get(i);
                assertTrue(item.has("create"));
                assertFalse(item.has("update"));
                assertFalse(item.has("delete"));
                assertFalse(item.has("action"));

                // validate the result
                JsonNode result = item.get("create");
                if (i == 0) {
                    assertEquals(entityType, result.get("_id").asText());
                    assertEquals(ModelsAction.INDEX_NAME, result.get("_index").asText());
                    assertEquals("created", result.get("result").asText());
                    assertTrue(result.has("_type"));
                    assertTrue(result.has("_version"));
                    assertTrue(result.has("_shards"));
                    assertTrue(result.has("_seq_no"));
                    assertTrue(result.has("_primary_term"));
                    assertFalse(result.has("error"));
                } else {
                    assertTrue(result.has("error"));
                    assertEquals("org.elasticsearch.index.engine.VersionConflictEngineException", result.get("error").get("type").asText());
                }

                // ensure the entity model is properly reflected in the '.zentity-models' index
                Request getRequest = new Request("GET", "_zentity/models/" + entityType);
                Response getResponse = client().performRequest(getRequest);
                JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
                assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
                assertTrue(getResponseJson.get("found").booleanValue());
                entityModel = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(entityModel));
                assertEquals(entityModel, Json.ORDERED_MAPPER.writeValueAsString(getResponseJson.get("_source")));
            }
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testBulkUpdate() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityTypeA = "zentity_test_entity_a";
            String entityTypeB = "zentity_test_entity_b";
            String entityModelA =  "{\"attributes\":{\"a\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            String entityModelAUpdated =  "{\"attributes\":{\"a_updated\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            String entityModelB =  "{\"attributes\":{\"b\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{\"entity_type\":\"" + entityTypeA + "\"}}",
                    entityModelA,
                    "{\"create\":{\"entity_type\":\"" + entityTypeB + "\"}}",
                    entityModelB,
                    "{\"update\":{\"entity_type\":\"" + entityTypeA + "\"}}",
                    entityModelAUpdated
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check shape
            assertTrue(json.isObject());
            assertTrue(json.has("errors"));
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.has("took"));
            assertTrue(json.get("took").isNumber());
            assertTrue(json.has("items"));
            assertTrue(json.get("items").isArray());

            // check the values
            assertTrue(json.get("took").asLong() >= 0);
            assertFalse(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            int size = items.size();
            assertEquals(3, size);

            // check the values of each item
            for (int i = 0; i < size; i++) {

                // validate the action
                JsonNode item = items.get(i);
                String action = "action";
                if (i == 0 || i == 1) {
                    assertTrue(item.has("create"));
                    assertFalse(item.has("update"));
                    action = "create";
                } else if (i == 2) {
                    assertTrue(item.has("update"));
                    assertFalse(item.has("create"));
                    action = "update";
                }
                assertFalse(item.has("delete"));
                assertFalse(item.has("action"));

                // validate the result
                JsonNode result = item.get(action);
                assertEquals(ModelsAction.INDEX_NAME, result.get("_index").asText());
                assertEquals( action + "d", result.get("result").asText());
                assertTrue(result.has("_type"));
                assertTrue(result.has("_version"));
                assertTrue(result.has("_shards"));
                assertTrue(result.has("_seq_no"));
                assertTrue(result.has("_primary_term"));
                assertFalse(result.has("error"));

                // items should be returned in the order they were processed
                String _id = "";
                String model = "";
                if (i == 0 || i == 2) {
                    _id = entityTypeA;
                    model = entityModelAUpdated;
                } else if (i == 1) {
                    _id = entityTypeB;
                    model = entityModelB;
                }
                assertEquals(_id, result.get("_id").asText());

                // ensure the entity model is properly reflected in the '.zentity-models' index
                Request getRequest = new Request("GET", "_zentity/models/" + _id);
                Response getResponse = client().performRequest(getRequest);
                JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
                assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
                assertTrue(getResponseJson.get("found").booleanValue());
                model = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(model));
                assertEquals(model, Json.ORDERED_MAPPER.writeValueAsString(getResponseJson.get("_source")));
            }
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testBulkDelete() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityTypeA = "zentity_test_entity_a";
            String entityTypeB = "zentity_test_entity_b";
            String entityModelA =  "{\"attributes\":{\"a\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            String entityModelB =  "{\"attributes\":{\"b\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{\"entity_type\":\"" + entityTypeA + "\"}}",
                    entityModelA,
                    "{\"create\":{\"entity_type\":\"" + entityTypeB + "\"}}",
                    entityModelB,
                    "{\"delete\":{\"entity_type\":\"" + entityTypeA + "\"}}",
                    "{}"
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check shape
            assertTrue(json.isObject());
            assertTrue(json.has("errors"));
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.has("took"));
            assertTrue(json.get("took").isNumber());
            assertTrue(json.has("items"));
            assertTrue(json.get("items").isArray());

            // check the values
            assertTrue(json.get("took").asLong() >= 0);
            assertFalse(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            int size = items.size();
            assertEquals(3, size);

            // check the values of each item
            for (int i = 0; i < size; i++) {

                // validate the action
                JsonNode item = items.get(i);
                String action = "action";
                if (i == 0 || i == 1) {
                    assertTrue(item.has("create"));
                    assertFalse(item.has("delete"));
                    action = "create";
                } else if (i == 2) {
                    assertTrue(item.has("delete"));
                    assertFalse(item.has("create"));
                    action = "delete";
                }
                assertFalse(item.has("update"));
                assertFalse(item.has("action"));

                // validate the result
                JsonNode result = item.get(action);
                assertEquals(ModelsAction.INDEX_NAME, result.get("_index").asText());
                assertEquals( action + "d", result.get("result").asText());
                assertTrue(result.has("_type"));
                assertTrue(result.has("_version"));
                assertTrue(result.has("_shards"));
                assertTrue(result.has("_seq_no"));
                assertTrue(result.has("_primary_term"));
                assertFalse(result.has("error"));

                // items should be returned in the order they were processed
                if (i == 0 || i == 2) {
                    assertEquals(entityTypeA, result.get("_id").asText());

                    // Ensure entityTypeA was deleted
                    Request getRequest = new Request("GET", "_zentity/models/" + entityTypeA);
                    Response getResponse = client().performRequest(getRequest);
                    JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
                    assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
                    assertFalse(getResponseJson.get("found").booleanValue());

                } else if (i == 1) {
                    String model = entityModelB;
                    assertEquals(entityTypeB, result.get("_id").asText());

                    // Ensure entityTypeB was created and not affected
                    Request getRequest = new Request("GET", "_zentity/models/" + entityTypeB);
                    Response getResponse = client().performRequest(getRequest);
                    JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
                    assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
                    assertTrue(getResponseJson.get("found").booleanValue());
                    model = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(model));
                    assertEquals(model, Json.ORDERED_MAPPER.writeValueAsString(getResponseJson.get("_source")));
                }
            }
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testBulkInvalidActionUnsupported() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityType = "zentity_test_entity_a";
            String entityModel =  "{\"attributes\":{\"a\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{\"entity_type\":\"" + entityType + "\"}}",
                    entityModel,
                    "{\"get\":{\"entity_type\":\"" + entityType + "\"}}",
                    entityModel
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check shape
            assertTrue(json.isObject());
            assertTrue(json.has("errors"));
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.has("took"));
            assertTrue(json.get("took").isNumber());
            assertTrue(json.has("items"));
            assertTrue(json.get("items").isArray());

            // check the values
            assertTrue(json.get("took").asLong() >= 0);
            assertTrue(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            int size = items.size();
            assertEquals(2, size);

            // check the values of each item
            for (int i = 0; i < size; i++) {

                // validate the action
                JsonNode item = items.get(i);
                String action = "action";
                if (i == 0) {
                    assertTrue(item.has("create"));
                    assertFalse(item.has("action"));
                    action = "create";
                } else if (i == 2) {
                    assertTrue(item.has("action"));
                    assertFalse(item.has("create"));
                    action = "action";
                }
                assertFalse(item.has("update"));
                assertFalse(item.has("delete"));

                // validate the result
                JsonNode result = item.get(action);

                // items should be returned in the order they were processed
                if (i == 0) {
                    assertEquals(ModelsAction.INDEX_NAME, result.get("_index").asText());
                    assertEquals( "created", result.get("result").asText());
                    assertTrue(result.has("_type"));
                    assertTrue(result.has("_version"));
                    assertTrue(result.has("_shards"));
                    assertTrue(result.has("_seq_no"));
                    assertTrue(result.has("_primary_term"));
                    assertFalse(result.has("error"));
                    assertEquals(entityType, result.get("_id").asText());

                } else if (i == 1) {
                    assertTrue(result.get("error").get("reason").asText().startsWith("'get' is not a recognized action"));
                }

                // Ensure entityType was created and not affected
                Request getRequest = new Request("GET", "_zentity/models/" + entityType);
                Response getResponse = client().performRequest(getRequest);
                JsonNode getResponseJson = Json.MAPPER.readTree(getResponse.getEntity().getContent());
                assertEquals(getResponse.getStatusLine().getStatusCode(), 200);
                assertTrue(getResponseJson.get("found").booleanValue());
                String model = Json.ORDERED_MAPPER.writeValueAsString(Json.ORDERED_MAPPER.readTree(entityModel));
                assertEquals(model, Json.ORDERED_MAPPER.writeValueAsString(getResponseJson.get("_source")));
            }
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testBulkInvalidEntityTypeMissing() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityModel =  "{\"attributes\":{\"a\":{}},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{}}",
                    entityModel
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check the values
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.get("took").asLong() >= 0);
            assertTrue(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            assertEquals(1, items.size());
            assertTrue(items.get(0).get("create").get("error").get("reason").asText().startsWith("Entity type must be specified"));
        } finally {
            destroyTestResources();
        }
    }

    @Test
    public void testBulkInvalidEntityModelMalformed() throws Exception {
        destroyTestResources();
        try {
            String endpoint = "_zentity/models/_bulk";
            String entityType = "zentity_test_entity_a";
            String entityModel =  "{\"foo\":{},\"resolvers\":{},\"matchers\":{},\"indices\":{}}";
            Request request = new Request("POST", endpoint);
            String[] requestBodyLines = new String[] {
                    "{\"create\":{\"entity_type\":\"" + entityType + "\"}}",
                    entityModel
            };
            String requestBody = Strings.join(requestBodyLines, "\n");
            request.setEntity(new NStringEntity(requestBody, NDJSON_TYPE));
            Response response = client().performRequest(request);
            assertEquals(response.getStatusLine().getStatusCode(), 200);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check the values
            assertTrue(json.get("errors").isBoolean());
            assertTrue(json.get("took").asLong() >= 0);
            assertTrue(json.get("errors").booleanValue());
            ArrayNode items = (ArrayNode) json.get("items");
            assertEquals(1, items.size());
            assertTrue(items.get(0).get("create").get("error").get("reason").asText().startsWith("Entity model is missing required field"));
        } finally {
            destroyTestResources();
        }
    }
}
