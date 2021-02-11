package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
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
        }
    }
}
