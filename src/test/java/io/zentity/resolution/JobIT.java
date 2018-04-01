package io.zentity.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class JobIT extends AbstractITCase {

    private final Map<String, String> params = Collections.emptyMap();

    private final StringEntity testResolutionRequestPayload = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": \"a_00\"\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"indices\": [\n" +
            "      \".zentity_test_index_a\",\n" +
            "      \".zentity_test_index_b\",\n" +
            "      \".zentity_test_index_c\"\n" +
            "    ],\n" +
            "    \"resolvers\": [\n" +
            "      \"resolver_a\",\n" +
            "      \"resolver_b\"\n" +
            "    ]\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private byte[] readFile(String filename) throws IOException {
        InputStream stream = this.getClass().getResourceAsStream("/" + filename);
        return IOUtils.toByteArray(stream);
    }

    private void prepareTestResources() throws IOException {

        // Load files
        ByteArrayEntity testIndex = new ByteArrayEntity(readFile("TestIndex.json"), ContentType.APPLICATION_JSON);
        ByteArrayEntity testData = new ByteArrayEntity(readFile("TestData.txt"), ContentType.create("application/x-ndjson"));
        ByteArrayEntity testEntityModel = new ByteArrayEntity(readFile("TestEntityModel.json"), ContentType.APPLICATION_JSON);

        // Create indices
        client.performRequest("PUT", ".zentity_test_index_a", params, testIndex);
        client.performRequest("PUT", ".zentity_test_index_b", params, testIndex);
        client.performRequest("PUT", ".zentity_test_index_c", params, testIndex);
        client.performRequest("PUT", ".zentity_test_index_d", params, testIndex);

        // Load data into indices
        client.performRequest("POST", "_bulk?refresh", params, testData);

        // Create entity model
        client.performRequest("POST", "_zentity/models/zentity_test_entity_a", params, testEntityModel);
    }

    public void testJob() throws Exception {
        prepareTestResources();
        Response response = client.performRequest("POST", "_zentity/resolution/zentity_test_entity_a", params, testResolutionRequestPayload);
        JsonNode json = mapper.readTree(response.getEntity().getContent());
        assertEquals(json.get("hits").get("total").asInt(), 6);

        Set<String> docsExpected = new HashSet<>();
        docsExpected.add("a0,0");
        docsExpected.add("b0,0");
        docsExpected.add("c0,1");
        docsExpected.add("a1,2");
        docsExpected.add("b1,3");
        docsExpected.add("c1,4");

        Set<String> docsActual = new HashSet<>();
        for (JsonNode node : json.get("hits").get("hits")) {
            String _id = node.get("_id").asText();
            int _hop = node.get("_hop").asInt();
            docsActual.add(_id + "," + _hop);
        }

        assertEquals(docsExpected, docsActual);
    }
}