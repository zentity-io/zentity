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

    private final StringEntity testJobPayload = new StringEntity("{\n" +
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

    private final StringEntity testJobMaxHopsAndDocsPayload = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_d\": \"d_00\"\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private byte[] readFile(String filename) throws IOException {
        InputStream stream = this.getClass().getResourceAsStream("/" + filename);
        return IOUtils.toByteArray(stream);
    }

    private void destroyTestResources() throws IOException {

        // Delete indices
        client.performRequest("DELETE", ".zentity_test_index_a");
        client.performRequest("DELETE", ".zentity_test_index_b");
        client.performRequest("DELETE", ".zentity_test_index_c");
        client.performRequest("DELETE", ".zentity_test_index_d");

        // Delete entity model
        client.performRequest("DELETE", "_zentity/models/zentity_test_entity_a");
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
        try {
            prepareTestResources();
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Response response = client.performRequest("POST", endpoint, params, testJobPayload);
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
        } finally {
            destroyTestResources();
        }
    }

    public void testJobMaxHopsAndDocs() throws Exception {
        try {
            prepareTestResources();
            String endpoint = "_zentity/resolution/zentity_test_entity_a?max_hops=2&max_docs_per_query=2";
            Response response = client.performRequest("POST", endpoint, params, testJobMaxHopsAndDocsPayload);
            JsonNode json = mapper.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 20);

            Set<String> docsExpected = new HashSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a1,0");
            docsExpected.add("b0,0");
            docsExpected.add("b1,0");
            docsExpected.add("c0,0");
            docsExpected.add("c1,0");
            docsExpected.add("d0,0");
            docsExpected.add("d1,0");
            docsExpected.add("a2,1");
            docsExpected.add("b2,1");
            docsExpected.add("c2,1");
            docsExpected.add("d2,1");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("d3,2");
            docsExpected.add("d4,2");

            Set<String> docsActual = new HashSet<>();
            for (JsonNode node : json.get("hits").get("hits")) {
                String _id = node.get("_id").asText();
                int _hop = node.get("_hop").asInt();
                docsActual.add(_id + "," + _hop);
            }

            assertEquals(docsExpected, docsActual);
        } finally {
            destroyTestResources();
        }
    }
}