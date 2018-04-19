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

    private final StringEntity TEST_PAYLOAD_JOB = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": \"a_00\"\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\", \".zentity_test_index_b\", \".zentity_test_index_c\" ],\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_MAX_HOPS_AND_DOCS = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_d\": \"d_00\"\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_boolean\": true },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_boolean\": false },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_double\": 3.141592653589793 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_double\": -3.141592653589793 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_float\": 1.0 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_float\": -1.0 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_integer\": 1 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_integer\": -1 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_long\": 922337203685477 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_long\": -922337203685477 },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_string\": \"a\" },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_string\": \"b\" },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_OBJECT = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_object\": \"a\" },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_object\" ]\n" +
            "    }\n" +
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

    private Set<String> getActual(JsonNode json) {
        Set<String> docsActual = new HashSet<>();
        for (JsonNode node : json.get("hits").get("hits")) {
            String _id = node.get("_id").asText();
            int _hop = node.get("_hop").asInt();
            docsActual.add(_id + "," + _hop);
        }
        return docsActual;
    }

    public void testJob() throws Exception {
        try {
            prepareTestResources();
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Response response = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB);
            JsonNode json = mapper.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 6);

            Set<String> docsExpected = new HashSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,1");
            docsExpected.add("a1,2");
            docsExpected.add("b1,3");
            docsExpected.add("c1,4");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobMaxHopsAndDocs() throws Exception {
        try {
            prepareTestResources();
            String endpoint = "_zentity/resolution/zentity_test_entity_a?max_hops=2&max_docs_per_query=2";
            Response response = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_MAX_HOPS_AND_DOCS);
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

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobDataTypes() throws Exception {
        try {
            prepareTestResources();
            String endpoint = "_zentity/resolution/zentity_test_entity_a";

            Set<String> docsExpectedA = new HashSet<>();
            docsExpectedA.add("a0,0");
            docsExpectedA.add("a2,0");
            docsExpectedA.add("a4,0");
            docsExpectedA.add("a6,0");
            docsExpectedA.add("a8,0");

            Set<String> docsExpectedB = new HashSet<>();
            docsExpectedB.add("a1,0");
            docsExpectedB.add("a3,0");
            docsExpectedB.add("a5,0");
            docsExpectedB.add("a7,0");
            docsExpectedB.add("a9,0");

            // Boolean true
            Response r1 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE);
            JsonNode j1 = mapper.readTree(r1.getEntity().getContent());
            assertEquals(j1.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j1));

            // Boolean false
            Response r2 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE);
            JsonNode j2 = mapper.readTree(r2.getEntity().getContent());
            assertEquals(j2.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j2));

            // Double positive
            Response r3 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE);
            JsonNode j3 = mapper.readTree(r3.getEntity().getContent());
            assertEquals(j3.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j3));

            // Double negative
            Response r4 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE);
            JsonNode j4 = mapper.readTree(r4.getEntity().getContent());
            assertEquals(j4.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j4));

            // Float positive
            Response r5 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE);
            JsonNode j5 = mapper.readTree(r5.getEntity().getContent());
            assertEquals(j5.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j5));

            // Float negative
            Response r6 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE);
            JsonNode j6 = mapper.readTree(r6.getEntity().getContent());
            assertEquals(j6.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j6));

            // Integer positive
            Response r7 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE);
            JsonNode j7 = mapper.readTree(r7.getEntity().getContent());
            assertEquals(j7.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j7));

            // Integer negative
            Response r8 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE);
            JsonNode j8 = mapper.readTree(r8.getEntity().getContent());
            assertEquals(j8.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j8));

            // Long positive
            Response r9 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE);
            JsonNode j9 = mapper.readTree(r9.getEntity().getContent());
            assertEquals(j9.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j9));

            // Long negative
            Response r10 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE);
            JsonNode j10 = mapper.readTree(r10.getEntity().getContent());
            assertEquals(j10.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j10));

            // String A
            Response r11 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A);
            JsonNode j11 = mapper.readTree(r11.getEntity().getContent());
            assertEquals(j11.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j11));

            // String B
            Response r12 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B);
            JsonNode j12 = mapper.readTree(r12.getEntity().getContent());
            assertEquals(j12.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j12));

        } finally {
            destroyTestResources();
        }
    }

    public void testJobObject() throws Exception {
        try {
            prepareTestResources();
            String endpoint = "_zentity/resolution/zentity_test_entity_a";

            Set<String> docsExpectedA = new HashSet<>();
            docsExpectedA.add("a0,0");
            docsExpectedA.add("a2,0");
            docsExpectedA.add("a4,0");
            docsExpectedA.add("a6,0");
            docsExpectedA.add("a8,0");

            // Boolean true
            Response r1 = client.performRequest("POST", endpoint, params, TEST_PAYLOAD_JOB_OBJECT);
            JsonNode j1 = mapper.readTree(r1.getEntity().getContent());
            assertEquals(j1.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j1));

        } finally {
            destroyTestResources();
        }
    }
}