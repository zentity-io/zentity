package io.zentity.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.plugin.zentity.ZentityPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static io.zentity.common.Json.ORDERED_MAPPER;

public class JobIT extends AbstractITCase {

    private final StringEntity TEST_PAYLOAD_JOB_NO_SCOPE = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": [ \"a_00\" ]\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_ATTRIBUTES = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": [ \"a_00\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\", \".zentity_test_index_b\", \".zentity_test_index_c\" ],\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_EXPLANATION = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": [ \"a_00\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_IDS = new StringEntity("{\n" +
            "  \"ids\": {\n" +
            "    \".zentity_test_index_a\": [ \"a0\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\", \".zentity_test_index_b\", \".zentity_test_index_c\" ],\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_ATTRIBUTES_IDS = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": [ \"a_00\" ]\n" +
            "  },\n" +
            "  \"ids\": {\n" +
            "    \".zentity_test_index_a\": [ \"a6\" ]\n" +
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
            "    \"attribute_d\": { \"values\": [ \"d_00\" ] }\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_boolean\": [ true ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_DATE = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_d\": { \"values\": [ \"d_00\" ] },\n" +
            "    \"attribute_type_date\": {\n" +
            "      \"values\": [ \"2000-01-01 00:00:00\" ],\n" +
            "      \"params\": { \"format\": \"yyyy-MM-dd HH:mm:ss\", \"window\": \"1s\" }\n" +
            "    }\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"resolvers\": [ \"resolver_type_date_a\", \"resolver_type_date_b\", \"resolver_type_date_c\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_boolean\": [ false ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_double\": [ -3.141592653589793 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_float\": [ 1.0 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_float\": [ -1.0 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_integer\": [ 1 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_integer\": [ -1 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_long\": [ 922337203685477 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_long\": [ -922337203685477 ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_string\": [ \"a\" ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_type_string\": [ \"b\" ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_OBJECT = new StringEntity("{\n" +
            "  \"attributes\": { \"attribute_object\": [ \"a\" ] },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ], \"resolvers\": [ \"resolver_object\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_ATTRIBUTES = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": [ \"a_00\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"exclude\": {\n" +
            "      \"attributes\": { \"attribute_a\":[ \"a_11\" ], \"attribute_c\": [ \"c_03\" ] }\n" +
            "    },\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\", \".zentity_test_index_b\", \".zentity_test_index_c\" ],\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_INCLUDE_ATTRIBUTES = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_d\": [ \"d_00\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"attributes\": { \"attribute_d\": [ \"d_00\" ], \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
            "      \"indices\": [ \".zentity_test_index_a\", \".zentity_test_index_b\", \".zentity_test_index_c\", \".zentity_test_index_d\" ],\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_AND_INCLUDE_ATTRIBUTES = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_d\": [ \"d_00\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"exclude\": {\n" +
            "      \"attributes\": { \"attribute_c\": [ \"c_00\", \"c_01\" ] }\n" +
            "    },\n" +
            "    \"include\": {\n" +
            "      \"attributes\": { \"attribute_d\": [ \"d_00\" ], \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
            "      \"indices\": [ \".zentity_test_index_a\", \".zentity_test_index_b\", \".zentity_test_index_c\", \".zentity_test_index_d\" ],\n" +
            "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_PRIORITY = new StringEntity("{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_a\": [ \"a_10\" ],\n" +
            "    \"attribute_b\": [ \"b_10\" ]\n" +
            "  },\n" +
            "  \"scope\": {\n" +
            "    \"include\": {\n" +
            "      \"indices\": [ \".zentity_test_index_a\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);

    private byte[] readFile(String filename) throws IOException {
        InputStream stream = this.getClass().getResourceAsStream("/" + filename);
        return IOUtils.toByteArray(stream);
    }

    private void destroyTestResources() throws IOException {

        // Delete indices
        client.performRequest(new Request("DELETE", ".zentity_test_index_a"));
        client.performRequest(new Request("DELETE", ".zentity_test_index_b"));
        client.performRequest(new Request("DELETE", ".zentity_test_index_c"));
        client.performRequest(new Request("DELETE", ".zentity_test_index_d"));

        // Delete entity model
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_a"));
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_b"));
    }

    private void prepareTestResources() throws Exception {

        // Load files
        ByteArrayEntity testIndex;
        ByteArrayEntity testData;
        ByteArrayEntity testEntityModelA;
        ByteArrayEntity testEntityModelB;
        // Elasticsearch 7.0.0+ removes mapping types
        Properties props = new Properties();
        props.load(ZentityPlugin.class.getResourceAsStream("/plugin-descriptor.properties"));
        if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
            testIndex = new ByteArrayEntity(readFile("TestIndex.json"), ContentType.APPLICATION_JSON);
            testData = new ByteArrayEntity(readFile("TestData.txt"), ContentType.create("application/x-ndjson"));
        } else {
            testIndex = new ByteArrayEntity(readFile("TestIndexElasticsearch6.json"), ContentType.APPLICATION_JSON);
            testData = new ByteArrayEntity(readFile("TestDataElasticsearch6.txt"), ContentType.create("application/x-ndjson"));
        }
        testEntityModelA = new ByteArrayEntity(readFile("TestEntityModelA.json"), ContentType.APPLICATION_JSON);
        testEntityModelB = new ByteArrayEntity(readFile("TestEntityModelB.json"), ContentType.APPLICATION_JSON);

        // Create indices
        Request putTestIndexA = new Request("PUT", ".zentity_test_index_a");
        putTestIndexA.setEntity(testIndex);
        client.performRequest(putTestIndexA);
        Request putTestIndexB = new Request("PUT", ".zentity_test_index_b");
        putTestIndexB.setEntity(testIndex);
        client.performRequest(putTestIndexB);
        Request putTestIndexC = new Request("PUT", ".zentity_test_index_c");
        putTestIndexC.setEntity(testIndex);
        client.performRequest(putTestIndexC);
        Request putTestIndexD = new Request("PUT", ".zentity_test_index_d");
        putTestIndexD.setEntity(testIndex);
        client.performRequest(putTestIndexD);

        // Load data into indices
        Request postBulk = new Request("POST", "_bulk");
        postBulk.addParameter("refresh", "true");
        postBulk.setEntity(testData);
        client.performRequest(postBulk);

        // Create entity models
        Request postModelA = new Request("POST", "_zentity/models/zentity_test_entity_a");
        postModelA.setEntity(testEntityModelA);
        client.performRequest(postModelA);
        Request postModelB = new Request("POST", "_zentity/models/zentity_test_entity_b");
        postModelB.setEntity(testEntityModelB);
        client.performRequest(postModelB);
    }

    private Set<String> getActual(JsonNode json) {
        Set<String> docsActual = new TreeSet<>();
        for (JsonNode node : json.get("hits").get("hits")) {
            String _id = node.get("_id").asText();
            int _hop = node.get("_hop").asInt();
            docsActual.add(_id + "," + _hop);
        }
        return docsActual;
    }

    public void testJobNoScope() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_NO_SCOPE);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 40);
        } finally {
            destroyTestResources();
        }
    }

    public void testJobAttributes() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 6);

            Set<String> docsExpected = new TreeSet<>();
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

    public void testJobExplanation() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("_attributes", "false");
            postResolution.addParameter("_explanation", "true");
            postResolution.addParameter("_source", "false");
            postResolution.addParameter("max_hops", "1");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_EXPLANATION);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

            String expected = "[{\"_index\":\".zentity_test_index_a\",\"_type\":\"_doc\",\"_id\":\"a0\",\"_hop\":0,\"_explanation\":{\"attributes\":{\"attribute_a\":[\"a_00\"]},\"resolvers\":[\"resolver_a\"]}},{\"_index\":\".zentity_test_index_a\",\"_type\":\"_doc\",\"_id\":\"a1\",\"_hop\":1,\"_explanation\":{\"attributes\":{\"attribute_d\":[\"d_00\"],\"attribute_type_date\":[\"1999-12-31T23:59:57.0000\"]},\"resolvers\":[\"resolver_c\",\"resolver_type_date_c\"]}},{\"_index\":\".zentity_test_index_a\",\"_type\":\"_doc\",\"_id\":\"a2\",\"_hop\":1,\"_explanation\":{\"attributes\":{\"attribute_d\":[\"d_00\"],\"attribute_object\":[\"a\"],\"attribute_type_boolean\":[true],\"attribute_type_double\":[3.141592653589793],\"attribute_type_float\":[1.0],\"attribute_type_integer\":[1],\"attribute_type_long\":[922337203685477],\"attribute_type_string\":[\"a\"]},\"resolvers\":[\"resolver_c\",\"resolver_object\",\"resolver_type_boolean\",\"resolver_type_double\",\"resolver_type_float\",\"resolver_type_integer\",\"resolver_type_long\",\"resolver_type_string\"]}}]";
            String actual = ORDERED_MAPPER.writeValueAsString(json.get("hits").get("hits"));

            assertEquals(expected, actual);

        } finally {
            destroyTestResources();
        }
    }

    public void testJobIds() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_IDS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 6);

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,1");
            docsExpected.add("c0,2");
            docsExpected.add("a1,3");
            docsExpected.add("b1,4");
            docsExpected.add("c1,5");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobAttributesIds() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_ATTRIBUTES_IDS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 30);

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a6,0");
            docsExpected.add("b0,0");
            docsExpected.add("a2,1");
            docsExpected.add("a7,1");
            docsExpected.add("a8,1");
            docsExpected.add("a9,1");
            docsExpected.add("b2,1");
            docsExpected.add("b6,1");
            docsExpected.add("b7,1");
            docsExpected.add("b8,1");
            docsExpected.add("b9,1");
            docsExpected.add("c0,1");
            docsExpected.add("c2,1");
            docsExpected.add("c6,1");
            docsExpected.add("c7,1");
            docsExpected.add("c8,1");
            docsExpected.add("c9,1");
            docsExpected.add("a1,2");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("a5,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("b5,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("c5,2");
            docsExpected.add("b1,3");
            docsExpected.add("c1,4");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobMaxHopsAndDocs() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("max_hops", "2");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_MAX_HOPS_AND_DOCS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 20);

            Set<String> docsExpected = new TreeSet<>();
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
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";

            Set<String> docsExpectedA = new TreeSet<>();
            docsExpectedA.add("a0,0");
            docsExpectedA.add("a2,0");
            docsExpectedA.add("a4,0");
            docsExpectedA.add("a6,0");
            docsExpectedA.add("a8,0");

            Set<String> docsExpectedB = new TreeSet<>();
            docsExpectedB.add("a1,0");
            docsExpectedB.add("a3,0");
            docsExpectedB.add("a5,0");
            docsExpectedB.add("a7,0");
            docsExpectedB.add("a9,0");

            // Boolean true
            Request q1 = new Request("POST", endpoint);
            q1.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE);
            Response r1 = client.performRequest(q1);
            JsonNode j1 = Json.MAPPER.readTree(r1.getEntity().getContent());
            assertEquals(j1.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j1));

            // Boolean false
            Request q2 = new Request("POST", endpoint);
            q2.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE);
            Response r2 = client.performRequest(q2);
            JsonNode j2 = Json.MAPPER.readTree(r2.getEntity().getContent());
            assertEquals(j2.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j2));

            // Double positive
            Request q3 = new Request("POST", endpoint);
            q3.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE);
            Response r3 = client.performRequest(q3);
            JsonNode j3 = Json.MAPPER.readTree(r3.getEntity().getContent());
            assertEquals(j3.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j3));

            // Double negative
            Request q4 = new Request("POST", endpoint);
            q4.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE);
            Response r4 = client.performRequest(q4);
            JsonNode j4 = Json.MAPPER.readTree(r4.getEntity().getContent());
            assertEquals(j4.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j4));

            // Float positive
            Request q5 = new Request("POST", endpoint);
            q5.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE);
            Response r5 = client.performRequest(q5);
            JsonNode j5 = Json.MAPPER.readTree(r5.getEntity().getContent());
            assertEquals(j5.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j5));

            // Float negative
            Request q6 = new Request("POST", endpoint);
            q6.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE);
            Response r6 = client.performRequest(q6);
            JsonNode j6 = Json.MAPPER.readTree(r6.getEntity().getContent());
            assertEquals(j6.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j6));

            // Integer positive
            Request q7 = new Request("POST", endpoint);
            q7.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE);
            Response r7 = client.performRequest(q7);
            JsonNode j7 = Json.MAPPER.readTree(r7.getEntity().getContent());
            assertEquals(j7.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j7));

            // Integer negative
            Request q8 = new Request("POST", endpoint);
            q8.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE);
            Response r8 = client.performRequest(q8);
            JsonNode j8 = Json.MAPPER.readTree(r8.getEntity().getContent());
            assertEquals(j8.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j8));

            // Long positive
            Request q9 = new Request("POST", endpoint);
            q9.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE);
            Response r9 = client.performRequest(q9);
            JsonNode j9 = Json.MAPPER.readTree(r9.getEntity().getContent());
            assertEquals(j9.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j9));

            // Long negative
            Request q10 = new Request("POST", endpoint);
            q10.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE);
            Response r10 = client.performRequest(q10);
            JsonNode j10 = Json.MAPPER.readTree(r10.getEntity().getContent());
            assertEquals(j10.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j10));

            // String A
            Request q11 = new Request("POST", endpoint);
            q11.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A);
            Response r11 = client.performRequest(q11);
            JsonNode j11 = Json.MAPPER.readTree(r11.getEntity().getContent());
            assertEquals(j11.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j11));

            // String B
            Request q12 = new Request("POST", endpoint);
            q12.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B);
            Response r12 = client.performRequest(q12);
            JsonNode j12 = Json.MAPPER.readTree(r12.getEntity().getContent());
            assertEquals(j12.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActual(j12));

        } finally {
            destroyTestResources();
        }
    }

    public void testJobDataTypesDate() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a?max_hops=2&max_docs_per_query=2";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("max_hops", "2");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_DATE);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

            /*
            Elasticsearch 7.0.0+ has a different behavior when querying date ranges.

            To demonstrate, compare this query (below) with the test indices, data, and entity models on Elasticsearch
            versions 6.7.1 and 7.0.0:

            GET .zentity_test_index_d/_search
            {
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "type_date": {
                          "gte": "2000-01-01 00:00:01||-2s",
                          "lte": "2000-01-01 00:00:01||+2s",
                          "format": "yyyy-MM-dd HH:mm:ss"
                        }
                      }
                    }
                  ]
                }
              }
            }

            In 7.0.0 the result has a fourth hit ("_id" = "d3") where the "type_date" field is "2000-01-01T00:00:02.500",
            which is a half second greater than the 2s window that was specified in the search.

            We'll allow this behavior in the test, since this is a behavior of Elasticsearch and not zentity.
            */
            Properties props = new Properties();
            props.load(ZentityPlugin.class.getResourceAsStream("/plugin-descriptor.properties"));
            if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
                assertEquals(json.get("hits").get("total").asInt(), 15);
            } else {
                assertEquals(json.get("hits").get("total").asInt(), 13);
            }
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a1,0");
            docsExpected.add("a2,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,0");
            docsExpected.add("d0,0");
            docsExpected.add("d1,0");
            docsExpected.add("a3,1");
            docsExpected.add("b3,1");
            docsExpected.add("c1,1");
            docsExpected.add("d2,1");
            docsExpected.add("b1,2");
            docsExpected.add("c3,2");
            if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
                docsExpected.add("d3,1");
                docsExpected.add("a4,2");
                docsExpected.add("c4,2");
            } else {
                docsExpected.add("d3,2");
            }

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobObject() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";

            Set<String> docsExpectedA = new TreeSet<>();
            docsExpectedA.add("a0,0");
            docsExpectedA.add("a2,0");
            docsExpectedA.add("a4,0");
            docsExpectedA.add("a6,0");
            docsExpectedA.add("a8,0");

            // Boolean true
            Request q1 = new Request("POST", endpoint);
            q1.setEntity(TEST_PAYLOAD_JOB_OBJECT);
            Response r1 = client.performRequest(q1);
            JsonNode j1 = Json.MAPPER.readTree(r1.getEntity().getContent());
            assertEquals(j1.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActual(j1));

        } finally {
            destroyTestResources();
        }
    }

    public void testJobScopeExcludeAttributes() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 16);

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("a2,1");
            docsExpected.add("b2,1");
            docsExpected.add("c0,1");
            docsExpected.add("c1,1");
            docsExpected.add("c2,1");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("a5,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("b5,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("c5,2");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobScopeIncludeAttributes() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_INCLUDE_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 8);

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a2,0");
            docsExpected.add("b0,0");
            docsExpected.add("b2,0");
            docsExpected.add("c0,0");
            docsExpected.add("c2,0");
            docsExpected.add("d0,0");
            docsExpected.add("d2,0");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobScopeExcludeAndIncludeAttributes() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_AND_INCLUDE_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 4);

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a2,0");
            docsExpected.add("b2,0");
            docsExpected.add("c2,0");
            docsExpected.add("d2,0");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }

    public void testJobPriority() throws Exception {
        prepareTestResources();
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_b";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_PRIORITY);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 4);

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a2,0");
            docsExpected.add("a3,0");
            docsExpected.add("a4,1");
            docsExpected.add("a5,1");

            assertEquals(docsExpected, getActual(json));
        } finally {
            destroyTestResources();
        }
    }
}