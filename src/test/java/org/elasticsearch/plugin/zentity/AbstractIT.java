/*
 * zentity
 * Copyright © 2018-2025 Dave Moore
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
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

@Ignore("Base class")
public abstract class AbstractIT {

    public final static String SERVICE_NAME = "es01";
    public final static int SERVICE_PORT = 9400;

    // A docker-compose cluster used for all test cases in the test class.
    private static DockerComposeContainer cluster;

    // Client that communicates with the docker-compose cluster.
    private static RestClient client;

    @BeforeClass
    public static void setup() throws Exception {
        createCluster();
        createClient();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        destroyClient();
        destroyCluster();
    }

    /**
     * Create the cluster if it doesn't exist.
     */
    public static void createCluster() {
        Path path = Paths.get(System.getenv("BUILD_DIRECTORY"), "test-classes", "docker-compose.yml");
        cluster = new DockerComposeContainer(new File(path.toString()))
                .withEnv("BUILD_DIRECTORY", System.getenv("BUILD_DIRECTORY"))
                .withEnv("ELASTICSEARCH_VERSION", System.getenv("ELASTICSEARCH_VERSION"))
                .withEnv("ZENTITY_VERSION", System.getenv("ZENTITY_VERSION"))
                .withExposedService(SERVICE_NAME, SERVICE_PORT,
                        Wait.forHttp("/_cat/health")
                                .forStatusCodeMatching(it -> it >= 200 && it < 300)
                                .withReadTimeout(Duration.ofSeconds(120)));
        cluster.start();
    }

    /**
     * Destroy the cluster if it exists.
     */
    public static void destroyCluster() {
        if (cluster != null) {
            cluster.stop();
            cluster = null;
        }
    }

    /**
     * Create the client if it doesn't exist.
     */
    public static void createClient() throws IOException {

        // The client configuration depends on the cluster implementation,
        // so create the cluster first if it hasn't already been created.
        if (cluster == null)
            createCluster();
      
        try {

            // Create the client.
            String host = cluster.getServiceHost(SERVICE_NAME, SERVICE_PORT);
            Integer port = cluster.getServicePort(SERVICE_NAME, SERVICE_PORT);
            client = RestClient.builder(new HttpHost(host, port)).build();

            // Verify if the client can establish a connection to the cluster.
            Response response = client.performRequest(new Request("GET", "/"));
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals("You Know, for Search", json.get("tagline").textValue());

        } catch (IOException e) {

            // If we have an exception here, let's ignore the test.
            destroyClient();
            assumeFalse("Integration tests are skipped", e.getMessage().contains("Connection refused"));
            fail("Something wrong is happening. REST Client seemed to raise an exception: " + e.getMessage());
        }
    }

    /**
     * Destroy the client if it exists.
     */
    public static void destroyClient() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /**
     * Return the client to be used in test cases.
     *
     * @return The client.
     */
    public static RestClient client() throws IOException {
        if (client == null)
            createClient();
        return client;
    }
}