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
package io.zentity.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final ObjectMapper ORDERED_MAPPER = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    private static final JsonStringEncoder STRING_ENCODER = new JsonStringEncoder();

    public static String quoteString(String value) {
        return jsonStringFormat(value);
    }

    private static String jsonStringEscape(String value) {
        if (value == null)
            return "null"; // Prevents NullPointerException on STRING_ENCODER.quoteAsString()
        return new String(STRING_ENCODER.quoteAsString(value));
    }

    private static String jsonStringQuote(String value) {
        return "\"" + value + "\"";
    }

    private static String jsonStringFormat(String value) {
        return jsonStringQuote(jsonStringEscape(value));
    }


    /**
     * Converts an object {@link JsonNode JsonNode's} fields iterator to a {@link Map} of strings.
     *
     * @param iterator The object iterator.
     * @return The node's map representation.
     * @throws JsonProcessingException If the object cannot be written as a string.
     */
    public static Map<String, String> toStringMap(Iterator<Map.Entry<String, JsonNode>> iterator) throws JsonProcessingException {
        Map<String, String> map = new TreeMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> paramNode = iterator.next();
            String paramField = paramNode.getKey();
            JsonNode paramValue = paramNode.getValue();
            if (paramValue.isObject() || paramValue.isArray()) {
                map.put(paramField, MAPPER.writeValueAsString(paramValue));
            } else if (paramValue.isNull()) {
                map.put(paramField, "null");
            } else {
                map.put(paramField, paramValue.asText());
            }
        }
        return map;
    }

    /**
     * Converts an object {@link JsonNode} to a {@link Map} of strings.
     *
     * @param node The object node.
     * @return The node's map representation.
     * @throws JsonProcessingException If the object cannot be written as a string.
     */
    public static Map<String, String> toStringMap(JsonNode node) throws JsonProcessingException {
        if (!node.isObject()) {
            throw new IllegalArgumentException("Can only convert JSON objects to maps");
        }
        return toStringMap(node.fields());
    }

    /**
     * Converts an object JSON {@link String} to a {@link Map} of strings.
     *
     * @param jsonString The object node string.
     * @return The node's map representation.
     * @throws JsonProcessingException If the object cannot be written/ parsed as a string.
     */
    public static Map<String, String> toStringMap(String jsonString) throws JsonProcessingException {
        return toStringMap(Json.MAPPER.readTree(jsonString));
    }

    /**
     * Re-serialize a JSON string with pretty-printing.
     *
     * @param json The JSON string.
     * @return The pretty JSON string.
     * @throws JsonProcessingException If there is an issue parsing the input.
     */
    public static String pretty(String json) throws JsonProcessingException {
        return Json.ORDERED_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(Json.ORDERED_MAPPER.readTree(json));
    }
}
