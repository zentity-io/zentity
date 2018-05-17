package io.zentity.common;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final ObjectMapper ORDERED_MAPPER = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    private static final JsonStringEncoder STRING_ENCODER = new JsonStringEncoder();

    public static String quoteString(String value) {
        return jsonStringFormat(value);
    }

    private static String jsonStringEscape(String value) {
        return new String(STRING_ENCODER.quoteAsString(value));
    }

    private static String jsonStringQuote(String value) {
        return "\"" + value + "\"";
    }

    private static String jsonStringFormat(String value) {
        return jsonStringQuote(jsonStringEscape(value));
    }

}
