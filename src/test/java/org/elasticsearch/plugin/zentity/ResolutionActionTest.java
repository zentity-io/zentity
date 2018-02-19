package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ResolutionActionTest {

    // Valid input
    private static final String attributeStringValid = "\"attribute_string\":\"abc\"";
    private static final String attributeArrayValid = "\"attribute_array\":[\"abc\"]";
    private static final String attributesValid = "\"attributes\":{" + attributeStringValid + "," + attributeArrayValid + "}";
    private static final String modelValid = "\"model\":" + ModelsActionTest.entityModelValid;
    private static final String filterIndicesValid = "\"filter_indices\":[\"index_name\"]";
    private static final String filterResolversValid = "\"filter_resolvers\":[\"resolver_name\"]";
    private static final String inputValid = "{" + attributesValid + "," + modelValid + "," + filterIndicesValid + "," + filterResolversValid + "}";

    // Invalid attribute
    private static final String attributeArrayInvalidNamePeriod = "\"attributes\":{\"attribute.string\":[\"abc\"]}";
    private static final String attributeStringInvalidNamePeriod = "\"attributes\":{\"attribute.string\":\"abc\"}";

    // Invalid attributes
    private static final String attributesInvalidEmpty = "\"attributes\":{}";
    private static final String attributesInvalidTypeArray = "\"attributes\":[]";
    private static final String attributesInvalidTypeFloat = "\"attributes\":1.0";
    private static final String attributesInvalidTypeInteger = "\"attributes\":1";
    private static final String attributesInvalidTypeNull = "\"attributes\":null";
    private static final String attributesInvalidTypeString = "\"attributes\":\"abc\"";

    // Invalid model
    private static final String modelInvalidEmpty = "\"model\":{}";
    private static final String modelInvalidTypeArray = "\"model\":[]";
    private static final String modelInvalidTypeFloat = "\"model\":1.0";
    private static final String modelInvalidTypeInteger = "\"model\":1";
    private static final String modelInvalidTypeNull = "\"model\":null";
    private static final String modelInvalidTypeString = "\"model\":\"abc\"";

    // Invalid index filters
    private static final String filterIndicesInvalidTypeFloat = "\"filter_indices\":1.0";
    private static final String filterIndicesInvalidTypeInteger = "\"filter_indices\":1";
    private static final String filterIndicesInvalidTypeNull = "\"filter_indices\":null";
    private static final String filterIndicesInvalidTypeObject = "\"filter_indices\":{\"abc\":\"xyz\"}";
    private static final String filterIndicesInvalidTypeString = "\"filter_indices\":\"abc\"";
    private static final String filterIndicesInvalidNotFound = "\"filter_indices\":[\"index_name_not_found\"]";

    // Invalid resolver filters
    private static final String filterResolversInvalidTypeFloat = "\"filter_resolvers\":1.0";
    private static final String filterResolversInvalidTypeInteger = "\"filter_resolvers\":1";
    private static final String filterResolversInvalidTypeNull = "\"filter_resolvers\":null";
    private static final String filterResolversInvalidTypeObject = "\"filter_resolvers\":{\"abc\":\"xyz\"}";
    private static final String filterResolversInvalidTypeString = "\"filter_resolvers\":\"abc\"";
    private static final String filterResolversInvalidNotFound = "\"filter_resolvers\":[\"resolver_name_not_found\"]";

    private static String inputAttributes(String attributes) {
        return "{" + attributes + "," + modelValid + "," + filterIndicesValid + "," + filterResolversValid + "}";
    }

    private static String inputModel(String model) {
        return "{" + attributesValid + "," + model + "," + filterIndicesValid + "," + filterResolversValid + "}";
    }

    private static String inputFilterIndices(String filterIndices) {
        return "{" + attributesValid + "," + modelValid + "," + filterIndices + "," + filterResolversValid + "}";
    }

    private static String inputFilterResolvers(String filterResolvers) {
        return "{" + attributesValid + "," + modelValid + "," + filterIndicesValid + "," + filterResolvers + "}";
    }

    private static JsonNode parseRequestBody(String mock) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(mock);
    }

    @Test
    public void testInputValid() throws Exception {
        parseRequestBody(inputValid);
        Assert.assertTrue(true);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeArrayInvalidNamePeriod() throws Exception {
        String mock = inputAttributes(attributeArrayInvalidNamePeriod);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeStringInvalidNamePeriod() throws Exception {
        String mock = inputAttributes(attributeStringInvalidNamePeriod);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidEmpty() throws Exception {
        String mock = inputAttributes(attributesInvalidEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeArray() throws Exception {
        String mock = inputAttributes(attributesInvalidTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeFloat() throws Exception {
        String mock = inputAttributes(attributesInvalidTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeInteger() throws Exception {
        String mock = inputAttributes(attributesInvalidTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeNull() throws Exception {
        String mock = inputAttributes(attributesInvalidTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeString() throws Exception {
        String mock = inputAttributes(attributesInvalidTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testModelInvalidEmpty() throws Exception {
        String mock = inputModel(modelInvalidEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testModelInvalidTypeArray() throws Exception {
        String mock = inputModel(modelInvalidTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testModelInvalidTypeFloat() throws Exception {
        String mock = inputModel(modelInvalidTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testModelInvalidTypeInteger() throws Exception {
        String mock = inputModel(modelInvalidTypeInteger);
        JsonNode requestBody = ModelsAction.parseEntityModel(mock);
        ResolutionAction.parseEntityModel(requestBody);
        Assert.assertTrue(true);
    }

    @Test(expected = BadRequestException.class)
    public void testModelInvalidTypeNull() throws Exception {
        String mock = inputModel(modelInvalidTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testModelInvalidTypeString() throws Exception {
        String mock = inputModel(modelInvalidTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterIndicesInvalidTypeFloat() throws Exception {
        String mock = inputFilterIndices(filterIndicesInvalidTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterIndicesInvalidTypeInteger() throws Exception {
        String mock = inputFilterIndices(filterIndicesInvalidTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterIndicesInvalidTypeNull() throws Exception {
        String mock = inputFilterIndices(filterIndicesInvalidTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterIndicesInvalidTypeObject() throws Exception {
        String mock = inputFilterIndices(filterIndicesInvalidTypeObject);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterIndicesInvalidTypeString() throws Exception {
        String mock = inputFilterIndices(filterIndicesInvalidTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesFilter(requestBody);
    }

    @Test
    public void testFilterIndicesValid() throws Exception {
        JsonNode requestBody = parseRequestBody(inputValid);
        HashSet<String> indicesFilter = ResolutionAction.parseIndicesFilter(requestBody);
        HashMap<String, HashMap<String, String>> indicesObj = ModelsAction.parseIndices(requestBody.get("model"));
        indicesObj = ResolutionAction.filterIndices(indicesObj, indicesFilter);
        Assert.assertTrue(indicesObj.containsKey("index_name"));
    }

    @Test(expected = BadRequestException.class)
    public void testFilterIndicesInvalidNotFound() throws Exception {
        String mock = inputFilterIndices(filterIndicesInvalidNotFound);
        JsonNode requestBody = parseRequestBody(mock);
        HashSet<String> indicesFilter = ResolutionAction.parseIndicesFilter(requestBody);
        ResolutionAction.filterIndices(ModelsAction.parseIndices(requestBody.get("model")), indicesFilter);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterResolversInvalidTypeFloat() throws Exception {
        String mock = inputFilterResolvers(filterResolversInvalidTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterResolversInvalidTypeInteger() throws Exception {
        String mock = inputFilterResolvers(filterResolversInvalidTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterResolversInvalidTypeNull() throws Exception {
        String mock = inputFilterResolvers(filterResolversInvalidTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterResolversInvalidTypeObject() throws Exception {
        String mock = inputFilterResolvers(filterResolversInvalidTypeObject);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversFilter(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testFilterResolversInvalidTypeString() throws Exception {
        String mock = inputFilterResolvers(filterResolversInvalidTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversFilter(requestBody);
    }

    @Test
    public void testFilterResolversValid() throws Exception {
        JsonNode requestBody = parseRequestBody(inputValid);
        HashSet<String> resolversFilter = ResolutionAction.parseResolversFilter(requestBody);
        HashMap<String, ArrayList<String>> resolversObj = ModelsAction.parseResolvers(requestBody.get("model"));
        resolversObj = ResolutionAction.filterResolvers(resolversObj, resolversFilter);
        Assert.assertTrue(resolversObj.containsKey("resolver_name"));
    }

    @Test(expected = BadRequestException.class)
    public void testFilterResolversInvalidNotFound() throws Exception {
        String mock = inputFilterResolvers(filterResolversInvalidNotFound);
        JsonNode requestBody = parseRequestBody(mock);
        HashSet<String> resolversFilter = ResolutionAction.parseResolversFilter(requestBody);
        ResolutionAction.filterResolvers(ModelsAction.parseResolvers(requestBody.get("model")), resolversFilter);
    }

}
