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
    private static final String validAttributeString = "\"attribute_string\":\"abc\"";
    private static final String validAttributeArray = "\"attribute_array\":[\"abc\"]";
    private static final String validAttributes = "\"attributes\":{" + validAttributeString + "," + validAttributeArray + "}";
    private static final String validModel = "\"model\":" + ModelsActionTest.validEntityModel;
    private static final String validScopeIndicesTypeArray = "\"indices\":[\"index_name\"]";
    private static final String validScopeIndicesTypeArrayEmpty = "\"indices\":[]";
    private static final String validScopeIndicesTypeString = "\"indices\":\"index_name\"";
    private static final String validScopeResolversTypeArray = "\"resolvers\":[\"resolver_name\"]";
    private static final String validScopeResolversTypeArrayEmpty = "\"resolvers\":[]";
    private static final String validScopeResolversTypeString = "\"resolvers\":\"resolver_name\"";
    private static final String validScopeEmpty = "\"scope\":{}";
    private static final String validScopeTypeNull = "\"scope\":null";
    private static final String validInput = "{" + validAttributes + "," + validModel + "}";

    // Invalid attribute
    private static final String invalidAttributeArrayNamePeriod = "\"attributes\":{\"attribute.string\":[\"abc\"]}";
    private static final String invalidAttributeStringNamePeriod = "\"attributes\":{\"attribute.string\":\"abc\"}";

    // Invalid attributes
    private static final String invalidAttributesEmpty = "\"attributes\":{}";
    private static final String invalidAttributesTypeArray = "\"attributes\":[]";
    private static final String invalidAttributesTypeFloat = "\"attributes\":1.0";
    private static final String invalidAttributesTypeInteger = "\"attributes\":1";
    private static final String invalidAttributesTypeNull = "\"attributes\":null";
    private static final String invalidAttributesTypeString = "\"attributes\":\"abc\"";

    // Invalid model
    private static final String invalidModelEmpty = "\"model\":{}";
    private static final String invalidModelTypeArray = "\"model\":[]";
    private static final String invalidModelTypeFloat = "\"model\":1.0";
    private static final String invalidModelTypeInteger = "\"model\":1";
    private static final String invalidModelTypeNull = "\"model\":null";
    private static final String invalidModelTypeString = "\"model\":\"abc\"";

    // Invalid scope
    private static final String invalidScopeTypeArray = "\"scope\":[]";
    private static final String invalidScopeTypeFloat = "\"scope\":1.0";
    private static final String invalidScopeTypeInteger = "\"scope\":1";
    private static final String invalidScopeTypeString = "\"scope\":\"abc\"";

    // Invalid scope.indices
    private static final String invalidScopeIndicesNotFoundArray = "\"indices\":[\"index_name_not_found\"]";
    private static final String invalidScopeIndicesNotFoundString = "\"indices\":\"index_name_not_found\"";
    private static final String invalidScopeIndicesTypeArrayFloat = "\"indices\":[1.0]";
    private static final String invalidScopeIndicesTypeArrayInteger = "\"indices\":[1]";
    private static final String invalidScopeIndicesTypeArrayObject = "\"indices\":[{\"abc\":\"xyz\"}]";
    private static final String invalidScopeIndicesTypeArrayNull = "\"indices\":[null]";
    private static final String invalidScopeIndicesTypeArrayStringEmpty = "\"indices\":[\"\"]";
    private static final String invalidScopeIndicesTypeFloat = "\"indices\":1.0";
    private static final String invalidScopeIndicesTypeInteger = "\"indices\":1";
    private static final String invalidScopeIndicesTypeObject = "\"indices\":{\"abc\":\"xyz\"}";

    // Invalid scope.resolvers
    private static final String invalidScopeResolversNotFoundArray = "\"resolvers\":[\"resolver_name_not_found\"]";
    private static final String invalidScopeResolversNotFoundString = "\"resolvers\":\"resolver_name_not_found\"";
    private static final String invalidScopeResolversTypeArrayFloat = "\"resolvers\":[1.0]";
    private static final String invalidScopeResolversTypeArrayInteger = "\"resolvers\":[1]";
    private static final String invalidScopeResolversTypeArrayObject = "\"resolvers\":[{\"abc\":\"xyz\"}]";
    private static final String invalidScopeResolversTypeArrayNull = "\"resolvers\":[null]";
    private static final String invalidScopeResolversTypeArrayStringEmpty = "\"resolvers\":[\"\"]";
    private static final String invalidScopeResolversTypeFloat = "\"resolvers\":1.0";
    private static final String invalidScopeResolversTypeInteger = "\"resolvers\":1";
    private static final String invalidScopeResolversTypeObject = "\"resolvers\":{\"abc\":\"xyz\"}";

    private static String inputAttributes(String attributes) {
        return "{" + attributes + "," + validModel + "}";
    }

    private static String inputModel(String model) {
        return "{" + validAttributes + "," + model + "}";
    }

    private static String inputScope(String scope) {
        return "{" + validAttributes + "," + validModel + "," + scope + "}";
    }

    private static String inputScopeIndices(String scopeIndices) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{" + scopeIndices + "}}";
    }

    private static String inputScopeResolvers(String scopeResolvers) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{" + scopeResolvers + "}}";
    }

    private static JsonNode parseRequestBody(String mock) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(mock);
    }

    @Test
    public void testValidInput() throws Exception {
        JsonNode requestBody = parseRequestBody(validInput);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeIndicesTypeArray() throws Exception {
        String mock = inputScopeIndices(validScopeIndicesTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeIndicesTypeArrayEmpty() throws Exception {
        String mock = inputScopeIndices(validScopeIndicesTypeArrayEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeIndicesTypeString() throws Exception {
        String mock = inputScopeIndices(validScopeIndicesTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeResolversTypeArray() throws Exception {
        String mock = inputScopeResolvers(validScopeResolversTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeResolversTypeArrayEmpty() throws Exception {
        String mock = inputScopeResolvers(validScopeResolversTypeArrayEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeResolversTypeString() throws Exception {
        String mock = inputScopeResolvers(validScopeResolversTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeEmpty() throws Exception {
        String mock = inputScope(validScopeEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeTypeNull() throws Exception {
        String mock = inputScope(validScopeTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
        Assert.assertTrue(true);
    }

    @Test
    public void testValidScopeIndices() throws Exception {
        JsonNode requestBody = parseRequestBody(validInput);
        HashSet<String> indicesFilter = ResolutionAction.parseIndicesScope(requestBody);
        HashMap<String, HashMap<String, String>> indicesObj = ModelsAction.parseIndices(requestBody.get("model"));
        indicesObj = ResolutionAction.filterIndices(indicesObj, indicesFilter);
        Assert.assertTrue(indicesObj.containsKey("index_name"));
    }

    @Test
    public void testValidScopeResolvers() throws Exception {
        JsonNode requestBody = parseRequestBody(validInput);
        HashSet<String> resolversFilter = ResolutionAction.parseResolversScope(requestBody);
        HashMap<String, ArrayList<String>> resolversObj = ModelsAction.parseResolvers(requestBody.get("model"));
        resolversObj = ResolutionAction.filterResolvers(resolversObj, resolversFilter);
        Assert.assertTrue(resolversObj.containsKey("resolver_name"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeArrayNamePeriod() throws Exception {
        String mock = inputAttributes(invalidAttributeArrayNamePeriod);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeStringNamePeriod() throws Exception {
        String mock = inputAttributes(invalidAttributeStringNamePeriod);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesEmpty() throws Exception {
        String mock = inputAttributes(invalidAttributesEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeArray() throws Exception {
        String mock = inputAttributes(invalidAttributesTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeFloat() throws Exception {
        String mock = inputAttributes(invalidAttributesTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeInteger() throws Exception {
        String mock = inputAttributes(invalidAttributesTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeNull() throws Exception {
        String mock = inputAttributes(invalidAttributesTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeString() throws Exception {
        String mock = inputAttributes(invalidAttributesTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseAttributes(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelEmpty() throws Exception {
        String mock = inputModel(invalidModelEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeArray() throws Exception {
        String mock = inputModel(invalidModelTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeFloat() throws Exception {
        String mock = inputModel(invalidModelTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeInteger() throws Exception {
        String mock = inputModel(invalidModelTypeInteger);
        JsonNode requestBody = ModelsAction.parseEntityModel(mock);
        ResolutionAction.parseEntityModel(requestBody);
        Assert.assertTrue(true);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeNull() throws Exception {
        String mock = inputModel(invalidModelTypeNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeString() throws Exception {
        String mock = inputModel(invalidModelTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeArray() throws Exception {
        String mock = inputScope(invalidScopeTypeArray);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeFloat() throws Exception {
        String mock = inputScope(invalidScopeTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeInteger() throws Exception {
        String mock = inputScope(invalidScopeTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeString() throws Exception {
        String mock = inputScope(invalidScopeTypeString);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseEntityModel(requestBody);
        ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesNotFoundArray() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesNotFoundArray);
        JsonNode requestBody = parseRequestBody(mock);
        HashSet<String> indicesFilter = ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.filterIndices(ModelsAction.parseIndices(requestBody.get("model")), indicesFilter);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesNotFoundString() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesNotFoundString);
        JsonNode requestBody = parseRequestBody(mock);
        HashSet<String> indicesFilter = ResolutionAction.parseIndicesScope(requestBody);
        ResolutionAction.filterIndices(ModelsAction.parseIndices(requestBody.get("model")), indicesFilter);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeArrayFloat() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeArrayFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeArrayInteger() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeArrayInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeArrayObject() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeArrayObject);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeArrayNull() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeArrayNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeArrayStringEmpty() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeArrayStringEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeFloat() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeInteger() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIndicesTypeObject() throws Exception {
        String mock = inputScopeIndices(invalidScopeIndicesTypeObject);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseIndicesScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversNotFoundArray() throws Exception {
        String mock = inputScopeIndices(invalidScopeResolversNotFoundArray);
        JsonNode requestBody = parseRequestBody(mock);
        HashSet<String> resolversScope = ResolutionAction.parseResolversScope(requestBody);
        ResolutionAction.filterResolvers(ModelsAction.parseResolvers(requestBody.get("model")), resolversScope);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversNotFoundString() throws Exception {
        String mock = inputScopeIndices(invalidScopeResolversNotFoundString);
        JsonNode requestBody = parseRequestBody(mock);
        HashSet<String> resolversScope = ResolutionAction.parseResolversScope(requestBody);
        ResolutionAction.filterResolvers(ModelsAction.parseResolvers(requestBody.get("model")), resolversScope);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeArrayFloat() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeArrayFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeArrayInteger() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeArrayInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeArrayObject() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeArrayObject);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeArrayNull() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeArrayNull);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeArrayStringEmpty() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeArrayStringEmpty);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeFloat() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeFloat);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeInteger() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeInteger);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeResolversTypeObject() throws Exception {
        String mock = inputScopeResolvers(invalidScopeResolversTypeObject);
        JsonNode requestBody = parseRequestBody(mock);
        ResolutionAction.parseResolversScope(requestBody);
    }

}
