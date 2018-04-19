package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zentity.model.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ResolutionActionTest {

    // Valid input
    private static final String validAttributeString = "\"attribute_string\":\"abc\"";
    private static final String validAttributeArray = "\"attribute_array\":[\"abc\"]";
    private static final String validAttributes = "\"attributes\":{" + validAttributeString + "," + validAttributeArray + "}";
    private static final String validModel = "\"model\":" + ModelTest.VALID_OBJECT;
    private static final String validScopeAttributesEmpty = "\"attributes\":{}";
    private static final String validScopeAttributesTypeNull = "\"attributes\":null";
    private static final String validScopeAttributeTypeArray = "\"attributes\":{\"attribute_name\":[\"abc\"]}";
    private static final String validScopeAttributeTypeArrayEmpty = "\"attributes\":{\"attribute_name\":[]}";
    private static final String validScopeAttributeTypeNull = "\"attributes\":{\"attribute_name\":null}";
    private static final String validScopeAttributeTypeBoolean = "\"attributes\":{\"attribute_type_boolean\":true}";
    private static final String validScopeAttributeTypeNumber = "\"attributes\":{\"attribute_type_number\":1.0}";
    private static final String validScopeAttributeTypeString = "\"attributes\":{\"attribute_type_string\":\"abc\"}";
    private static final String validScopeIndicesTypeArray = "\"indices\":[\"index_name_a\"]";
    private static final String validScopeIndicesTypeArrayEmpty = "\"indices\":[]";
    private static final String validScopeIndicesTypeNull = "\"indices\":null";
    private static final String validScopeIndicesTypeString = "\"indices\":\"index_name_a\"";
    private static final String validScopeResolversTypeArray = "\"resolvers\":[\"resolver_name_a\"]";
    private static final String validScopeResolversTypeArrayEmpty = "\"resolvers\":[]";
    private static final String validScopeResolversTypeNull = "\"resolvers\":null";
    private static final String validScopeResolversTypeString = "\"resolvers\":\"resolver_name_a\"";
    private static final String validScopeEmpty = "\"scope\":{}";
    private static final String validScopeTypeNull = "\"scope\":null";
    private static final String validScopeExcludeEmpty = "\"scope\":{\"exclude\":{}}";
    private static final String validScopeExcludeTypeNull = "\"scope\":{\"exclude\":null}";
    private static final String validScopeIncludeEmpty = "\"scope\":{\"include\":{}}";
    private static final String validScopeIncludeTypeNull = "\"scope\":{\"include\":null}";
    private static final String validInput = "{" + validAttributes + "," + validModel + "}";

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
    private static final String invalidScopeUnrecognizedField = "\"scope\":{\"foo\":{}}";
    private static final String invalidScopeTypeArray = "\"scope\":[]";
    private static final String invalidScopeTypeFloat = "\"scope\":1.0";
    private static final String invalidScopeTypeInteger = "\"scope\":1";
    private static final String invalidScopeTypeString = "\"scope\":\"abc\"";
    private static final String invalidScopeExcludeTypeArray = "\"scope\":{\"exclude\":[]}";
    private static final String invalidScopeExcludeTypeBoolean = "\"scope\":{\"exclude\":true}";
    private static final String invalidScopeExcludeTypeFloat = "\"scope\":{\"exclude\":1.0}";
    private static final String invalidScopeExcludeTypeInteger = "\"scope\":{\"exclude\":1}";
    private static final String invalidScopeExcludeTypeString = "\"scope\":{\"exclude\":\"abc\"}";
    private static final String invalidScopeIncludeTypeArray = "\"scope\":{\"exclude\":[]}";
    private static final String invalidScopeIncludeTypeBoolean = "\"scope\":{\"exclude\":true}";
    private static final String invalidScopeIncludeTypeFloat = "\"scope\":{\"exclude\":1.0}";
    private static final String invalidScopeIncludeTypeInteger = "\"scope\":{\"exclude\":1}";
    private static final String invalidScopeIncludeTypeString = "\"scope\":{\"exclude\":\"abc\"}";

    // Invalid scope.attributes
    private static final String invalidScopeAttributeNotFoundArray = "\"attributes\":{\"attribute_name_x\":[\"abc\"]}";
    private static final String invalidScopeAttributeNotFoundString = "\"attributes\":{\"attribute_name_x\":\"abc\"}";
    private static final String invalidScopeAttributesTypeArray = "\"attributes\":[]";
    private static final String invalidScopeAttributesTypeBoolean = "\"attributes\":true";
    private static final String invalidScopeAttributesTypeFloat = "\"attributes\":1.0";
    private static final String invalidScopeAttributesTypeInteger = "\"attributes\":1";
    private static final String invalidScopeAttributesTypeString = "\"attributes\":\"abc\"";

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

    // Attribute types
    private static final String validAttributeTypes = "\"attributes\":{\"attribute_type_string\":[\"abc\"],\"attribute_type_number\":1.0,\"attribute_type_boolean\":true}";
    private static final String validAttributeTypesModel = "\"model\": {\n" +
            "  \"attributes\":{\"attribute_type_string\":{\"type\":\"string\"},\"attribute_type_number\":{\"type\":\"number\"},\"attribute_type_boolean\":{\"type\":\"boolean\"}},\n" +
            "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
            "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
            "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
            "}";

    private static String inputAttributes(String attributes) {
        return "{" + attributes + "," + validModel + "}";
    }

    private static String inputModel(String model) {
        return "{" + validAttributes + "," + model + "}";
    }

    private static String inputModelAttributeTypes(String attributes) {
        return "{" + attributes + "," + validAttributeTypesModel + "}";
    }

    private static String inputModelAttributeTypesScope(String scope) {
        return "{" + validAttributeTypes + "," + validAttributeTypesModel + "," + scope + "}";
    }

    private static String inputScope(String scope) {
        return "{" + validAttributes + "," + validModel + "," + scope + "}";
    }

    private static String inputScopeExcludeAttributes(String scopeAttributes) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"exclude\":{" + scopeAttributes + "}}}";
    }

    private static String inputScopeExcludeIndices(String scopeIndices) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"exclude\":{" + scopeIndices + "}}}";
    }

    private static String inputScopeExcludeResolvers(String scopeResolvers) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"exclude\":{" + scopeResolvers + "}}}";
    }

    private static String inputScopeIncludeAttributes(String scopeAttributes) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"include\":{" + scopeAttributes + "}}}";
    }

    private static String inputScopeIncludeIndices(String scopeIndices) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"include\":{" + scopeIndices + "}}}";
    }

    private static String inputScopeIncludeResolvers(String scopeResolvers) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"include\":{" + scopeResolvers + "}}}";
    }

    private static JsonNode parseRequestBody(String mock) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(mock);
    }

    private static void parseInput(JsonNode requestBody) throws IOException, ValidationException, BadRequestException {
        Model model = new Model(ModelTest.VALID_OBJECT);
        ResolutionAction.parseAttributes(model, requestBody);
        ResolutionAction.parseEntityModel(requestBody);
        if (requestBody.has("scope")) {
            ResolutionAction.parseScope(requestBody.get("scope"));
            if (requestBody.get("scope").has("exclude")) {
                ResolutionAction.parseScopeExclude(requestBody.get("scope").get("exclude"));
                if (requestBody.get("scope").get("exclude").has("attributes"))
                    ResolutionAction.parseScopeExcludeAttributes(model, requestBody.get("scope").get("exclude").get("attributes"));
                if (requestBody.get("scope").get("exclude").has("indices"))
                    ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
                if (requestBody.get("scope").get("exclude").has("resolvers"))
                    ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
            }
            if (requestBody.get("scope").has("include")) {
                ResolutionAction.parseScopeInclude(requestBody.get("scope").get("include"));
                if (requestBody.get("scope").get("include").has("attributes"))
                    ResolutionAction.parseScopeIncludeAttributes(model, requestBody.get("scope").get("include").get("attributes"));
                if (requestBody.get("scope").get("include").has("indices"))
                    ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
                if (requestBody.get("scope").get("include").has("resolvers"))
                    ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
            }
        }
    }

    @Test
    public void testValidInput() throws Exception {
        parseInput(parseRequestBody(validInput));
    }

    ////  "attributes"  ////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesEmpty() throws Exception {
        ResolutionAction.parseAttributes(new Model(ModelTest.VALID_OBJECT), parseRequestBody(inputAttributes(invalidAttributesEmpty)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeArray() throws Exception {
        ResolutionAction.parseAttributes(new Model(ModelTest.VALID_OBJECT), parseRequestBody(inputAttributes(invalidAttributesTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeFloat() throws Exception {
        ResolutionAction.parseAttributes(new Model(ModelTest.VALID_OBJECT), parseRequestBody(inputAttributes(invalidAttributesTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeInteger() throws Exception {
        ResolutionAction.parseAttributes(new Model(ModelTest.VALID_OBJECT), parseRequestBody(inputAttributes(invalidAttributesTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeNull() throws Exception {
        ResolutionAction.parseAttributes(new Model(ModelTest.VALID_OBJECT), parseRequestBody(inputAttributes(invalidAttributesTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeString() throws Exception {
        ResolutionAction.parseAttributes(new Model(ModelTest.VALID_OBJECT), parseRequestBody(inputAttributes(invalidAttributesTypeString)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputAttributes(invalidScopeAttributeNotFoundArray));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputAttributes(invalidScopeAttributeNotFoundString));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test
    public void testValidAttributeTypeBoolean() throws Exception {
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(validScopeAttributeTypeBoolean));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test
    public void testValidAttributeTypeNumber() throws Exception {
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(validScopeAttributeTypeNumber));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test
    public void testValidAttributeTypeString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(validScopeAttributeTypeString));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeBooleanWhenNumber() throws Exception {
        String attributes = "\"attributes\":{\"attribute_type_boolean\":[1.0]}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(attributes));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeBooleanWhenString() throws Exception {
        String attributes = "\"attributes\":{\"attribute_type_boolean\":[\"abc\"]}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(attributes));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeNumberWhenBoolean() throws Exception {
        String attributes = "\"attributes\":{\"attribute_type_number\":[true]}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(attributes));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeNumberWhenString() throws Exception {
        String attributes = "\"attributes\":{\"attribute_type_number\":[\"abc\"]}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(attributes));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeStringWhenBoolean() throws Exception {
        String attributes = "\"attributes\":{\"attribute_type_string\":[true]}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(attributes));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeStringWhenNumber() throws Exception {
        String attributes = "\"attributes\":{\"attribute_type_string\":[1.0]}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypes(attributes));
        ResolutionAction.parseAttributes(new Model(requestBody.get("model")), requestBody);
    }

    ////  "model"  /////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidModelEmpty() throws Exception {
        ResolutionAction.parseEntityModel(parseRequestBody(inputModel(invalidModelEmpty)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeArray() throws Exception {
        ResolutionAction.parseEntityModel(parseRequestBody(inputModel(invalidModelTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeFloat() throws Exception {
        ResolutionAction.parseEntityModel(parseRequestBody(inputModel(invalidModelTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeInteger() throws Exception {
        ResolutionAction.parseEntityModel(parseRequestBody(inputModel(invalidModelTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeNull() throws Exception {
        ResolutionAction.parseEntityModel(parseRequestBody(inputModel(invalidModelTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidModelTypeString() throws Exception {
        ResolutionAction.parseEntityModel(parseRequestBody(inputModel(invalidModelTypeString)));
    }

    ////  "scope"  /////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeEmpty() throws Exception {
        parseInput(parseRequestBody(inputScope(validScopeEmpty)));
    }

    @Test
    public void testValidScopeTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScope(validScopeTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeFloat() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeInteger() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeTypeString() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeTypeString)));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeUnrecognizedField() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeUnrecognizedField)));
    }

    ////  "scope"."exclude"  ///////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeEmpty() throws Exception {
        parseInput(parseRequestBody(inputScope(validScopeExcludeEmpty)));
    }

    @Test
    public void testValidScopeExcludeTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScope(validScopeExcludeTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeExcludeTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeTypeBoolean() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeExcludeTypeBoolean)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeTypeFloat() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeExcludeTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeTypeInteger() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeExcludeTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeTypeString() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeExcludeTypeString)));
    }

    ////  "scope"."include"  ///////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeEmpty() throws Exception {
        parseInput(parseRequestBody(inputScope(validScopeIncludeEmpty)));
    }

    @Test
    public void testValidScopeIncludeTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScope(validScopeIncludeTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeIncludeTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeTypeBoolean() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeIncludeTypeBoolean)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeTypeFloat() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeIncludeTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeTypeInteger() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeIncludeTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeTypeString() throws Exception {
        parseInput(parseRequestBody(inputScope(invalidScopeIncludeTypeString)));
    }

    ////  "scope"."exclude"."attributes"  //////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeAttributesEmpty() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(validScopeAttributesEmpty)));
    }

    @Test
    public void testValidScopeExcludeAttributesTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(validScopeAttributesTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributesTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributesTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributesTypeBoolean() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributesTypeBoolean)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributesTypeFloat() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributesTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributesTypeInteger() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributesTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributesTypeString() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributesTypeString)));
    }

    ////  "scope"."include"."attributes"  //////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeAttributesEmpty() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(validScopeAttributesEmpty)));
    }

    @Test
    public void testValidScopeIncludeAttributesTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(validScopeAttributesTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributesTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributesTypeArray)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributesTypeBoolean() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributesTypeBoolean)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributesTypeFloat() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributesTypeFloat)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributesTypeInteger() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributesTypeInteger)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributesTypeString() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributesTypeString)));
    }

    ////  "scope"."exclude"."attributes".ATTRIBUTE_NAME  ///////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeAttributeTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(validScopeAttributeTypeArray)));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeArrayEmpty() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(validScopeAttributeTypeArrayEmpty)));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScopeExcludeAttributes(validScopeAttributeTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributeNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributeNotFoundArray));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeAttributeNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeAttributes(invalidScopeAttributeNotFoundString));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeBoolean() throws Exception {
        String scope = "\"scope\":{\"exclude\":{" + validScopeAttributeTypeBoolean + "}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeNumber() throws Exception {
        String scope = "\"scope\":{\"exclude\":{" + validScopeAttributeTypeNumber + "}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeString() throws Exception {
        String scope = "\"scope\":{\"exclude\":{" + validScopeAttributeTypeString + "}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeBooleanWhenNumber() throws Exception {
        String scope = "\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_boolean\":[1.0]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeBooleanWhenString() throws Exception {
        String scope = "\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_boolean\":[\"abc\"]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeNumberWhenBoolean() throws Exception {
        String scope = "\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_number\":[true]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeNumberWhenString() throws Exception {
        String scope = "\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_number\":[\"abc\"]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeStringWhenBoolean() throws Exception {
        String scope = "\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_string\":[true]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeStringWhenNumber() throws Exception {
        String scope = "\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_string\":[1.0]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeExcludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("exclude").get("attributes"));
    }

    ////  "scope"."include"."attributes".ATTRIBUTE_NAME  ///////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeAttributeTypeArray() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(validScopeAttributeTypeArray)));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeArrayEmpty() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(validScopeAttributeTypeArrayEmpty)));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeNull() throws Exception {
        parseInput(parseRequestBody(inputScopeIncludeAttributes(validScopeAttributeTypeNull)));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributeNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributeNotFoundArray));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeAttributeNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeAttributes(invalidScopeAttributeNotFoundString));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeBoolean() throws Exception {
        String scope = "\"scope\":{\"include\":{" + validScopeAttributeTypeBoolean + "}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeNumber() throws Exception {
        String scope = "\"scope\":{\"include\":{" + validScopeAttributeTypeNumber + "}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeString() throws Exception {
        String scope = "\"scope\":{\"include\":{" + validScopeAttributeTypeString + "}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeBooleanWhenNumber() throws Exception {
        String scope = "\"scope\":{\"include\":{\"attributes\":{\"attribute_type_boolean\":[1.0]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeBooleanWhenString() throws Exception {
        String scope = "\"scope\":{\"include\":{\"attributes\":{\"attribute_type_boolean\":[\"abc\"]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeNumberWhenBoolean() throws Exception {
        String scope = "\"scope\":{\"include\":{\"attributes\":{\"attribute_type_number\":[true]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeNumberWhenString() throws Exception {
        String scope = "\"scope\":{\"include\":{\"attributes\":{\"attribute_type_number\":[\"abc\"]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeStringWhenBoolean() throws Exception {
        String scope = "\"scope\":{\"include\":{\"attributes\":{\"attribute_type_string\":[true]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeStringWhenNumber() throws Exception {
        String scope = "\"scope\":{\"include\":{\"attributes\":{\"attribute_type_string\":[1.0]}}}";
        JsonNode requestBody = parseRequestBody(inputModelAttributeTypesScope(scope));
        ResolutionAction.parseScopeIncludeAttributes(new Model(requestBody.get("model")), requestBody.get("scope").get("include").get("attributes"));
    }

    ////  "scope"."exclude"."indices"  /////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeIndicesTypeArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(validScopeIndicesTypeArray));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        Model model = ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(!model.indices().containsKey("index_name_a"));
        Assert.assertTrue(model.indices().containsKey("index_name_b"));
        Assert.assertTrue(model.indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeExcludeIndicesTypeArrayEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(validScopeIndicesTypeArrayEmpty));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        Model model = ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(model.indices().containsKey("index_name_a"));
        Assert.assertTrue(model.indices().containsKey("index_name_b"));
        Assert.assertTrue(model.indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeExcludeIndicesTypeNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(validScopeIndicesTypeNull));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        Model model = ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(model.indices().containsKey("index_name_a"));
        Assert.assertTrue(model.indices().containsKey("index_name_b"));
        Assert.assertTrue(model.indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeExcludeIndicesTypeString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(validScopeIndicesTypeString));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        Model model = ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(!model.indices().containsKey("index_name_a"));
        Assert.assertTrue(model.indices().containsKey("index_name_b"));
        Assert.assertTrue(model.indices().containsKey("index_name_c"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesNotFoundArray));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesNotFoundString));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayFloat));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayInteger));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayNull));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayObject));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayStringEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayStringEmpty));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeFloat));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeInteger));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeIndicesTypeObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeIndices(invalidScopeIndicesTypeObject));
        Set<String> indices = ResolutionAction.parseScopeExcludeIndices(requestBody.get("scope").get("exclude").get("indices"));
        ResolutionAction.excludeIndices(new Model(requestBody.get("model")), indices);
    }

    ////  "scope"."include"."indices"  /////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeIndicesTypeArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(validScopeIndicesTypeArray));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        Model model = ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(model.indices().containsKey("index_name_a"));
        Assert.assertTrue(!model.indices().containsKey("index_name_b"));
        Assert.assertTrue(!model.indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeIncludeIndicesTypeArrayEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(validScopeIndicesTypeArrayEmpty));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        Model model = ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(model.indices().containsKey("index_name_a"));
        Assert.assertTrue(model.indices().containsKey("index_name_b"));
        Assert.assertTrue(model.indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeIncludeIndicesTypeNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(validScopeIndicesTypeNull));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        Model model = ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(model.indices().containsKey("index_name_a"));
        Assert.assertTrue(model.indices().containsKey("index_name_b"));
        Assert.assertTrue(model.indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeIncludeIndicesTypeString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(validScopeIndicesTypeString));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        Model model = ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
        Assert.assertTrue(model.indices().containsKey("index_name_a"));
        Assert.assertTrue(!model.indices().containsKey("index_name_b"));
        Assert.assertTrue(!model.indices().containsKey("index_name_c"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesNotFoundArray));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesNotFoundString));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayFloat));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayInteger));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayObject));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayNull));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayStringEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayStringEmpty));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeFloat));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeInteger));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeIndicesTypeObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeIndices(invalidScopeIndicesTypeObject));
        Set<String> indices = ResolutionAction.parseScopeIncludeIndices(requestBody.get("scope").get("include").get("indices"));
        ResolutionAction.includeIndices(new Model(requestBody.get("model")), indices);
    }

    ////  "scope"."exclude"."resolvers"  ///////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeResolversTypeArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(validScopeResolversTypeArray));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        Model model = ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(!model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeExcludeResolversTypeArrayEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(validScopeResolversTypeArrayEmpty));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        Model model = ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeExcludeResolversTypeNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(validScopeResolversTypeNull));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        Model model = ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeExcludeResolversTypeString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(validScopeResolversTypeString));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        Model model = ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(!model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_c"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversNotFoundArray));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversNotFoundString));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeArrayFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayFloat));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeArrayInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayInteger));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeArrayObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayObject));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeArrayNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayNull));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeArrayStringEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayStringEmpty));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeFloat));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeInteger));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeExcludeResolversTypeObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeExcludeResolvers(invalidScopeResolversTypeObject));
        Set<String> resolvers = ResolutionAction.parseScopeExcludeResolvers(requestBody.get("scope").get("exclude").get("resolvers"));
        ResolutionAction.excludeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    ////  "scope"."include"."resolvers"  ///////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeResolversTypeArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(validScopeResolversTypeArray));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        Model model = ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(!model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(!model.resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeIncludeResolversTypeArrayEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(validScopeResolversTypeArrayEmpty));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        Model model = ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeIncludeResolversTypeNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(validScopeResolversTypeNull));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        Model model = ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeIncludeResolversTypeString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(validScopeResolversTypeString));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        Model model = ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
        Assert.assertTrue(model.resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(!model.resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(!model.resolvers().containsKey("resolver_name_c"));
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversNotFoundArray() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversNotFoundArray));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversNotFoundString() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversNotFoundString));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeArrayFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayFloat));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeArrayInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayInteger));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeArrayObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayObject));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeArrayNull() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayNull));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeArrayStringEmpty() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayStringEmpty));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeFloat() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeFloat));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeInteger() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeInteger));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidScopeIncludeResolversTypeObject() throws Exception {
        JsonNode requestBody = parseRequestBody(inputScopeIncludeResolvers(invalidScopeResolversTypeObject));
        Set<String> resolvers = ResolutionAction.parseScopeIncludeResolvers(requestBody.get("scope").get("include").get("resolvers"));
        ResolutionAction.includeResolvers(new Model(requestBody.get("model")), resolvers);
    }

}
