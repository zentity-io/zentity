package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

public class ModelsActionTest {

    // Valid entity model
    private static final String validQueryTemplate = "\"term\":{\"{{field}}\":\"{{value}}\"}";
    private static final String validMatcher = "\"matcher_name\":{" + validQueryTemplate + "}";
    private static final String validAttribute = "\"attribute_name\":{" + validMatcher + "}";
    private static final String validAttributes = "\"attributes\":{" + validAttribute + "}";
    private static final String validIndex = "\"index_name\":{\"attribute_name.matcher_name\":\"field_name\"}";
    private static final String validIndices = "\"indices\":{" + validIndex + "}";
    private static final String validResolver = "\"resolver_name\":[\"attribute_name\"]";
    private static final String validResolvers = "\"resolvers\":{" + validResolver + "}";
    public static final String validEntityModel = "{" + validAttributes + "," + validIndices + "," + validResolvers + "}";

    // Invalid entity model
    private static final String invalidEntityModelAttributesMissing = "{" + validIndices + "," + validResolvers + "}";
    private static final String invalidEntityModelIndicesMissing = "{" + validAttributes + "," + validResolvers + "}";
    private static final String invalidEntityModelResolversMissing = "{" + validAttributes + "," + validIndices + "}";

    // Invalid attribute
    private static final String invalidAttributeEmpty = "\"attribute_name\":{}";
    private static final String invalidAttributeNameMissing = "\"\":{" + validMatcher + "}";
    private static final String invalidAttributeNamePeriod = "\"attribute.name\":{" + validMatcher + "}";
    private static final String invalidAttributeTypeArray = "\"attribute_name\":[]";
    private static final String invalidAttributeTypeFloat = "\"attribute_name\":1.0";
    private static final String invalidAttributeTypeInteger = "\"attribute_name\":1";
    private static final String invalidAttributeTypeNull = "\"attribute_name\":null";
    private static final String invalidAttributeTypeString = "\"attribute_name\":\"abc\"";

    // Invalid attributes
    private static final String invalidAttributesEmpty = "\"attributes\":{}";
    private static final String invalidAttributesTypeArray = "\"attributes\":[]";
    private static final String invalidAttributesTypeFloat = "\"attributes\":1.0";
    private static final String invalidAttributesTypeInteger = "\"attributes\":1";
    private static final String invalidAttributesTypeNull = "\"attributes\":null";
    private static final String invalidAttributesTypeString = "\"attributes\":\"abc\"";

    // Invalid index
    private static final String invalidIndexAttributeNameMissing = "\"index_name\":{\"\":\"field_name\"}";
    private static final String invalidIndexAttributeNamePeriod = "\"index_name\":{\"attribute.name.matcher_name\":\"field_name\"}";
    private static final String invalidIndexEmpty = "\"index_name\":{}";
    private static final String invalidIndexMatcherEmpty = "\"index_name\":{\"attribute_name.matcher_name\":\"\"}";
    private static final String invalidIndexMatcherNameMissing = "\"index_name\":{\"attribute_name\":\"field_name\"}";
    private static final String invalidIndexMatcherNamePeriod = "\"index_name\":{\"attribute_name.matcher.name\":\"field_name\"}";
    private static final String invalidIndexMatcherTypeArray = "\"index_name\":{\"attribute_name.matcher_name\":[]}";
    private static final String invalidIndexMatcherTypeFloat = "\"index_name\":{\"attribute_name.matcher_name\":1.0}";
    private static final String invalidIndexMatcherTypeInteger = "\"index_name\":{\"attribute_name.matcher_name\":1}";
    private static final String invalidIndexMatcherTypeNull = "\"index_name\":{\"attribute_name.matcher_name\":null}";
    private static final String invalidIndexMatcherTypeObject = "\"index_name\":{\"attribute_name.matcher_name\":{\"abc\":\"xyz\"}}";
    private static final String invalidIndexNameMissing = "\"\":{\"attribute.name.matcher_name\":\"field_name\"}";
    private static final String invalidIndexTypeArray = "\"index_name\":[]";
    private static final String invalidIndexTypeFloat = "\"index_name\":1.0";
    private static final String invalidIndexTypeInteger = "\"index_name\":1";
    private static final String invalidIndexTypeNull = "\"index_name\":null";
    private static final String invalidIndexTypeString = "\"index_name\":\"abc\"";

    // Invalid indices
    private static final String invalidIndicesEmpty = "\"indices\":{}";
    private static final String invalidIndicesTypeArray = "\"indices\":[]";
    private static final String invalidIndicesTypeFloat = "\"indices\":1.0";
    private static final String invalidIndicesTypeInteger = "\"indices\":1";
    private static final String invalidIndicesTypeNull = "\"indices\":null";
    private static final String invalidIndicesTypeString = "\"indices\":\"abc\"";

    // Invalid resolver
    private static final String invalidResolverAttributeNameMissing = "\"resolver_name\":[\"\"]";
    private static final String invalidResolverAttributeNamePeriod = "\"resolver_name\":[\"attribute.name\"]";
    private static final String invalidResolverEmpty = "\"resolver_name\":[]";
    private static final String invalidResolverNameMissing = "\"\":[\"attribute_name\"]";
    private static final String invalidResolverNamePeriod = "\"resolver.name\":[\"attribute_name\"]";
    private static final String invalidResolverTypeFloat = "\"resolver_name\":1.0";
    private static final String invalidResolverTypeInteger = "\"resolver_name\":1";
    private static final String invalidResolverTypeNull = "\"resolver_name\":null";
    private static final String invalidResolverTypeObject = "\"resolver_name\":{\"abc\":\"xyz\"}";
    private static final String invalidResolverTypeString = "\"resolver_name\":\"abc\"";

    // Invalid resolvers
    private static final String invalidResolversEmpty = "\"resolvers\":[]";
    private static final String invalidResolversTypeFloat = "\"resolvers\":1.0";
    private static final String invalidResolversTypeInteger = "\"resolvers\":1";
    private static final String invalidResolversTypeNull = "\"resolvers\":null";
    private static final String invalidResolversTypeObject = "\"resolvers\":{\"abc\":\"xyz\"}";
    private static final String invalidResolversTypeString = "\"resolvers\":\"abc\"";

    private static String entityModelAttribute(String attribute) {
        String attributes = "\"attributes\":{" + attribute + "}";
        return "{" + attributes + "," + validIndices + "," + validResolvers + "}";
    }

    private static String entityModelAttributes(String attributes) {
        return "{" + attributes + "," + validIndices + "," + validResolvers + "}";
    }

    private static String entityModelIndex(String index) {
        String indices = "\"indices\":{" + index + "}";
        return "{" + validAttributes + "," + indices + "," + validResolvers + "}";
    }

    private static String entityModelIndices(String indices) {
        return "{" + validAttributes + "," + indices + "," + validResolvers + "}";
    }

    private static String entityModelResolver(String resolver) {
        String resolvers = "\"resolvers\":{" + resolver + "}";
        return "{" + validAttributes + "," + validIndices + "," + resolvers + "}";
    }

    private static String entityModelResolvers(String resolvers) {
        return "{" + validAttributes + "," + validIndices + "," + resolvers + "}";
    }

    @Test
    public void testParseValidEntityModel() throws Exception {
        ModelsAction.parseEntityModel(validEntityModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testParseValidAttributes() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(validEntityModel);
        ModelsAction.parseAttributes(entityModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testParseValidIndices() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(validEntityModel);
        ModelsAction.parseIndices(entityModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testParseValidResolvers() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(validEntityModel);
        ModelsAction.parseResolvers(entityModel);
        Assert.assertTrue(true);
    }

    @Test(expected = BadRequestException.class)
    public void testParseInvalidAttributesMissing() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(invalidEntityModelAttributesMissing);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testParseInvalidIndicesMissing() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(invalidEntityModelIndicesMissing);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testParseInvalidResolversMissing() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(invalidEntityModelResolversMissing);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeEmpty() throws Exception {
        String mock = entityModelAttribute(invalidAttributeEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeNameMissing() throws Exception {
        String mock = entityModelAttribute(invalidAttributeNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeNamePeriod() throws Exception {
        String mock = entityModelAttribute(invalidAttributeNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeTypeArray() throws Exception {
        String mock = entityModelAttribute(invalidAttributeTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeTypeFloat() throws Exception {
        String mock = entityModelAttribute(invalidAttributeTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeTypeInteger() throws Exception {
        String mock = entityModelAttribute(invalidAttributeTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeTypeNull() throws Exception {
        String mock = entityModelAttribute(invalidAttributeTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributeTypeString() throws Exception {
        String mock = entityModelAttribute(invalidAttributeTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesEmpty() throws Exception {
        String mock = entityModelAttributes(invalidAttributesEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeArray() throws Exception {
        String mock = entityModelAttributes(invalidAttributesTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeFloat() throws Exception {
        String mock = entityModelAttributes(invalidAttributesTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeInteger() throws Exception {
        String mock = entityModelAttributes(invalidAttributesTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeNull() throws Exception {
        String mock = entityModelAttributes(invalidAttributesTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidAttributesTypeString() throws Exception {
        String mock = entityModelAttributes(invalidAttributesTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexAttributeNameMissing() throws Exception {
        String mock = entityModelIndex(invalidIndexAttributeNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexAttributeNamePeriod() throws Exception {
        String mock = entityModelIndex(invalidIndexAttributeNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexEmpty() throws Exception {
        String mock = entityModelIndex(invalidIndexEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherEmpty() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherNameMissing() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherNamePeriod() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherTypeArray() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherTypeFloat() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherTypeInteger() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherTypeNull() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexMatcherTypeObject() throws Exception {
        String mock = entityModelIndex(invalidIndexMatcherTypeObject);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexNameMissing() throws Exception {
        String mock = entityModelIndex(invalidIndexNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexTypeArray() throws Exception {
        String mock = entityModelIndex(invalidIndexTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexTypeFloat() throws Exception {
        String mock = entityModelIndex(invalidIndexTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexTypeInteger() throws Exception {
        String mock = entityModelIndex(invalidIndexTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexTypeNull() throws Exception {
        String mock = entityModelIndex(invalidIndexTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndexTypeString() throws Exception {
        String mock = entityModelIndex(invalidIndexTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndicesEmpty() throws Exception {
        String mock = entityModelIndices(invalidIndicesEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndicesTypeArray() throws Exception {
        String mock = entityModelIndices(invalidIndicesTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndicesTypeFloat() throws Exception {
        String mock = entityModelIndices(invalidIndicesTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndicesTypeInteger() throws Exception {
        String mock = entityModelIndices(invalidIndicesTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndicesTypeNull() throws Exception {
        String mock = entityModelIndices(invalidIndicesTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidIndicesTypeString() throws Exception {
        String mock = entityModelIndices(invalidIndicesTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverAttributeNameMissing() throws Exception {
        String mock = entityModelResolver(invalidResolverAttributeNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverAttributeNamePeriod() throws Exception {
        String mock = entityModelResolver(invalidResolverAttributeNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverEmpty() throws Exception {
        String mock = entityModelResolver(invalidResolverEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverNameMissing() throws Exception {
        String mock = entityModelResolver(invalidResolverNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverNamePeriod() throws Exception {
        String mock = entityModelResolver(invalidResolverNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverTypeFloat() throws Exception {
        String mock = entityModelResolver(invalidResolverTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverTypeInteger() throws Exception {
        String mock = entityModelResolver(invalidResolverTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverTypeNull() throws Exception {
        String mock = entityModelResolver(invalidResolverTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverTypeObject() throws Exception {
        String mock = entityModelResolver(invalidResolverTypeObject);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolverTypeString() throws Exception {
        String mock = entityModelResolver(invalidResolverTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolversEmpty() throws Exception {
        String mock = entityModelResolvers(invalidResolversEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolversTypeFloat() throws Exception {
        String mock = entityModelResolvers(invalidResolversTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolversTypeInteger() throws Exception {
        String mock = entityModelResolvers(invalidResolversTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolversTypeNull() throws Exception {
        String mock = entityModelResolvers(invalidResolversTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolversTypeObject() throws Exception {
        String mock = entityModelResolvers(invalidResolversTypeObject);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testInvalidResolversTypeString() throws Exception {
        String mock = entityModelResolvers(invalidResolversTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

}
