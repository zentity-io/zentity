package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

public class ModelsActionTest {

    // Valid entity model
    private static final String queryTemplateValid = "\"term\":{\"{{field}}\":\"{{value}}\"}";
    private static final String matcherValid = "\"matcher_name\":{" + queryTemplateValid + "}";
    private static final String attributeValid = "\"attribute_name\":{" + matcherValid + "}";
    private static final String attributesValid = "\"attributes\":{" + attributeValid + "}";
    private static final String indexValid = "\"index_name\":{\"attribute_name.matcher_name\":\"field_name\"}";
    private static final String indicesValid = "\"indices\":{" + indexValid + "}";
    private static final String resolverValid = "\"resolver_name\":[\"attribute_name\"]";
    private static final String resolversValid = "\"resolvers\":{" + resolverValid + "}";
    public static final String entityModelValid = "{" + attributesValid + "," + indicesValid + "," + resolversValid + "}";

    // Invalid entity model
    private static final String entityModelAttributesMissing = "{" + indicesValid + "," + resolversValid + "}";
    private static final String entityModelIndicesMissing = "{" + attributesValid + "," + resolversValid + "}";
    private static final String entityModelResolversMissing = "{" + attributesValid + "," + indicesValid + "}";

    // Invalid attribute
    private static final String attributeInvalidEmpty = "\"attribute_name\":{}";
    private static final String attributeInvalidNameMissing = "\"\":{" + matcherValid + "}";
    private static final String attributeInvalidNamePeriod = "\"attribute.name\":{" + matcherValid + "}";
    private static final String attributeInvalidTypeArray = "\"attribute_name\":[]";
    private static final String attributeInvalidTypeFloat = "\"attribute_name\":1.0";
    private static final String attributeInvalidTypeInteger = "\"attribute_name\":1";
    private static final String attributeInvalidTypeNull = "\"attribute_name\":null";
    private static final String attributeInvalidTypeString = "\"attribute_name\":\"abc\"";

    // Invalid attributes
    private static final String attributesInvalidEmpty = "\"attributes\":{}";
    private static final String attributesInvalidTypeArray = "\"attributes\":[]";
    private static final String attributesInvalidTypeFloat = "\"attributes\":1.0";
    private static final String attributesInvalidTypeInteger = "\"attributes\":1";
    private static final String attributesInvalidTypeNull = "\"attributes\":null";
    private static final String attributesInvalidTypeString = "\"attributes\":\"abc\"";

    // Invalid index
    private static final String indexInvalidAttributeNameMissing = "\"index_name\":{\"\":\"field_name\"}";
    private static final String indexInvalidAttributeNamePeriod = "\"index_name\":{\"attribute.name.matcher_name\":\"field_name\"}";
    private static final String indexInvalidEmpty = "\"index_name\":{}";
    private static final String indexInvalidMatcherEmpty = "\"index_name\":{\"attribute_name.matcher_name\":\"\"}";
    private static final String indexInvalidMatcherNameMissing = "\"index_name\":{\"attribute_name\":\"field_name\"}";
    private static final String indexInvalidMatcherNamePeriod = "\"index_name\":{\"attribute_name.matcher.name\":\"field_name\"}";
    private static final String indexInvalidMatcherTypeArray = "\"index_name\":{\"attribute_name.matcher_name\":[]}";
    private static final String indexInvalidMatcherTypeFloat = "\"index_name\":{\"attribute_name.matcher_name\":1.0}";
    private static final String indexInvalidMatcherTypeInteger = "\"index_name\":{\"attribute_name.matcher_name\":1}";
    private static final String indexInvalidMatcherTypeNull = "\"index_name\":{\"attribute_name.matcher_name\":null}";
    private static final String indexInvalidMatcherTypeObject = "\"index_name\":{\"attribute_name.matcher_name\":{\"abc\":\"xyz\"}}";
    private static final String indexInvalidNameMissing = "\"\":{\"attribute.name.matcher_name\":\"field_name\"}";
    private static final String indexInvalidTypeArray = "\"index_name\":[]";
    private static final String indexInvalidTypeFloat = "\"index_name\":1.0";
    private static final String indexInvalidTypeInteger = "\"index_name\":1";
    private static final String indexInvalidTypeNull = "\"index_name\":null";
    private static final String indexInvalidTypeString = "\"index_name\":\"abc\"";

    // Invalid indices
    private static final String indicesInvalidEmpty = "\"indices\":{}";
    private static final String indicesInvalidTypeArray = "\"indices\":[]";
    private static final String indicesInvalidTypeFloat = "\"indices\":1.0";
    private static final String indicesInvalidTypeInteger = "\"indices\":1";
    private static final String indicesInvalidTypeNull = "\"indices\":null";
    private static final String indicesInvalidTypeString = "\"indices\":\"abc\"";

    // Invalid resolver
    private static final String resolverInvalidAttributeNameMissing = "\"resolver_name\":[\"\"]";
    private static final String resolverInvalidAttributeNamePeriod = "\"resolver_name\":[\"attribute.name\"]";
    private static final String resolverInvalidEmpty = "\"resolver_name\":[]";
    private static final String resolverInvalidNameMissing = "\"\":[\"attribute_name\"]";
    private static final String resolverInvalidNamePeriod = "\"resolver.name\":[\"attribute_name\"]";
    private static final String resolverInvalidTypeFloat = "\"resolver_name\":1.0";
    private static final String resolverInvalidTypeInteger = "\"resolver_name\":1";
    private static final String resolverInvalidTypeNull = "\"resolver_name\":null";
    private static final String resolverInvalidTypeObject = "\"resolver_name\":{\"abc\":\"xyz\"}";
    private static final String resolverInvalidTypeString = "\"resolver_name\":\"abc\"";

    // Invalid resolvers
    private static final String resolversInvalidEmpty = "\"resolvers\":[]";
    private static final String resolversInvalidTypeFloat = "\"resolvers\":1.0";
    private static final String resolversInvalidTypeInteger = "\"resolvers\":1";
    private static final String resolversInvalidTypeNull = "\"resolvers\":null";
    private static final String resolversInvalidTypeObject = "\"resolvers\":{\"abc\":\"xyz\"}";
    private static final String resolversInvalidTypeString = "\"resolvers\":\"abc\"";

    private static String entityModelAttribute(String attribute) {
        String attributes = "\"attributes\":{" + attribute + "}";
        return "{" + attributes + "," + indicesValid + "," + resolversValid + "}";
    }

    private static String entityModelAttributes(String attributes) {
        return "{" + attributes + "," + indicesValid + "," + resolversValid + "}";
    }

    private static String entityModelIndex(String index) {
        String indices = "\"indices\":{" + index + "}";
        return "{" + attributesValid + "," + indices + "," + resolversValid + "}";
    }

    private static String entityModelIndices(String indices) {
        return "{" + attributesValid + "," + indices + "," + resolversValid + "}";
    }

    private static String entityModelResolver(String resolver) {
        String resolvers = "\"resolvers\":{" + resolver + "}";
        return "{" + attributesValid + "," + indicesValid + "," + resolvers + "}";
    }

    private static String entityModelResolvers(String resolvers) {
        return "{" + attributesValid + "," + indicesValid + "," + resolvers + "}";
    }

    @Test
    public void testParseEntityModelValid() throws Exception {
        ModelsAction.parseEntityModel(entityModelValid);
        Assert.assertTrue(true);
    }

    @Test
    public void testParseAttributesValid() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(entityModelValid);
        ModelsAction.parseAttributes(entityModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testParseIndicesValid() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(entityModelValid);
        ModelsAction.parseIndices(entityModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testParseResolversValid() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(entityModelValid);
        ModelsAction.parseResolvers(entityModel);
        Assert.assertTrue(true);
    }

    @Test(expected = BadRequestException.class)
    public void testParseAttributesMissing() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(entityModelAttributesMissing);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testParseIndicesMissing() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(entityModelIndicesMissing);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testParseResolversMissing() throws Exception {
        JsonNode entityModel = ModelsAction.parseEntityModel(entityModelResolversMissing);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidEmpty() throws Exception {
        String mock = entityModelAttribute(attributeInvalidEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidNameMissing() throws Exception {
        String mock = entityModelAttribute(attributeInvalidNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidNamePeriod() throws Exception {
        String mock = entityModelAttribute(attributeInvalidNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidTypeArray() throws Exception {
        String mock = entityModelAttribute(attributeInvalidTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidTypeFloat() throws Exception {
        String mock = entityModelAttribute(attributeInvalidTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidTypeInteger() throws Exception {
        String mock = entityModelAttribute(attributeInvalidTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidTypeNull() throws Exception {
        String mock = entityModelAttribute(attributeInvalidTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributeInvalidTypeString() throws Exception {
        String mock = entityModelAttribute(attributeInvalidTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidEmpty() throws Exception {
        String mock = entityModelAttributes(attributesInvalidEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeArray() throws Exception {
        String mock = entityModelAttributes(attributesInvalidTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeFloat() throws Exception {
        String mock = entityModelAttributes(attributesInvalidTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeInteger() throws Exception {
        String mock = entityModelAttributes(attributesInvalidTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeNull() throws Exception {
        String mock = entityModelAttributes(attributesInvalidTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testAttributesInvalidTypeString() throws Exception {
        String mock = entityModelAttributes(attributesInvalidTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseAttributes(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidAttributeNameMissing() throws Exception {
        String mock = entityModelIndex(indexInvalidAttributeNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidAttributeNamePeriod() throws Exception {
        String mock = entityModelIndex(indexInvalidAttributeNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidEmpty() throws Exception {
        String mock = entityModelIndex(indexInvalidEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherEmpty() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherNameMissing() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherNamePeriod() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherTypeArray() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherTypeFloat() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherTypeInteger() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherTypeNull() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidMatcherTypeObject() throws Exception {
        String mock = entityModelIndex(indexInvalidMatcherTypeObject);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidNameMissing() throws Exception {
        String mock = entityModelIndex(indexInvalidNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidTypeArray() throws Exception {
        String mock = entityModelIndex(indexInvalidTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidTypeFloat() throws Exception {
        String mock = entityModelIndex(indexInvalidTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidTypeInteger() throws Exception {
        String mock = entityModelIndex(indexInvalidTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidTypeNull() throws Exception {
        String mock = entityModelIndex(indexInvalidTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndexInvalidTypeString() throws Exception {
        String mock = entityModelIndex(indexInvalidTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndicesInvalidEmpty() throws Exception {
        String mock = entityModelIndices(indicesInvalidEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndicesInvalidTypeArray() throws Exception {
        String mock = entityModelIndices(indicesInvalidTypeArray);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndicesInvalidTypeFloat() throws Exception {
        String mock = entityModelIndices(indicesInvalidTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndicesInvalidTypeInteger() throws Exception {
        String mock = entityModelIndices(indicesInvalidTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndicesInvalidTypeNull() throws Exception {
        String mock = entityModelIndices(indicesInvalidTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testIndicesInvalidTypeString() throws Exception {
        String mock = entityModelIndices(indicesInvalidTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseIndices(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidAttributeNameMissing() throws Exception {
        String mock = entityModelResolver(resolverInvalidAttributeNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidAttributeNamePeriod() throws Exception {
        String mock = entityModelResolver(resolverInvalidAttributeNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidEmpty() throws Exception {
        String mock = entityModelResolver(resolverInvalidEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidNameMissing() throws Exception {
        String mock = entityModelResolver(resolverInvalidNameMissing);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidNamePeriod() throws Exception {
        String mock = entityModelResolver(resolverInvalidNamePeriod);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidTypeFloat() throws Exception {
        String mock = entityModelResolver(resolverInvalidTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidTypeInteger() throws Exception {
        String mock = entityModelResolver(resolverInvalidTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidTypeNull() throws Exception {
        String mock = entityModelResolver(resolverInvalidTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidTypeObject() throws Exception {
        String mock = entityModelResolver(resolverInvalidTypeObject);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolverInvalidTypeString() throws Exception {
        String mock = entityModelResolver(resolverInvalidTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolversInvalidEmpty() throws Exception {
        String mock = entityModelResolvers(resolversInvalidEmpty);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolversInvalidTypeFloat() throws Exception {
        String mock = entityModelResolvers(resolversInvalidTypeFloat);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolversInvalidTypeInteger() throws Exception {
        String mock = entityModelResolvers(resolversInvalidTypeInteger);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolversInvalidTypeNull() throws Exception {
        String mock = entityModelResolvers(resolversInvalidTypeNull);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolversInvalidTypeObject() throws Exception {
        String mock = entityModelResolvers(resolversInvalidTypeObject);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

    @Test(expected = BadRequestException.class)
    public void testResolversInvalidTypeString() throws Exception {
        String mock = entityModelResolvers(resolversInvalidTypeString);
        JsonNode entityModel = ModelsAction.parseEntityModel(mock);
        ModelsAction.parseResolvers(entityModel);
    }

}
