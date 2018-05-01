package io.zentity.resolution;

import io.zentity.model.Matcher;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.Attribute;
import io.zentity.resolution.input.Input;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JobTest {

    @Test
    public void testMakeResolversClause() throws Exception {
        String attributes = "\"attributes\":{\"name\":{},\"street\":{},\"city\":{},\"state\":{},\"zip\":{},\"phone\":{},\"id\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"name\",\"street\",\"city\",\"state\"]},\"b\":{\"attributes\":[\"name\",\"street\",\"zip\"]},\"c\":{\"attributes\":[\"name\",\"phone\"]},\"d\":{\"attributes\":[\"id\"]}}";
        String matchers = "\"matchers\":{\"x\":{\"clause\":{\"term\":{\"{{field}}\":\"{{value}}\"}}},\"y\":{\"clause\":{\"match\":{\"{{field}}\":\"{{value}}\",\"fuzziness\":\"{{ params.fuzziness }}\"}}},\"z\":{\"clause\":{\"match\":{\"{{field}}\":\"{{value}}\",\"fuzziness\":\"{{ params.fuzziness }}\"}},\"params\":{\"fuzziness\":\"1\"}}}";
        String indices = "\"indices\":{\"index\":{\"fields\":{\"name\":{\"attribute\":\"name\",\"matcher\":\"x\"},\"street\":{\"attribute\":\"street\",\"matcher\":\"x\"},\"city\":{\"attribute\":\"city\",\"matcher\":\"x\"},\"state\":{\"attribute\":\"state\",\"matcher\":\"x\"},\"zip\":{\"attribute\":\"zip\",\"matcher\":\"x\"},\"phone\":{\"attribute\":\"phone\",\"matcher\":\"z\"},\"id\":{\"attribute\":\"id\",\"matcher\":\"y\"}}}}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {" +
                "    \"name\": { \"values\": [ \"Alice Jones\", \"Alice Jones-Smith\" ]}," +
                "    \"street\": { \"values\": [ \"123 Main St\" ]}," +
                "    \"city\": { \"values\": [ \"Beverly Hills\" ]}," +
                "    \"state\": { \"values\": [ \"CA\" ]}," +
                "    \"zip\": [ \"90210\" ]," +
                "    \"phone\": { \"values\": [ \"555-123-4567\" ], \"params\": { \"fuzziness\": \"2\" }}," +
                "    \"id\": { \"values\": [ \"1234567890\" ], \"params\": { \"fuzziness\": \"auto\" }}" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> resolversList = new ArrayList<>();
        resolversList.add("a");
        resolversList.add("b");
        resolversList.add("c");
        resolversList.add("d");
        Map<String, Integer> counts = Job.countAttributesAcrossResolvers(model, resolversList);
        List<List<String>> resolversSorted = Job.sortResolverAttributes(model, resolversList, counts);
        TreeMap<String, TreeMap> resolversFilterTree = Job.makeResolversFilterTree(resolversSorted);
        String resolversClause = Job.populateResolversFilterTree(model, "index", resolversFilterTree, input.attributes());
        String expected = "{\"bool\":{\"should\":[{\"match\":{\"id\":\"1234567890\",\"fuzziness\":\"auto\"}},{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"term\":{\"name\":\"Alice Jones\"}},{\"term\":{\"name\":\"Alice Jones-Smith\"}}]}},{\"bool\":{\"should\":[{\"match\":{\"phone\":\"555-123-4567\",\"fuzziness\":\"2\"}},{\"bool\":{\"filter\":[{\"term\":{\"street\":\"123 Main St\"}},{\"bool\":{\"should\":[{\"bool\":{\"filter\":[{\"term\":{\"city\":\"Beverly Hills\"}},{\"bool\":{\"filter\":{\"term\":{\"state\":\"CA\"}}}}]}},{\"term\":{\"zip\":\"90210\"}}]}}]}}]}}]}}]}}";
        Assert.assertEquals(resolversClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClause() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ]\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": \"{{ value }}\"\n" +
                "    }" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        String matcherClause = Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
        String expected = "{\"match\":{\"field_phone\":\"555-123-4567\"}}";
        Assert.assertEquals(matcherClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables,
     * but don't include {{ field }} and expect an exception to be raised.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseFieldMissing() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ]\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"foo\": {\n" +
                "        \"query\": \"{{ value }}\",\n" +
                "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "      }\n" +
                "    }" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables,
     * but don't include {{ value }} and expect an exception to be raised.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseValueMissing() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ]\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": {\n" +
                "        \"query\": \"foo\",\n" +
                "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "      }\n" +
                "    }" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the input attribute.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttribute() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ],\n" +
                "  \"params\": { \"fuzziness\": 2 }\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": {\n" +
                "        \"query\": \"{{ value }}\",\n" +
                "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "      }\n" +
                "    }" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        String matcherClause = Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"2\"}}}";
        Assert.assertEquals(matcherClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the input attribute and overrides the params of a matcher.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttributeOverridesMatcher() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ],\n" +
                "  \"params\": { \"fuzziness\": 1 }\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": {\n" +
                "        \"query\": \"{{ value }}\",\n" +
                "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "      }\n" +
                "    }" +
                "  },\n" +
                "  \"params\": {\n" +
                "    \"fuzziness\": 2\n" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        String matcherClause = Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"1\"}}}";
        Assert.assertEquals(matcherClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the matcher.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromMatcher() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ]\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": {\n" +
                "        \"query\": \"{{ value }}\",\n" +
                "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "      }\n" +
                "    }" +
                "  },\n" +
                "  \"params\": {\n" +
                "    \"fuzziness\": 2\n" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        String matcherClause = Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"2\"}}}";
        Assert.assertEquals(matcherClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * but don't pass any values to the params and expect an exception to be raised.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseParamsMissing() throws Exception {
        String attributeJson = "{\n" +
                "  \"values\": [ \"555-123-4567\", \"555-098-7654\" ]\n" +
                "}";
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": {\n" +
                "        \"query\": \"{{ value }}\",\n" +
                "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "      }\n" +
                "    }" +
                "  }\n" +
                "}";
        Attribute attribute = new Attribute("attribute_phone", "string", attributeJson);
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Job.populateMatcherClause(matcher, "field_phone", "555-123-4567", attribute);
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified only in the
     * input attribute.
     *
     * @throws Exception
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatInputAttributeOnly() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"format\": \"yyyy-MM-dd\",\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        String scriptFieldsClause = Job.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"doc[params.field].value.toString(params.format)\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd\"}}}}";
        Assert.assertEquals(scriptFieldsClause, expected);
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified only in the
     * matcher.
     *
     * @throws Exception
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatMatcherOnly() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd\"" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"format\": \"yyyy-MM-dd\",\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        String scriptFieldsClause = Job.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"doc[params.field].value.toString(params.format)\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd\"}}}}";
        Assert.assertEquals(scriptFieldsClause, expected);
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified only in the
     * model attribute.
     *
     * @throws Exception
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatModelAttributeOnly() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        String scriptFieldsClause = Job.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"doc[params.field].value.toString(params.format)\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd\"}}}}";
        Assert.assertEquals(scriptFieldsClause, expected);
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified both in the
     * model attribute and the matcher. The param of the model attribute should override the param of the matcher.
     *
     * @throws Exception
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatModelAttributeOverridesMatcher() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd\"" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        String scriptFieldsClause = Job.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"doc[params.field].value.toString(params.format)\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}}";
        Assert.assertEquals(scriptFieldsClause, expected);
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified both in the
     * input attribute and the model attribute. The param of the input attribute should override the param of the
     * model attribute.
     *
     * @throws Exception
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatInputAttributeOverridesModelAttribute() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd\"" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        String scriptFieldsClause = Job.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"doc[params.field].value.toString(params.format)\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\"}}}}";
        Assert.assertEquals(scriptFieldsClause, expected);
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified both in the
     * input attribute and the model attribute, but the value of the input attribute param is null. The param of the
     * input attribute should not override the non-null param of the model attribute.
     *
     * @throws Exception
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatNullNotOverrides() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd\"" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"format\": null,\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        String scriptFieldsClause = Job.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"doc[params.field].value.toString(params.format)\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}}";
        Assert.assertEquals(scriptFieldsClause, expected);
    }

    /**
     * The "script_fields" clause for a "date" type attribute must throw an exception if the "format" param is missing
     * from the matcher, the model attribute, and the input attribute.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testMakeScriptFieldsClauseTypeDateFormatMissing() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        Job.makeScriptFieldsClause(input, "index");
    }

    /**
     * The "script_fields" clause for a "date" type attribute must throw an exception if the only "format" param is null.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testMakeScriptFieldsClauseTypeDateFormatNull() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_ip\": {\n" +
                "    \"clause\": {\n" +
                "      \"term\": {\n" +
                "        \"{{ field }}\": \"{{ value }}\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"matcher_timestamp\": {\n" +
                "    \"clause\": {\n" +
                "      \"range\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
                "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
                "          \"format\": \"{{ params.format }}\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_ip\": {\n" +
                "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
                "      },\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_ip\": {\n" +
                "      \"values\": [\"192.168.0.1\"]\n" +
                "    },\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ],\n" +
                "      \"params\": {\n" +
                "        \"format\": null,\n" +
                "        \"window\": \"15m\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        Job.makeScriptFieldsClause(input, "index");
    }

}
