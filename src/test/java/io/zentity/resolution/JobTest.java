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
package io.zentity.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.Matcher;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.Input;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
        Map<String, Integer> counts = Query.countAttributesAcrossResolvers(model, resolversList);
        List<List<String>> resolversSorted = Query.sortResolverAttributes(model, resolversList, counts);
        TreeMap<String, TreeMap> resolversFilterTree = Query.makeResolversFilterTree(resolversSorted);
        String resolversClause = Query.populateResolversFilterTree(model, "index", resolversFilterTree, input.attributes(), false, new AtomicInteger());
        String expected = "{\"bool\":{\"should\":[{\"match\":{\"id\":\"1234567890\",\"fuzziness\":\"auto\"}},{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"term\":{\"name\":\"Alice Jones\"}},{\"term\":{\"name\":\"Alice Jones-Smith\"}}]}},{\"bool\":{\"should\":[{\"match\":{\"phone\":\"555-123-4567\",\"fuzziness\":\"2\"}},{\"bool\":{\"filter\":[{\"term\":{\"street\":\"123 Main St\"}},{\"bool\":{\"should\":[{\"bool\":{\"filter\":[{\"term\":{\"city\":\"Beverly Hills\"}},{\"term\":{\"state\":\"CA\"}}]}},{\"term\":{\"zip\":\"90210\"}}]}}]}}]}}]}}]}}";
        Assert.assertEquals(resolversClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClause() throws Exception {
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": \"{{ value }}\"\n" +
                "    }" +
                "  }\n" +
                "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        TreeMap<String, String> params = new TreeMap<>();
        String matcherClause = Query.populateMatcherClause(matcher, "field_phone", "555-123-4567", params);
        String expected = "{\"match\":{\"field_phone\":\"555-123-4567\"}}";
        Assert.assertEquals(matcherClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     * Supply parameters that don't exist. Ensure they are ignored without failing the Query.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseIgnoreUnusedParams() throws Exception {
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": \"{{ value }}\"\n" +
                "    }" +
                "  }\n" +
                "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        TreeMap<String, String> params = new TreeMap<>();
        params.put("foo", "bar");
        String matcherClause = Query.populateMatcherClause(matcher, "field_phone", "555-123-4567", params);
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
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        TreeMap<String, String> params = new TreeMap<>();
        Query.populateMatcherClause(matcher, "field_phone", "555-123-4567", params);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables,
     * but don't include {{ value }} and expect an exception to be raised.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseValueMissing() throws Exception {
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
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        TreeMap<String, String> params = new TreeMap<>();
        Query.populateMatcherClause(matcher, "field_phone", "555-123-4567", params);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     * Use a matcher that defines a param but doesn't use it in the clause. Ignore it without failing the Query.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsUnrecognized() throws Exception {
        String matcherJson = "{\n" +
                "  \"clause\": {\n" +
                "    \"match\": {\n" +
                "      \"{{ field }}\": {\n" +
                "        \"query\": \"{{ value }}\",\n" +
                "        \"fuzziness\": \"2\"\n" +
                "      }\n" +
                "    }" +
                "  },\n" +
                "  \"params\": {" +
                "    \"foo\": \"bar\"" +
                "  }\n" +
                "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        TreeMap<String, String> params = new TreeMap<>();
        String matcherClause = Query.populateMatcherClause(matcher, "field_phone", "555-123-4567", params);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"2\"}}}";
        Assert.assertEquals(matcherClause, expected);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the input attribute.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttribute() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_phone\": {\n" +
                "    \"clause\": {\n" +
                "      \"match\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"query\": \"{{ value }}\",\n" +
                "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "        }\n" +
                "      }" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_phone\": {\n" +
                "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_phone\": {\n" +
                "      \"values\": [ \"555-123-4567\" ],\n" +
                "      \"params\": { \"fuzziness\": 1 }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"1\"}}}";
        String actual = attributeClauses.get(0);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the input attribute and overrides the params of a matcher.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttributeOverridesMatcher() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_phone\": {\n" +
                "    \"clause\": {\n" +
                "      \"match\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"query\": \"{{ value }}\",\n" +
                "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "        }\n" +
                "      }" +
                "    },\n" +
                "    \"params\": {\n" +
                "      \"fuzziness\": 2\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_phone\": {\n" +
                "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_phone\": {\n" +
                "      \"values\": [ \"555-123-4567\" ],\n" +
                "      \"params\": { \"fuzziness\": 1 }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"1\"}}}";
        String actual = attributeClauses.get(0);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the matcher.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromMatcher() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_phone\": {\n" +
                "    \"clause\": {\n" +
                "      \"match\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"query\": \"{{ value }}\",\n" +
                "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "        }\n" +
                "      }" +
                "    },\n" +
                "    \"params\": {\n" +
                "      \"fuzziness\": 2\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_phone\": {\n" +
                "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_phone\": {\n" +
                "      \"values\": [ \"555-123-4567\" ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"fuzziness\":\"2\"}}}";
        String actual = attributeClauses.get(0);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the model attribute.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromModelAttribute() throws Exception {
        String attributes = "\"attributes\": {" +
                "  \"attribute_timestamp\": {" +
                "    \"type\": \"date\",\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss\",\n" +
                "      \"window\": \"30m\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
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
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
        String expected = "{\"range\":{\"field_timestamp\":{\"gte\":\"123 Main St||-30m\",\"lte\":\"123 Main St||+30m\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String actual = attributeClauses.get(0);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the model attribute and overrides the params of a matcher.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromModelAttributeOverridesMatcher() throws Exception {
        String attributes = "\"attributes\": {" +
                "  \"attribute_timestamp\": {" +
                "    \"type\": \"date\",\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss\",\n" +
                "      \"window\": \"30m\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
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
                "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",\n" +
                "      \"window\": \"1h\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_timestamp\": {\n" +
                "      \"values\": [ \"123 Main St\" ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
        String expected = "{\"range\":{\"field_timestamp\":{\"gte\":\"123 Main St||-30m\",\"lte\":\"123 Main St||+30m\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String actual = attributeClauses.get(0);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the model attribute and input attribute and overrides the params of a matcher,
     * and where the input attribute takes precedence over the model attribute.
     *
     * @throws Exception
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttributeOverridesModelAttribute() throws Exception {
        String attributes = "\"attributes\": {" +
                "  \"attribute_timestamp\": {" +
                "    \"type\": \"date\",\n" +
                "    \"params\": {\n" +
                "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss\",\n" +
                "      \"window\": \"30m\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
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
                "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",\n" +
                "      \"window\": \"1h\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_timestamp\": {\n" +
                "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
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
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
        String expected = "{\"range\":{\"field_timestamp\":{\"gte\":\"123 Main St||-15m\",\"lte\":\"123 Main St||+15m\",\"format\":\"yyyy-MM-dd\"}}}";
        String actual = attributeClauses.get(0);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * but don't pass any values to the params and expect an exception to be raised.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseParamsMissing() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_phone\": {\n" +
                "    \"clause\": {\n" +
                "      \"match\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"query\": \"{{ value }}\",\n" +
                "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "        }\n" +
                "      }" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_phone\": {\n" +
                "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_phone\": {\n" +
                "      \"values\": [ \"555-123-4567\" ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * but don't pass any values to the required params and expect an exception to be raised.
     *
     * @throws Exception
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseParamsMismatched() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
                "  \"matcher_phone\": {\n" +
                "    \"clause\": {\n" +
                "      \"match\": {\n" +
                "        \"{{ field }}\": {\n" +
                "          \"query\": \"{{ value }}\",\n" +
                "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
                "        }\n" +
                "      }" +
                "    }\n" +
                "  }\n" +
                "}";
        String indices = "\"indices\": {\n" +
                "  \"index\": {\n" +
                "    \"fields\": {\n" +
                "      \"field_phone\": {\n" +
                "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
                "  \"attributes\": {\n" +
                "    \"attribute_phone\": {\n" +
                "      \"values\": [ \"555-123-4567\" ],\n" +
                "      \"params\": { \"foo\": \"bar\" }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Input input = new Input(json, model);
        List<String> attributeClauses = Query.makeAttributeClauses(input.model(), "index", input.attributes(), "filter", false, new AtomicInteger());
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
        String scriptFieldsClause = Query.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd\"}}}}";
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
        String scriptFieldsClause = Query.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd\"}}}}";
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
        String scriptFieldsClause = Query.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd\"}}}}";
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
        String scriptFieldsClause = Query.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}}";
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
        String scriptFieldsClause = Query.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\"}}}}";
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
        String scriptFieldsClause = Query.makeScriptFieldsClause(input, "index");
        String expected = "\"script_fields\":{\"field_timestamp\":{\"script\":{\"lang\":\"painless\",\"source\":\"DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())\",\"params\":{\"field\":\"field_timestamp\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}}";
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
        Query.makeScriptFieldsClause(input, "index");
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
        Query.makeScriptFieldsClause(input, "index");
    }

    /**
     * Test various calculations of the attribute identity confidence score.
     */
    @Test
    public void testCalculateAttributeIdentityConfidenceScore() {

        // When all quality scores are 1.0,the output must be equal to the base score
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 1.00, 1.00), 0.75, 0.0000000001);

        // When any quality score is 0.0, the output must be 0.5
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 1.00, 0.00), 0.50, 0.0000000001);
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.90, 0.00), 0.50, 0.0000000001);
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.00, 0.00), 0.50, 0.0000000001);

        // The order of the quality scores must not matter
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.90, 0.80), 0.68, 0.0000000001);
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.80, 0.90), 0.68, 0.0000000001);

        // Any null quality scores must be omitted
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.90, null), 0.725, 0.0000000001);
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, null, 0.8), 0.70, 0.0000000001);
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, null, null), 0.75, 0.0000000001);

        // When the base score is null, the output must be null
        Assert.assertNull(Job.calculateAttributeIdentityConfidenceScore(null, 0.9, 0.8));
        Assert.assertNull(Job.calculateAttributeIdentityConfidenceScore(null, 0.9, null));
        Assert.assertNull(Job.calculateAttributeIdentityConfidenceScore(null, null, 0.8));
        Assert.assertNull(Job.calculateAttributeIdentityConfidenceScore(null, null, null));

        // Various tests
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.625, 0.99), 0.6546875, 0.0000000001);
        Assert.assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.87, 0.817, 0.93), 0.7811297, 0.0000000001);
    }

    /**
     * Test various calculations of the composite identity confidence score.
     */
    @Test
    public void testCalculateCompositeIdentityConfidenceScore() {

        // Inputs of 1.0 must always produce an output of 1.0
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 1.00)), 1.00000000000, 0.0000000001);

        // Inputs of 0.5 or null must not affect the output score
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.65, 0.75)), 0.87195121951, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.65, 0.75, 0.50)), 0.87195121951, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.65, 0.75, null)), 0.87195121951, 0.0000000001);

        // Inputs of 0.0 must always produce an output of 0.0
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.00)), 0.00000000000, 0.0000000001);

        // Inputs of 1.0 and 0.0 together must always produce an output of 0.5
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 1.00, 0.00)), 0.50000000000, 0.0000000001);

        // Output score must be null given an empty list of input scores.
        List<Double> scores = new ArrayList<>();
        Assert.assertNull(Job.calculateCompositeIdentityConfidenceScore(scores));

        // Output score must be null given only null input scores.
        Double nullScore = null;
        Assert.assertNull(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(nullScore, nullScore)));

        // The order of the inputs must not matter
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.75, 0.65)), 0.87195121951, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.65, 0.55, 0.75)), 0.87195121951, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.65, 0.75, 0.55)), 0.87195121951, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.65, 0.55)), 0.87195121951, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.55, 0.65)), 0.87195121951, 0.0000000001);

        // Various tests
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.95)), 0.98275862069, 0.0000000001);
        Assert.assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.85)), 0.94444444444, 0.0000000001);
    }

    /**
     * Extract attribute values from a document "_source" given the path to the index field.
     *
     * Example doc:
     *
     * {
     *   "a0": {
     *     "b0": {
     *       "c0": 0,
     *       "d0": 9
     *     }
     *   },
     *   "a1": {
     *     "b1": {
     *       "c1": [
     *         1, 2
     *       ],
     *       "d1": [
     *         9, 9
     *       ]
     *     }
     *   },
     *   "a2": {
     *     "b2": [
     *       {
     *         "c2": 3,
     *         "d2": 9
     *       },
     *       {
     *         "c2": 4,
     *         "d2": 9
     *       }
     *     ]
     *   },
     *   "a.3": {
     *     "b.3": [
     *       {
     *         "c.3": 5,
     *         "d.3": 9
     *       },
     *       {
     *         "c.3": 6,
     *         "d.3": 9
     *       }
     *     ]
     *   }
     * }
     *
     * Example paths, and the values extracted from the doc:
     *
     * Path         Values
     * -----------  ------
     * a0.b0.c0     0
     * a1.b1.c1     1, 2
     * a2.b2.c2     3, 4
     * a.3.b.3.c.3  5, 6
     *
     * @throws Exception
     */
    @Test
    public void testExtractValues() throws Exception {
        JsonNode json = Json.MAPPER.readTree("{\"a0\":{\"b0\":{\"c0\":0,\"d0\":9}},\"a1\":{\"b1\":{\"c1\":[1,2],\"d1\":[9,9]}},\"a2\":{\"b2\":[{\"c2\":3,\"d2\":9},{\"c2\":4,\"d2\":9}]},\"a.3\":{\"b.3\":[{\"c.3\":5,\"d.3\":9},{\"c.3\":6,\"d.3\":9}]}}");
        String[] path1 = Patterns.PERIOD.split("a0.b0.c0");
        String[] path2 = Patterns.PERIOD.split("a1.b1.c1");
        String[] path3 = Patterns.PERIOD.split("a2.b2.c2");
        String[] path4 = Patterns.PERIOD.split("a.3.b.3.c.3");
        ObjectNode values = Json.MAPPER.createObjectNode();
        values.put("0", 0);
        values.put("1", 1);
        values.put("2", 2);
        values.put("3", 3);
        values.put("4", 4);
        values.put("5", 5);
        values.put("6", 6);
        Assert.assertEquals(Arrays.asList(values.get("0")), Job.extractValues(json, path1, new ArrayList<>()));
        Assert.assertEquals(Arrays.asList(values.get("1"), values.get("2")), Job.extractValues(json, path2, new ArrayList<>()));
        Assert.assertEquals(Arrays.asList(values.get("3"), values.get("4")), Job.extractValues(json, path3, new ArrayList<>()));
        Assert.assertEquals(Arrays.asList(values.get("5"), values.get("6")), Job.extractValues(json, path4, new ArrayList<>()));
    }

}
