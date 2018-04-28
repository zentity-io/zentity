package io.zentity.resolution;

import io.zentity.model.Model;
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

}
