package io.zentity.resolution;

import io.zentity.model.Model;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

public class JobTest {

    @Test
    public void testMakeResolversClause() throws Exception {
        String attributes = "\"attributes\":{\"name\":{},\"street\":{},\"city\":{},\"state\":{},\"zip\":{},\"phone\":{},\"id\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"name\",\"street\",\"city\",\"state\"]},\"b\":{\"attributes\":[\"name\",\"street\",\"zip\"]},\"c\":{\"attributes\":[\"name\",\"phone\"]},\"d\":{\"attributes\":[\"id\"]}}";
        String matchers = "\"matchers\":{\"x\":{\"clause\":{\"term\":{\"{{field}}\":\"{{value}}\"}}}}";
        String indices = "\"indices\":{\"index\":{\"fields\":{\"name\":{\"attribute\":\"name\",\"matcher\":\"x\"},\"street\":{\"attribute\":\"street\",\"matcher\":\"x\"},\"city\":{\"attribute\":\"city\",\"matcher\":\"x\"},\"state\":{\"attribute\":\"state\",\"matcher\":\"x\"},\"zip\":{\"attribute\":\"zip\",\"matcher\":\"x\"},\"phone\":{\"attribute\":\"phone\",\"matcher\":\"x\"},\"id\":{\"attribute\":\"id\",\"matcher\":\"x\"}}}}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        HashMap<String, HashSet<Object>> inputAttributes = new HashMap<>();
        HashSet<Object> names = new HashSet<>();
        HashSet<Object> streets = new HashSet<>();
        HashSet<Object> cities = new HashSet<>();
        HashSet<Object> states = new HashSet<>();
        HashSet<Object> zips = new HashSet<>();
        HashSet<Object> phones = new HashSet<>();
        HashSet<Object> ids = new HashSet<>();
        names.add("Alice Jones");
        names.add("Alice Jones-Smith");
        streets.add("123 Main St");
        cities.add("Beverly Hills");
        states.add("CA");
        zips.add("90210");
        phones.add("555-123-4567");
        ids.add("1234567890");
        inputAttributes.put("name", names);
        inputAttributes.put("street", streets);
        inputAttributes.put("city", cities);
        inputAttributes.put("state", states);
        inputAttributes.put("zip", zips);
        inputAttributes.put("phone", phones);
        inputAttributes.put("id", ids);
        ArrayList<String> resolversList = new ArrayList<>();
        resolversList.add("a");
        resolversList.add("b");
        resolversList.add("c");
        resolversList.add("d");
        HashMap<String, Integer> counts = Job.countAttributesAcrossResolvers(model, resolversList);
        ArrayList<ArrayList<String>> resolversSorted = Job.sortResolverAttributes(model, resolversList, counts);
        TreeMap<String, TreeMap> resolversFilterTree = Job.makeResolversFilterTree(resolversSorted);
        String resolversClause = Job.populateResolversFilterTree(model, "index", resolversFilterTree, inputAttributes);
        String expected = "{\"bool\":{\"should\":[{\"term\":{\"id\":\"1234567890\"}},{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"term\":{\"name\":\"Alice Jones\"}},{\"term\":{\"name\":\"Alice Jones-Smith\"}}]}},{\"bool\":{\"should\":[{\"term\":{\"phone\":\"555-123-4567\"}},{\"bool\":{\"filter\":[{\"term\":{\"street\":\"123 Main St\"}},{\"bool\":{\"should\":[{\"bool\":{\"filter\":[{\"term\":{\"city\":\"Beverly Hills\"}},{\"bool\":{\"filter\":{\"term\":{\"state\":\"CA\"}}}}]}},{\"term\":{\"zip\":\"90210\"}}]}}]}}]}}]}}]}}";
        Assert.assertEquals(resolversClause, expected);
    }

}
