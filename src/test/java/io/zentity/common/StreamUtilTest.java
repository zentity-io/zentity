package io.zentity.common;

import org.elasticsearch.common.collect.Tuple;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class StreamUtilTest {
    @Test
    public void testTupleFlatmapper() {
        Stream<String> stream = Stream.of("0a", "0b", "1a", "1b", "2a", "2b");

        List<Tuple<String, String>> tuples = stream
            .flatMap(StreamUtil.tupleFlatmapper())
            .collect(Collectors.toList());

        assertEquals(3, tuples.size());

        for (int i = 0; i < tuples.size(); i++) {
            Tuple<String, String> tup = tuples.get(i);
            assertEquals(i + "a", tup.v1());
            assertEquals(i + "b", tup.v2());
        }
    }
}
