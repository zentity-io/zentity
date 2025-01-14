/*
 * zentity
 * Copyright © 2018-2025 Dave Moore
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
package io.zentity.common;

import org.elasticsearch.core.Tuple;
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
