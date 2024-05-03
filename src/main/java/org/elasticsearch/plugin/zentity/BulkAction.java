/*
 * zentity
 * Copyright © 2018-2024 Dave Moore
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
package org.elasticsearch.plugin.zentity;

import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.common.StreamUtil;
import joptsimple.internal.Strings;
import org.elasticsearch.core.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BulkAction {

    public static final int MAX_CONCURRENT_OPERATIONS_PER_REQUEST = 100;

    /**
     * Split an NDJSON-formatted string into pairs of params and payloads.
     *
     * @param body NDJSON-formatted string.
     * @return
     */
    static List<Tuple<String, String>> splitBulkEntries(String body) {
        String[] lines = Patterns.NEWLINE.split(body);
        if (lines.length % 2 != 0)
            throw new BadRequestException("Bulk request must have repeating pairs of params and payloads on separate lines.");
        return Arrays.stream(lines)
            .flatMap(StreamUtil.tupleFlatmapper())
            .collect(Collectors.toList());
    }

    /**
     * Serialize the response of a bulk request.
     *
     * @param result The result of a bulk request.
     * @return
     */
    static String bulkResultToJson(BulkResult result) {
        return "{" +
            Json.quoteString("took") + ":" + result.took +
            "," + Json.quoteString("errors") + ":" + result.errors +
            "," + Json.quoteString("items") + ":" + "[" + Strings.join(result.items, ",") + "]" +
            "}";
    }

    /**
     * Small wrapper around a single response for a bulk request.
     */
    static final class SingleResult {
        final String response;
        final boolean failed;

        SingleResult(String response, boolean failed) {
            this.response = response;
            this.failed = failed;
        }
    }

    /**
     * A wrapper for a collection of single responses for a bulk request.
     */
    static final class BulkResult {
        final List<String> items;
        final boolean errors;
        final long took;

        BulkResult(List<String> items, boolean errors, long took) {
            this.items = items;
            this.errors = errors;
            this.took = took;
        }
    }
}
