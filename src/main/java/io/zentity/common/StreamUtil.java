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
package io.zentity.common;

import org.elasticsearch.core.Tuple;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

public class StreamUtil {
    /**
     * Constructs a stateful function to be used in {@link Stream#flatMap} that buffers items into {@link Tuple Tuples}
     * of a single type. This is not re-usable between streams, as it is stateful. Parallel streams also might be
     * prone to issues with ordering.
     *
     * @param <T> The type of each item.
     * @return A function for flat mapping a single stream.
     */
    public static <T> Function<T, Stream<Tuple<T, T>>> tupleFlatmapper() {
        final AtomicLong idxCounter = new AtomicLong(0);

        AtomicReference<T> v1 = new AtomicReference<>();

        return (T item) -> {
            int index = (int) (idxCounter.getAndIncrement() % 2);

            if (index == 0) {
                v1.set(item);
                return Stream.empty();
            }

            return Stream.of(Tuple.tuple(v1.get(), item));
        };
    }
}
