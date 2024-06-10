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
package io.zentity.model.relation;

import io.zentity.model.ValidationException;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class Zid {

    public static final Set<String> VALID_RELATION_DIRECTIONS = new TreeSet<>(
            Arrays.asList("a>b", "a<b", "a<>b", "")
    );

    /**
     * Encode a relation _zid.
     * Normalizes the value by ensuring that entities A and B appear in lexicographical order.
     *
     * @param type       The relation type.
     * @param direction  The relation direction ("a>b", "a<b", "a<>b", "", or null)
     * @param entityZidA The _zid of entity A.
     * @param entityZidB The _zid if entity B.
     * @return Serialized _zid for the relation.
     * @throws ValidationException
     */
    public static String encode(String type, String direction, String entityZidA, String entityZidB) throws ValidationException {
        type = type == null ? "" : type;
        direction = direction == null ? "" : direction;
        if (!VALID_RELATION_DIRECTIONS.contains(direction))
            throw new ValidationException("'" + direction + "' is not a valid relation direction.");

        // If entity A < entity B, return the value.
        if (entityZidA.compareTo(entityZidB) <= 0)
            return String.join("#", type, direction, entityZidA, entityZidB);

        // If entity A > entity B, reverse their positions and reverse the direction.
        if (direction.equals("a>b"))
            direction = "a<b";
        else if (direction.equals("a<b"))
            direction = "a>b";
        return String.join("#", type, direction, entityZidB, entityZidA);
    }
}
