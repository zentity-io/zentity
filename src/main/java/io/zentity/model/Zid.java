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
package io.zentity.model;

import java.util.Arrays;
import java.util.Base64;
import java.util.Set;
import java.util.TreeSet;

public class Zid {

    public static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder();
    public static final Set<String> VALID_RELATION_DIRECTIONS = new TreeSet<>(
            Arrays.asList("a>b", "a<b", "a<>b", "")
    );

    /**
     * Encode an entity _zid.
     *
     * @param entityType           The entity type.
     * @param indexName            The index name in which the entity was first observed during resolution.
     * @param docId                The doc _id in which the entity was first observed during resolution.
     * @param entityTypeOccurrence The occurrence in which the entity type appeared in the doc.
     * @return Serialized _zid for the entity.
     */
    public static String encodeEntity(String entityType, String indexName, String docId, Integer entityTypeOccurrence) {
        return String.join("|",
                entityType,
                indexName,
                BASE64_ENCODER.encodeToString(docId.getBytes()),
                entityTypeOccurrence.toString()
        );
    }

    /**
     * Encode a relation _zid.
     * Normalizes the value by ensuring that entities A and B appear in lexicographical order.
     *
     * @param relationType      The relation type.
     * @param relationDirection The relation direction ("a>b", "a<b", "a<>b", "", or null)
     * @param entityZidA        The _zid of entity A.
     * @param entityZidB        The _zid if entity B.
     * @return Serialized _zid for the relation.
     * @throws ValidationException
     */
    public static String encodeRelation(String relationType, String relationDirection, String entityZidA, String entityZidB) throws ValidationException {
        relationType = relationType == null ? "" : relationType;
        relationDirection = relationDirection == null ? "" : relationDirection;
        if (!VALID_RELATION_DIRECTIONS.contains(relationDirection))
            throw new ValidationException("'" + relationDirection + "' is not a valid relation direction.");

        // If entity A < entity B, return the value.
        if (entityZidA.compareTo(entityZidB) <= 0)
            return String.join("#", relationType, relationDirection, entityZidA, entityZidB);

        // If entity A > entity B, reverse their positions and reverse the direction.
        if (relationDirection.equals("a>b"))
            relationDirection = "a<b";
        else if (relationDirection.equals("a<b"))
            relationDirection = "a>b";
        return String.join("#", relationType, relationDirection, entityZidB, entityZidA);
    }
}
