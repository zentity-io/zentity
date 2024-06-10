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
package io.zentity.model.entity;

import java.util.Base64;

public class Zid {

    public static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder();

    /**
     * Encode an entity _zid.
     *
     * @param type       The entity type.
     * @param indexName  The index name in which the entity was first observed during resolution.
     * @param docId      The doc _id in which the entity was first observed during resolution.
     * @param occurrence The occurrence in which the entity type appeared in the doc.
     * @return Serialized _zid for the entity.
     */
    public static String encode(String type, String indexName, String docId, Integer occurrence) {
        return String.join("|",
                type,
                indexName,
                BASE64_ENCODER.encodeToString(docId.getBytes()),
                occurrence.toString()
        );
    }
}
