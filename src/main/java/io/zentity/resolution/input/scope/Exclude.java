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
package io.zentity.resolution.input.scope;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Exclude extends ScopeField {

    public Exclude() {
        super();
    }

    @Override
    public void deserialize(JsonNode json, Model model) throws ValidationException, IOException {
        if (!json.isNull() && !json.isObject())
            throw new ValidationException("The 'scope.exclude' field of the request body must be an object.");

        // Parse and validate the "scope.exclude" fields of the request body.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            switch (name) {
                case "attributes":
                    this.attributes = parseAttributes("exclude", model, json.get("attributes"));
                    break;
                case "resolvers":
                    this.resolvers = parseResolvers("exclude", json.get("resolvers"));
                    break;
                case "indices":
                    this.indices = parseIndices("exclude", json.get("indices"));
                    break;
                default:
                    throw new ValidationException("'scope.exclude." + name + "' is not a recognized field.");
            }
        }
    }

}
