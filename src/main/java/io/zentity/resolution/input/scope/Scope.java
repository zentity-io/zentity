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
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Scope {

    private Exclude exclude = new Exclude();
    private Include include = new Include();

    public Scope() {
    }

    public Exclude exclude() {
        return this.exclude;
    }

    public Include include() {
        return this.include;
    }

    public void deserialize(JsonNode json, Model model) throws ValidationException, IOException {
        if (!json.isNull() && !json.isObject())
            throw new ValidationException("The 'scope' field of the request body must be an object.");

        // Parse and validate the "scope.exclude" and "scope.include" fields of the request body.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            switch (name) {
                case "exclude":
                    this.exclude.deserialize(json.get("exclude"), model);
                    break;
                case "include":
                    this.include.deserialize(json.get("include"), model);
                    break;
                default:
                    throw new ValidationException("'scope." + name + "' is not a recognized field.");
            }
        }

    }

    public void deserialize(String json, Model model) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json), model);
    }
}

