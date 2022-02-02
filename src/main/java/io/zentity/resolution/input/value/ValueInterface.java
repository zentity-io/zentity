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
package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

public interface ValueInterface extends Comparable<Value> {

    /**
     * Validate the attribute value. Throw an exception on validation error. Pass on success.
     *
     * @param value Attribute value.
     */
    void validate(JsonNode value) throws ValidationException;

    /**
     * Serialize the attribute value from a JsonNode object to a String object.
     */
    String serialize(JsonNode value);

    /**
     * Return the attribute type.
     *
     * @return
     */
    Object type();

    /**
     * Return the attribute value.
     *
     * @return
     */
    Object value();

    /**
     * Return the serialized attribute value.
     *
     * @return
     */
    String serialized();

}
