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

public class StringValue extends Value {

    public final String type = "string";

    public StringValue(JsonNode value) throws ValidationException {
        super(value);
    }

    /**
     * Serialize the attribute value from a JsonNode object to a String object.
     *
     * @return
     */
    @Override
    public String serialize(JsonNode value) {
        if (value.isNull())
            return "null";
        return value.textValue();
    }

    /**
     * Validate the value.
     *
     * @param value Attribute value.
     * @throws ValidationException
     */
    @Override
    public void validate(JsonNode value) throws ValidationException {
        if (!value.isTextual() && !value.isNull())
            throw new ValidationException("Expected '" + this.type + "' attribute data type.");
    }
}
