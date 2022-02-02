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

public abstract class Value implements ValueInterface {

    protected final String type = "value";
    protected final JsonNode value;
    protected final String serialized;

    /**
     * Validate and hold the object of a value.
     *
     * @param value Attribute value.
     */
    Value(JsonNode value) throws ValidationException {
        this.validate(value);
        this.value = value.isNull() ? null : value;
        this.serialized = this.serialize(value);
    }

    /**
     * Factory method to construct a Value.
     *
     * @param attributeType Attribute type.
     * @param value         Attribute value.
     * @return
     * @throws ValidationException
     */
    public static Value create(String attributeType, JsonNode value) throws ValidationException {
        switch (attributeType) {
            case "boolean":
                return new BooleanValue(value);
            case "date":
                return new DateValue(value);
            case "number":
                return new NumberValue(value);
            case "string":
                return new StringValue(value);
            default:
                throw new ValidationException("'" + attributeType + " is not a recognized attribute type.");
        }
    }

    @Override
    public abstract String serialize(JsonNode value);

    @Override
    public abstract void validate(JsonNode value) throws ValidationException;

    @Override
    public Object type() {
        return this.type;
    }

    @Override
    public JsonNode value() {
        return this.value;
    }

    @Override
    public String serialized() {
        return this.serialized;
    }

    @Override
    public int compareTo(Value o) {
        return this.serialized.compareTo(o.serialized);
    }

    @Override
    public String toString() {
        return this.serialized;
    }

    @Override
    public boolean equals(Object o) { return this.hashCode() == o.hashCode(); }

    @Override
    public int hashCode() { return this.serialized.hashCode(); }

}
