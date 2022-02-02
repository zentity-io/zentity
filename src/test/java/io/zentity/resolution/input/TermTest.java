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
package io.zentity.resolution.input;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.value.Value;
import org.junit.Assert;
import org.junit.Test;

public class TermTest {

    ////  "terms".TERM  ////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidTypeEmptyString() throws Exception {
        new Term("");
    }
    @Test(expected = ValidationException.class)
    public void testInvalidTypeEmptyStringWhitespace() throws Exception {
        new Term(" ");
    }

    ////  Value type detection  ////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidTypeBooleanFalse() throws Exception {
        Term term = new Term("false");
        Assert.assertTrue(term.isBoolean());
        Assert.assertFalse(term.isNumber());
    }

    @Test
    public void testValidTypeBooleanTrue() throws Exception {
        Term term = new Term("true");
        Assert.assertTrue(term.isBoolean());
        Assert.assertFalse(term.isNumber());
    }

    @Test
    public void testValidTypeDate() throws Exception {
        Term term = new Term("2019-12-31 12:45:00");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isDate("yyyy-MM-dd HH:mm:ss"));
        Assert.assertFalse(term.isNumber());
    }

    @Test
    public void testInvalidTypeDate() throws Exception {
        Term term = new Term("2019-12-31 12:45:00");
        Assert.assertFalse(term.isDate("yyyyMMdd"));
    }

    @Test
    public void testValidTypeNumberIntegerLongNegative() throws Exception {
        Term term = new Term("-922337203685477");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberIntegerLongPositive() throws Exception {
        Term term = new Term("922337203685477");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberIntegerShortNegative() throws Exception {
        Term term = new Term("-1");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberIntegerShortPositive() throws Exception {
        Term term = new Term("1");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberFloatLongNegative() throws Exception {
        Term term = new Term("-3.141592653589793");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberFloatLongPositive() throws Exception {
        Term term = new Term("3.141592653589793");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberFloatShortNegative() throws Exception {
        Term term = new Term("-1.0");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    @Test
    public void testValidTypeNumberFloatShortPositive() throws Exception {
        Term term = new Term("1.0");
        Assert.assertFalse(term.isBoolean());
        Assert.assertTrue(term.isNumber());
    }

    ////  Value conversion  ////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValueConversionBooleanFalse() throws Exception {
        Term term = new Term("false");
        JsonNode value = Json.MAPPER.readTree("{\"value\":false}").get("value");
        Assert.assertEquals(term.booleanValue(), Value.create("boolean", value));
    }

    @Test
    public void testValueConversionBooleanTrue() throws Exception {
        Term term = new Term("true");
        JsonNode value = Json.MAPPER.readTree("{\"value\":true}").get("value");
        Assert.assertEquals(term.booleanValue(), Value.create("boolean", value));
    }

    @Test
    public void testValueConversionDate() throws Exception {
        Term term = new Term("2019-12-31 12:45:00");
        JsonNode value = Json.MAPPER.readTree("{\"value\":\"2019-12-31 12:45:00\"}").get("value");
        Assert.assertEquals(term.dateValue(), Value.create("date", value));
    }

    @Test
    public void testValueConversionNumberIntegerLongNegative() throws Exception {
        Term term = new Term("-922337203685477");
        JsonNode value = Json.MAPPER.readTree("{\"value\":-922337203685477}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberIntegerLongPositive() throws Exception {
        Term term = new Term("922337203685477");
        JsonNode value = Json.MAPPER.readTree("{\"value\":922337203685477}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberIntegerShortNegative() throws Exception {
        Term term = new Term("-1");
        JsonNode value = Json.MAPPER.readTree("{\"value\":-1}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberIntegerShortPositive() throws Exception {
        Term term = new Term("1");
        JsonNode value = Json.MAPPER.readTree("{\"value\":1}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberFloatLongNegative() throws Exception {
        Term term = new Term("-3.141592653589793");
        JsonNode value = Json.MAPPER.readTree("{\"value\":-3.141592653589793}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberFloatLongPositive() throws Exception {
        Term term = new Term("3.141592653589793");
        JsonNode value = Json.MAPPER.readTree("{\"value\":3.141592653589793}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberFloatShortNegative() throws Exception {
        Term term = new Term("-1.0");
        JsonNode value = Json.MAPPER.readTree("{\"value\":-1.0}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionNumberFloatShortPositive() throws Exception {
        Term term = new Term("1.0");
        JsonNode value = Json.MAPPER.readTree("{\"value\":1.0}").get("value");
        Assert.assertEquals(term.numberValue(), Value.create("number", value));
    }

    @Test
    public void testValueConversionString() throws Exception {
        Term term = new Term("abc");
        JsonNode value = Json.MAPPER.readTree("{\"value\":\"abc\"}").get("value");
        Assert.assertEquals(term.stringValue(), Value.create("string", value));
    }
}
