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
import org.junit.Assert;
import org.junit.Test;

public class ModelTest {

    public final static String VALID_OBJECT = "{\n" +
            "  \"index\": \"foo\",\n" +
            "  \"type\": \"residence\",\n" +
            "  \"direction\": \"a<>b\",\n" +
            "  \"a\": \"person\",\n" +
            "  \"b\": \"address\"\n" +
            "}";

    ////  model  ///////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        Model model = new Model(VALID_OBJECT);
        Assert.assertEquals(model.index(), "foo");
        Assert.assertEquals(model.type(), "residence");
        Assert.assertEquals(model.direction(), "a<>b");
        Assert.assertEquals(model.a(), "person");
        Assert.assertEquals(model.b(), "address");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUnexpectedField() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\",\n" +
                "  \"foo\": \"bar\"\n" +
                "}");
    }

    ////  model."index"  ///////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidIndexEmpty() throws Exception {
        new Model("{\n" +
                "  \"index\": \"\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexEmptyWhitespace() throws Exception {
        new Model("{\n" +
                "  \"index\": \" \",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexOmitted() throws Exception {
        new Model("{\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeArray() throws Exception {
        new Model("{\n" +
                "  \"index\": [\"foo\"],\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"index\": true,\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"index\": 1.0,\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"index\": 1,\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeNull() throws Exception {
        new Model("{\n" +
                "  \"index\": null,\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeObject() throws Exception {
        new Model("{\n" +
                "  \"index\": {},\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    ////  model."type"  ///////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidTypeEmpty() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeEmptyWhitespace() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \" \",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeOmmitted() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeArray() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": [\"residence\"],\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": true,\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": 1.0,\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": 1,\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeNull() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": null,\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeObject() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": {},\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    ////  model."direction"  ///////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidDirectionEmpty() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testValidDirectionOmitted() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testValidDirectionAtoB() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testValidDirectionBtoA() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testValidDirectionBidirectional() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testValidDirectionPermissiveInputs() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a->b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a->b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<-b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \" a < > b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \" a < -- > b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"A<>B\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testValidDirectionNonPermissiveInput() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a->bb\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testInvalidDirectionEmptyWhitespace() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \" \",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDirectionTypeArray() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": [\"a<>b\"],\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDirectionTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": true,\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDirectionTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": 1.0,\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDirectionTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": 1,\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test
    public void testValidDirectionTypeNull() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": null,\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDirectionTypeObject() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": {},\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    ////  model."a"  ///////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidAEmpty() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAEmptyWhitespace() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \" \",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAOmitted() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidATypeArray() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": [\"person\"],\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidATypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": true,\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidATypeFloat() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": 1.0,\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidATypeInteger() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": 1,\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidATypeNull() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": null,\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidATypeObject() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": {},\n" +
                "  \"b\": \"address\"\n" +
                "}");
    }

    ////  model."b"  ///////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidBEmpty() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \"\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBEmptyWhitespace() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": \" \"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBOmmitted() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBTypeArray() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": [\"address\"]\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": true\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": 1.0\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": 1\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBTypeNull() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": null\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidBTypeObject() throws Exception {
        new Model("{\n" +
                "  \"index\": \"foo\",\n" +
                "  \"type\": \"residence\",\n" +
                "  \"direction\": \"a<>b\",\n" +
                "  \"a\": \"person\",\n" +
                "  \"b\": {}\n" +
                "}");
    }
}
