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

import io.zentity.model.ValidationException;
import org.junit.Test;

public class ModelTest {

    public final static String VALID_OBJECT = "{\n" +
            "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + ",\"attribute_array\":" + AttributeTest.VALID_OBJECT + ",\"attribute_object\":" + AttributeTest.VALID_OBJECT + "},\n" +
            "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + ",\"resolver_name_b\":" + ResolverTest.VALID_OBJECT + ",\"resolver_name_c\":" + ResolverTest.VALID_OBJECT + "},\n" +
            "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
            "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + ",\"index_name_b\":" + IndexTest.VALID_OBJECT + ",\"index_name_c\":" + IndexTest.VALID_OBJECT + "}\n" +
            "}";

    ////  model  ///////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        new Model(VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUnexpectedField() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "},\n" +
                "  \"foo\": \"bar\"\n" +
                "}");
    }

    ////  model."attributes"  //////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidAttributesEmpty() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test
    public void testValidAttributeNesting() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"a.b\":{},\"a.c\":{},\"d\":{}},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
        new Model("{\n" +
                "  \"attributes\":{\"a.b\":{},\"a.c\":{},\"a.d.a\":{},\"a.d.b\":{},\"a.d.c\":{}},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesEmptyRunnable() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}", true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesMissing() throws Exception {
        new Model("{\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":[],\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":true,\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":1.0,\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":1,\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":null,\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":\"foobar\",\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":[]},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":true},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":1.0},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":1},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":null},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":\"foobar\"},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeNestingConflict() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":{},\"attribute_name.nested\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    ////  model."resolvers"  ///////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidResolversEmpty() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversEmptyRunnable() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}", true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversMissing() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":[],\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":true,\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":1.0,\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":1,\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":null,\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolversTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":\"foobar\",\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolverTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":[]},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolverTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":true},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolverTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":1.0},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolverTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":1},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolverTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":null},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidResolverTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":\"foobar\"},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    ////  model."matchers"  ////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidMatchersEmpty() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersEmptyRunnable() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}", true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersMissing() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":[],\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":true,\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":1.0,\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":1,\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":null,\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatchersTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":\"foobar\",\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatcherTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":[]},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatcherTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":true},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatcherTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":1.0},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatcherTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":1},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatcherTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":null},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidMatcherTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":\"foobar\"},\n" +
                "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
                "}");
    }

    ////  model."indices"  /////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidIndicesEmpty() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesEmptyRunnable() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{}\n" +
                "}", true);
    }

    @Test
    public void testValidIndexEmpty() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\": {\"index_name\":{\"fields\":{}}}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexEmptyRunnable() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\": {\"index_name\":{\"fields\":{}}}\n" +
                "}", true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexFieldEmpty() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\": {\"index_name\":{\"fields\":{\"index_field_name\":{}}}}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesMissing() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":[]\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":true\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":1.0\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":1\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":null\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndicesTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":\"foobar\"\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeArray() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":[]}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeBoolean() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":true}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeFloat() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":1.0}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeInteger() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":1}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeNull() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":null}\n" +
                "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIndexTypeString() throws Exception {
        new Model("{\n" +
                "  \"attributes\":{\"attribute_name\":" + AttributeTest.VALID_OBJECT + "},\n" +
                "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
                "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
                "  \"indices\":{\"index_name_a\":\"foobar\"}\n" +
                "}");
    }
}
