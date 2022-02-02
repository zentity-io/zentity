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
package io.zentity.model;

import org.junit.Test;

import java.util.Collections;

public class ResolverTest {

    public final static String VALID_OBJECT = "{\"attributes\":[\"attribute_a\"]}";

    ////  "resolvers"  /////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        new Resolver("resolver_name", VALID_OBJECT);
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",\"attribute_b\"]}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":1}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":0}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":-1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUnexpectedField() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"foo\":\"bar\"}");
    }

    ////  "resolvers".RESOLVER_NAME  ///////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidNameEmpty() throws Exception {
        new Resolver(" ", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameContainsAsterisk() throws Exception {
        new Resolver("selectivemploymentax*", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameContainsHash() throws Exception {
        new Resolver("c#ke", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameContainsColon() throws Exception {
        new Resolver("p:psi", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsWithUnderscore() throws Exception {
        new Resolver("_fanta", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsWithDash() throws Exception {
        new Resolver("-fanta", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsWithPlus() throws Exception {
        new Resolver("+fanta", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsTooLong() throws Exception {
        new Resolver(String.join("", Collections.nCopies(100, "sprite")), VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameIsDot() throws Exception {
        new Resolver(".", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameIsDotDot() throws Exception {
        new Resolver("..", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameIsNotLowercase() throws Exception {
        new Resolver("MELLO_yello", VALID_OBJECT);
    }

    @Test
    public void testValidNames() throws Exception {
        new Resolver("hello", VALID_OBJECT);
        new Resolver(".hello", VALID_OBJECT);
        new Resolver("..hello", VALID_OBJECT);
        new Resolver("hello.world", VALID_OBJECT);
        new Resolver("hello_world", VALID_OBJECT);
        new Resolver("hello-world", VALID_OBJECT);
        new Resolver("hello+world", VALID_OBJECT);
        new Resolver("您好", VALID_OBJECT);
    }

    ////  "resolvers".RESOLVER_NAME."attributes"  //////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesMissing() throws Exception {
        new Resolver("resolver_name", "{}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesEmpty() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeBoolean() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeFloat() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeInteger() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeNull() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":null}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeObject() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":{}}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeString() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":\"foobar\"}");
    }

    ////  "resolvers".RESOLVER_NAME."attributes".ATTRIBUTE_NAME  ///////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameEmpty() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",\" \"]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameTypeArray() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",[]]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameTypeBoolean() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",true]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameTypeFloat() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",1.0]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameTypeInteger() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",1]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameTypeNull() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",null]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesNameTypeObject() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\",{}]}");
    }

    ////  "resolvers".RESOLVER_NAME."weight"  //////////////////////////////////////////////////////////////////////

    @Test
    public void testValidWeightValue() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":0}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":1}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":2}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":-1}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":-2}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":0.0}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":1.0}");
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":null}");
    }

    /**
     * Valid because the "weight" field is optional.
     */
    @Test
    public void testValidWeightMissing() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"]}");
    }

    /**
     * Valid because the "weight" field is optional.
     */
    @Test
    public void testValidWeightTypeNull() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":null}");
    }

    @Test
    public void testValidWeightTypeFloatZeroDecimal() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":10.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidWeightTypeArray() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidWeightTypeBoolean() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidWeightTypeFloat() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":1.1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidWeightTypeObject() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":{}}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidWeightTypeString() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":\"1\"}");
    }

}
