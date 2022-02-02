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

public class IndexTest {

    public final static String VALID_OBJECT = "{\"fields\":{\"index_field_name\":" + IndexFieldTest.VALID_OBJECT + "}}";

    ////  "indices"  ///////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        new Index("index_name", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUnexpectedField() throws Exception {
        new Index("index_name", "{\"fields\":{\"index_field_name\":" + IndexFieldTest.VALID_OBJECT + "},\"foo\":\"bar\"}");
    }

    ////  "indices".INDEX_NAME  ////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidNameEmpty() throws Exception {
        new Index(" ", VALID_OBJECT);
    }

    ////  "indices".INDEX_NAME."fields"  ///////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsMissing() throws Exception {
        new Index("index_name", "{}");
    }

    @Test
    public void testValidFieldsEmpty() throws Exception {
        new Index("index_name", "{\"fields\":{}}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsEmptyRunnable() throws Exception {
        new Index("index_name", "{\"fields\":{}}", true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsTypeArray() throws Exception {
        new Index("index_name", "{\"fields\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsTypeBoolean() throws Exception {
        new Index("index_name", "{\"fields\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsTypeFloat() throws Exception {
        new Index("index_name", "{\"fields\":1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsTypeInteger() throws Exception {
        new Index("index_name", "{\"fields\":1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsTypeNull() throws Exception {
        new Index("index_name", "{\"fields\":null}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsTypeString() throws Exception {
        new Index("index_name", "{\"fields\":\"foobar\"}");
    }
}