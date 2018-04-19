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

    @Test(expected = ValidationException.class)
    public void testInvalidFieldsEmpty() throws Exception {
        new Index("index_name", "{\"fields\":{}}");
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