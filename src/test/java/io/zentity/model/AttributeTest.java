package io.zentity.model;

import org.junit.Assert;
import org.junit.Test;

public class AttributeTest {

    public final static String VALID_OBJECT = "{\"type\":\"string\"}";

    ////  "attributes"  ////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        new Attribute("attribute_name", VALID_OBJECT);
        new Attribute("attribute_name", "{}");
        Assert.assertTrue(true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUnexpectedField() throws Exception {
        new Attribute("attribute_name", "{\"type\":\"string\",\"foo\":\"bar\"}");
    }

    ////  "attributes".ATTRIBUTE_NAME  /////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidNameEmpty() throws Exception {
        new Attribute(" ", VALID_OBJECT);
    }

    ////  "attributes".ATTRIBUTE_NAME."type"  //////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidTypeValue() throws Exception {
        for (String value : Attribute.VALID_TYPES)
            new Attribute("attribute_name", "{\"type\":\"" + value + "\"}");
        Assert.assertTrue(true);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeValue() throws Exception {
        new Attribute("attribute_name", "{\"type\":\"foobar\"}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeArray() throws Exception {
        new Attribute("attribute_name", "{\"type\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeBoolean() throws Exception {
        new Attribute("attribute_name", "{\"type\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeFloat() throws Exception {
        new Attribute("attribute_name", "{\"type\":1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeInteger() throws Exception {
        new Attribute("attribute_name", "{\"type\":1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeNull() throws Exception {
        new Attribute("attribute_name", "{\"type\":null}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeObject() throws Exception {
        new Attribute("attribute_name", "{\"type\":{}}");
    }

}
