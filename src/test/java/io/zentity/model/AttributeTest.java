package io.zentity.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AttributeTest {

    public final static String VALID_OBJECT = "{\"type\":\"string\"}";
    private static final ObjectMapper mapper = new ObjectMapper();

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

    ////  Input Data Type Detection  ///////////////////////////////////////////////////////////////////////////////////

    private JsonNode jsonValue(String json) throws IOException {
        return mapper.readTree(json).get("value");
    }

    @Test
    public void testValidIsTypeBooleanFalse() throws Exception {
        Assert.assertTrue(Attribute.isTypeBoolean(jsonValue("{\"value\":false}")));
    }

    @Test
    public void testValidIsTypeBooleanTrue() throws Exception {
        Assert.assertTrue(Attribute.isTypeBoolean(jsonValue("{\"value\":true}")));
    }

    @Test
    public void testValidIsTypeNumberIntegerLongNegative() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":-922337203685477}")));
    }

    @Test
    public void testValidIsTypeNumberIntegerLongPositive() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":922337203685477}")));
    }

    @Test
    public void testValidIsTypeNumberIntegerShortNegative() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":-1}")));
    }

    @Test
    public void testValidIsTypeNumberIntegerShortPositive() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":1}")));
    }

    @Test
    public void testValidIsTypeNumberFloatLongNegative() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":-3.141592653589793}")));
    }

    @Test
    public void testValidIsTypeNumberFloatLongPositive() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":3.141592653589793}")));
    }

    @Test
    public void testValidIsTypeNumberFloatShortNegative() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":-1.0}")));
    }

    @Test
    public void testValidIsTypeNumberFloatShortPositive() throws Exception {
        Assert.assertTrue(Attribute.isTypeNumber(jsonValue("{\"value\":1.0}")));
    }

    @Test
    public void testValidIsTypeString() throws Exception {
        Assert.assertTrue(Attribute.isTypeString(jsonValue("{\"value\":\"a\"}")));
    }

    ////  Booleans must not be strings or numbers

    @Test
    public void testInvalidIsTypeBooleanFalseInteger() throws Exception {
        Assert.assertFalse(Attribute.isTypeBoolean(jsonValue("{\"value\":0}")));
    }

    @Test
    public void testInvalidIsTypeBooleanFalseString() throws Exception {
        Assert.assertFalse(Attribute.isTypeBoolean(jsonValue("{\"value\":\"false\"}")));
    }

    @Test
    public void testInvalidIsTypeBooleanTrueInteger() throws Exception {
        Assert.assertFalse(Attribute.isTypeBoolean(jsonValue("{\"value\":1}")));
    }

    @Test
    public void testInvalidIsTypeBooleanTrueString() throws Exception {
        Assert.assertFalse(Attribute.isTypeBoolean(jsonValue("{\"value\":\"true\"}")));
    }

    ////  Validate Nullable Data Types  ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidValidateTypeBooleanNullable() throws Exception {
        Attribute.validateTypeBoolean(jsonValue("{\"value\":null}"));
        Assert.assertTrue(true);
    }

    @Test
    public void testValidValidateTypeNumberNullable() throws Exception {
        Attribute.validateTypeNumber(jsonValue("{\"value\":null}"));
        Assert.assertTrue(true);
    }

    @Test
    public void testValidValidateTypeStringNullable() throws Exception {
        Attribute.validateTypeString(jsonValue("{\"value\":null}"));
        Assert.assertTrue(true);
    }

    ////  Input Data Type Conversion  //////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidConvertTypeBooleanFalse() throws Exception {
        Assert.assertEquals(Attribute.convertTypeBoolean(jsonValue("{\"value\":false}")).toString(), "false");
    }

    @Test
    public void testValidConvertTypeBooleanTrue() throws Exception {
        Assert.assertEquals(Attribute.convertTypeBoolean(jsonValue("{\"value\":true}")).toString(), "true");
    }

    @Test
    public void testValidConvertTypeNumberIntegerLongNegative() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":-922337203685477}")).toString(), "-922337203685477");
    }

    @Test
    public void testValidConvertTypeNumberIntegerLongPositive() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":922337203685477}")).toString(), "922337203685477");
    }

    @Test
    public void testValidConvertTypeNumberIntegerShortNegative() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":-1}")).toString(), "-1");
    }

    @Test
    public void testValidConvertTypeNumberIntegerShortPositive() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":1}")).toString(), "1");
    }

    @Test
    public void testValidConvertTypeNumberFloatLongNegative() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":-3.141592653589793}")).toString(), "-3.141592653589793");
    }

    @Test
    public void testValidConvertTypeNumberFloatLongPositive() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":3.141592653589793}")).toString(), "3.141592653589793");
    }

    @Test
    public void testValidConvertTypeNumberFloatShortNegative() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":-1.0}")).toString(), "-1.0");
    }

    @Test
    public void testValidConvertTypeNumberFloatShortPositive() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":1.0}")).toString(), "1.0");
    }

    @Test
    public void testValidConvertTypeString() throws Exception {
        Assert.assertEquals(Attribute.convertTypeString(jsonValue("{\"value\":\"a\"}")), "a");
    }


    ////  Nullable Input Data Type Conversion  /////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidConvertTypeBooleanNullable() throws Exception {
        Assert.assertEquals(Attribute.convertTypeBoolean(jsonValue("{\"value\":null}")), null);
    }

    @Test
    public void testValidConvertTypeNumberNullable() throws Exception {
        Assert.assertEquals(Attribute.convertTypeNumber(jsonValue("{\"value\":null}")), null);
    }

    @Test
    public void testValidConvertTypeStringNullable() throws Exception {
        Assert.assertEquals(Attribute.convertTypeString(jsonValue("{\"value\":null}")), null);
    }

}
