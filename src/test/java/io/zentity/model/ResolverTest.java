package io.zentity.model;

import org.junit.Test;

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
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidWeightTypeNull() throws Exception {
        new Resolver("resolver_name", "{\"attributes\":[\"attribute_a\"],\"weight\":null}");
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
