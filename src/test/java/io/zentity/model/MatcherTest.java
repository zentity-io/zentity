package io.zentity.model;

import org.junit.Test;

public class MatcherTest {

    public final static String VALID_OBJECT = "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":\"value\"}";

    ////  "matchers"  //////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        new Matcher("matcher_name", VALID_OBJECT);
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}}}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUnexpectedField() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"foo\":\"bar\"}");
    }

    ////  "matchers".MATCHER_NAME  /////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidNameEmpty() throws Exception {
        new Matcher(" ", VALID_OBJECT);
    }

    ////  "matchers".MATCHER_NAME."clause"  ////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidClauseEmpty() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{}}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidClauseTypeArray() throws Exception {
        new Matcher("matcher_name", "{\"clause\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidClauseTypeBoolean() throws Exception {
        new Matcher("matcher_name", "{\"clause\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidClauseTypeFloat() throws Exception {
        new Matcher("matcher_name", "{\"clause\":1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidClauseTypeInteger() throws Exception {
        new Matcher("matcher_name", "{\"clause\":1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidClauseTypeNull() throws Exception {
        new Matcher("matcher_name", "{\"clause\":null}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidClauseTypeString() throws Exception {
        new Matcher("matcher_name", "{\"clause\":\"foobar\"}");
    }

    ////  "resolvers".RESOLVER_NAME."attributes".ATTRIBUTE_NAME  ///////////////////////////////////////////////////////

    @Test
    public void testValidTypeValue() throws Exception {
        for (String value : Matcher.VALID_TYPES)
            new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":\"" + value + "\"}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeValue() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":\"foobar\"}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeArray() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeBoolean() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeFloat() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeInteger() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeNull() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":null}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTypeTypeObject() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"type\":{}}");
    }

}
