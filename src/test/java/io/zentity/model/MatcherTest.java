package io.zentity.model;

import org.junit.Test;

public class MatcherTest {

    public final static String VALID_OBJECT = "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}}}";

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

}
