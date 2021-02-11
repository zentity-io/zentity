package io.zentity.model;

import org.junit.Test;

import java.util.Collections;

public class MatcherTest {

    public final static String VALID_OBJECT = "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}}}";

    ////  "matchers"  //////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValid() throws Exception {
        new Matcher("matcher_name", VALID_OBJECT);
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":0.5}");
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

    @Test(expected = ValidationException.class)
    public void testInvalidNameContainsAsterisk() throws Exception {
        new Matcher("selectivemploymentax*", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameContainsHash() throws Exception {
        new Matcher("c#ke", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameContainsColon() throws Exception {
        new Matcher("p:psi", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsWithUnderscore() throws Exception {
        new Matcher("_fanta", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsWithDash() throws Exception {
        new Matcher("-fanta", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsWithPlus() throws Exception {
        new Matcher("+fanta", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameStartsTooLong() throws Exception {
        new Matcher(String.join("", Collections.nCopies(100, "sprite")), VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameIsDot() throws Exception {
        new Matcher(".", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameIsDotDot() throws Exception {
        new Matcher("..", VALID_OBJECT);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidNameIsNotLowercase() throws Exception {
        new Matcher("MELLO_yello", VALID_OBJECT);
    }

    @Test
    public void testValidNames() throws Exception {
        new Matcher("hello", VALID_OBJECT);
        new Matcher(".hello", VALID_OBJECT);
        new Matcher("..hello", VALID_OBJECT);
        new Matcher("hello_world", VALID_OBJECT);
        new Matcher("hello-world", VALID_OBJECT);
        new Matcher("hello+world", VALID_OBJECT);
        new Matcher("您好", VALID_OBJECT);
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

    ////  "matchers".MATCHER_NAME."quality"  ///////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidQualityValue() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":0.0}");
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":0.5}");
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":1.0}");
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":null}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidQualityTypeArray() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":[]}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidQualityTypeBoolean() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":true}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidQualityTypeInteger() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":1}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidQualityTypeFloatNegative() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":-1.0}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidQualityTypeObject() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":{}}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidQualityValueTooHigh() throws Exception {
        new Matcher("matcher_name", "{\"clause\":{\"match\":{\"{{ field }}\":\"{{ value }}\"}},\"quality\":100.0}");
    }

}
