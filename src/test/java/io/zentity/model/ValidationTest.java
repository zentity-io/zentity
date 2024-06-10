package io.zentity.model;

import org.junit.Test;

import java.util.Collections;

import static io.zentity.model.Validation.validateStrictName;
import static org.junit.Assert.assertTrue;

public class ValidationTest {

    ////  Name validations  ////////////////////////////////////////////////////////////////////////////////////////////

    private static Exception assertInvalidStrictName(String name) {
        try {
            validateStrictName(name);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Invalid name [" + name + "]"));
            return e;
        }
        return new Exception("failure expected");
    }

    private static void assertValidStrictName(String name) throws ValidationException {
        validateStrictName(name);
    }

    @Test
    public void testInvalidStrictNameContainsAsterisk() {
        Exception ex = assertInvalidStrictName("selectivemploymentax*");
        assertTrue(ex.getMessage().contains("must not contain the following characters"));
        assertTrue(ex.getMessage().contains("*"));
    }

    @Test
    public void testInvalidStrictNameContainsHash() {
        Exception ex = assertInvalidStrictName("c#ke");
        assertTrue(ex.getMessage().contains("must not contain '#'"));
    }

    @Test
    public void testInvalidStrictNameContainsColon() {
        Exception ex = assertInvalidStrictName("p:psi");
        assertTrue(ex.getMessage().contains("must not contain ':'"));
    }

    @Test
    public void testInvalidStrictNameStartsWithUnderscore() {
        Exception ex = assertInvalidStrictName("_fanta");
        assertTrue(ex.getMessage().contains("must not start with '_', '-', or '+'"));
    }

    @Test
    public void testInvalidStrictNameStartsWithDash() {
        Exception ex = assertInvalidStrictName("-fanta");
        assertTrue(ex.getMessage().contains("must not start with '_', '-', or '+'"));
    }

    @Test
    public void testInvalidStrictNameStartsWithPlus() {
        Exception ex = assertInvalidStrictName("+fanta");
        assertTrue(ex.getMessage().contains("must not start with '_', '-', or '+'"));
    }

    @Test
    public void testInvalidStrictNameTooLong() {
        String name = String.join("", Collections.nCopies(100, "sprite"));
        Exception ex = assertInvalidStrictName(name);
        assertTrue(ex.getMessage().contains("name is too long"));
    }

    @Test
    public void testInvalidStrictNameIsDot() {
        Exception ex = assertInvalidStrictName(".");
        assertTrue(ex.getMessage().contains("must not be '.' or '..'"));
    }

    @Test
    public void testInvalidStrictNameIsDotDot() {
        Exception ex = assertInvalidStrictName("..");
        assertTrue(ex.getMessage().contains("must not be '.' or '..'"));
    }

    @Test
    public void testInvalidStrictNameIsNotLowercase() {
        Exception ex = assertInvalidStrictName("MELLO_yello");
        assertTrue(ex.getMessage().contains("must be lowercase"));
    }

    @Test
    public void testValidStrictNames() throws ValidationException {
        assertValidStrictName("hello");
        assertValidStrictName(".hello");
        assertValidStrictName("..hello");
        assertValidStrictName("hello_world");
        assertValidStrictName("hello-world");
        assertValidStrictName("hello+world");
        assertValidStrictName("您好");
    }
}
