package org.elasticsearch.plugin.zentity;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModelsActionTest {
    private static Exception assertInvalidEntityType(String entityType) {
        return ModelsAction.validateEntityType(entityType)
                .map((ex) -> {
                    assertTrue(ex.getMessage().contains("Invalid entity type [" + entityType + "]"));
                    return ex;
                })
                .orElseGet(() -> {
                    fail("failure expected");
                    return null;
                });
    }

    @Test
    public void testInvalidEntityNameContainsAstrix() {
        Exception ex = assertInvalidEntityType("selectivemploymentax*");
        assertTrue(ex.getMessage().contains("must not contain the following characters"));
        assertTrue(ex.getMessage().contains("*"));
    }

    @Test
    public void testInvalidEntityNameContainsHash() {
        Exception ex = assertInvalidEntityType("c#ke");
        assertTrue(ex.getMessage().contains("must not contain '#'"));
    }

    @Test
    public void testInvalidEntityNameContainsColon() {
        Exception ex = assertInvalidEntityType("p:psi");
        assertTrue(ex.getMessage().contains("must not contain ':'"));
    }

    @Test
    public void testInvalidEntityNameStartsWith_() {
        Exception ex = assertInvalidEntityType("_fanta");
        assertTrue(ex.getMessage().contains("must not start with '_', '-', or '+'"));
    }

    @Test
    public void testInvalidEntityNameStartsWithDash() {
        Exception ex = assertInvalidEntityType("-fanta");
        assertTrue(ex.getMessage().contains("must not start with '_', '-', or '+'"));
    }

    @Test
    public void testInvalidEntityNameStartsWithPlus() {
        Exception ex = assertInvalidEntityType("+fanta");
        assertTrue(ex.getMessage().contains("must not start with '_', '-', or '+'"));
    }

    @Test
    public void testInvalidEntityNameTooLong() {
        String name = String.join("", Collections.nCopies(100, "sprite"));;
        Exception ex = assertInvalidEntityType(name);
        assertTrue(ex.getMessage().contains("entity type is too long"));
    }

    @Test
    public void testInvalidEntityNameIsDot() {
        Exception ex = assertInvalidEntityType(".");
        assertTrue(ex.getMessage().contains("must not be '.' or '..'"));
    }

    @Test
    public void testInvalidEntityNameIsDotDot() {
        Exception ex = assertInvalidEntityType("..");
        assertTrue(ex.getMessage().contains("must not be '.' or '..'"));
    }

    @Test
    public void testInvalidEntityNameIsNotLowercase() {
        Exception ex = assertInvalidEntityType("MELLO_yello");
        assertTrue(ex.getMessage().contains("must be lowercase"));
    }
}
