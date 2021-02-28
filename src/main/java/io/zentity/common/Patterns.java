package io.zentity.common;

import java.util.regex.Pattern;

/**
 * Regular expression patterns to compile once when the zentity plugin loads.
 */
public class Patterns {

    public static final Pattern COLON = Pattern.compile(":");
    public static final Pattern EMPTY_STRING = Pattern.compile("^\\s*$");
    public static final Pattern NEWLINE = Pattern.compile("\\r?\\n");
    public static final Pattern NUMBER_STRING = Pattern.compile("^-?\\d*\\.{0,1}\\d+$");
    public static final Pattern PERIOD = Pattern.compile("\\.");
    public static final Pattern VARIABLE = Pattern.compile("\\{\\{\\s*([^\\s{}]+)\\s*}}");
    public static final Pattern VARIABLE_PARAMS = Pattern.compile("^params\\.(.+)");
}
