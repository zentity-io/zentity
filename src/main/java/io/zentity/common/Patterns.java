package io.zentity.common;

import java.util.regex.Pattern;

public class Patterns {

    public static final Pattern COLON = Pattern.compile(":");
    public static final Pattern EMPTY_STRING = Pattern.compile("^\\s*$");
    public static final Pattern PERIOD = Pattern.compile("\\.");
    public static final Pattern VARIABLE = Pattern.compile("\\{\\{\\s*([^\\s{}]+)\\s*}}");
    public static final Pattern VARIABLE_PARAMS = Pattern.compile("^params\\.(.+)");

}
