/*
 * zentity
 * Copyright © 2018-2022 Dave Moore
 * https://zentity.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
