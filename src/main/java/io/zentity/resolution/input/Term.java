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
package io.zentity.resolution.input;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.value.BooleanValue;
import io.zentity.resolution.input.value.DateValue;
import io.zentity.resolution.input.value.NumberValue;
import io.zentity.resolution.input.value.StringValue;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Term implements Comparable<Term> {

    private final String term;
    private Boolean isBoolean;
    private Boolean isDate;
    private Boolean isNumber;
    private BooleanValue booleanValue;
    private DateValue dateValue;
    private NumberValue numberValue;
    private StringValue stringValue;

    public Term(String term) throws ValidationException {
        validateTerm(term);
        this.term = term;
    }

    private void validateTerm(String term) throws ValidationException {
        if (Patterns.EMPTY_STRING.matcher(term).matches())
            throw new ValidationException("A term must be a non-empty string.");
    }

    public String term() { return this.term; }

    public static boolean isBoolean(String term) {
        String termLowerCase = term.toLowerCase();
        return termLowerCase.equals("true") || termLowerCase.equals("false");
    }

    public static boolean isDate(String term, String format) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            formatter.setLenient(false);
            formatter.parse(term);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    public static boolean isNumber(String term) {
        return Patterns.NUMBER_STRING.matcher(term).matches();
    }

    /**
     * Check if the term string is a boolean value.
     * Lazily store the decision and then return the decision.
     *
     * @return
     */
    public boolean isBoolean() {
        if (this.isBoolean == null)
            this.isBoolean = isBoolean(this.term);
        return this.isBoolean;
    }

    /**
     * Check if the term string is a date value.
     * Lazily store the decision and then return the decision.
     *
     * @return
     */
    public boolean isDate(String format) {
        if (this.isDate == null)
            this.isDate = isDate(this.term, format);
        return this.isDate;
    }

    /**
     * Convert the term to a BooleanValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public BooleanValue booleanValue() throws IOException, ValidationException {
        if (this.booleanValue == null) {
            JsonNode value = Json.MAPPER.readTree("{\"value\":" + this.term + "}").get("value");
            this.booleanValue = new BooleanValue(value);
        }
        return this.booleanValue;
    }

    /**
     * Check if the term string is a number value.
     * Lazily store the decision and then return the decision.
     *
     * @return
     */
    public boolean isNumber() {
        if (this.isNumber == null)
            this.isNumber = isNumber(this.term);
        return this.isNumber;
    }

    /**
     * Convert the term to a DateValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public DateValue dateValue() throws IOException, ValidationException {
        if (this.dateValue == null) {
            JsonNode value = Json.MAPPER.readTree("{\"value\":" + Json.quoteString(this.term) + "}").get("value");
            this.dateValue = new DateValue(value);
        }
        return this.dateValue;
    }

    /**
     * Convert the term to a NumberValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public NumberValue numberValue() throws IOException, ValidationException {
        if (this.numberValue == null) {
            JsonNode value = Json.MAPPER.readTree("{\"value\":" + this.term + "}").get("value");
            this.numberValue = new NumberValue(value);
        }
        return this.numberValue;
    }

    /**
     * Convert the term to a StringValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public StringValue stringValue() throws IOException, ValidationException {
        if (this.stringValue == null) {
            JsonNode value = Json.MAPPER.readTree("{\"value\":" + Json.quoteString(this.term) + "}").get("value");
            this.stringValue = new StringValue(value);
        }
        return this.stringValue;
    }

    @Override
    public int compareTo(Term o) {
        return this.term.compareTo(o.term);
    }

    @Override
    public String toString() {
        return this.term;
    }

    @Override
    public boolean equals(Object o) { return this.hashCode() == o.hashCode(); }

    @Override
    public int hashCode() { return this.term.hashCode(); }
}
