package io.zentity.model;

import io.zentity.common.Patterns;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;

import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.function.BiFunction;

public class Validation {

    public static final int MAX_STRICT_NAME_BYTES = 255;

    /**
     * Validate that a name meets the same requirements as the Elasticsearch index name requirements.
     *
     * @param name     The name to validate.
     * @param optional Whether the name can be empty.
     * @return an optional ValidationException if the type is not in a valid format.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/7.10/indices-create-index.html#indices-create-api-path-params">Elasticsearch Index Name Requirements</a>
     * @see org.elasticsearch.cluster.metadata.MetadataCreateIndexService#validateIndexOrAliasName
     */
    public static void validateStrictName(String name, Boolean optional) throws ValidationException {
        BiFunction<String, String, String> msg = (invalidName, description) -> "Invalid name [" + invalidName + "], " + description;
        if (!optional) {
            if (name == null)
                throw new ValidationException(msg.apply("", "must not be empty"));
            if (Patterns.EMPTY_STRING.matcher(name).matches())
                throw new ValidationException(msg.apply(name, "must not be empty"));
        }
        if (!Strings.validFileName(name))
            throw new ValidationException(msg.apply(name, "must not contain the following characters: " + Strings.INVALID_FILENAME_CHARS));
        if (name.contains("#"))
            throw new ValidationException(msg.apply(name, "must not contain '#'"));
        if (name.contains(":"))
            throw new ValidationException(msg.apply(name, "must not contain ':'"));
        if (name.charAt(0) == '_' || name.charAt(0) == '-' || name.charAt(0) == '+')
            throw new ValidationException(msg.apply(name, "must not start with '_', '-', or '+'"));
        int byteCount = 0;
        try {
            byteCount = name.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of name [" + name + "]", e);
        }
        if (byteCount > MAX_STRICT_NAME_BYTES)
            throw new ValidationException(msg.apply(name, "name is too long, (" + byteCount + " > " + MAX_STRICT_NAME_BYTES + ")"));
        if (name.equals(".") || name.equals(".."))
            throw new ValidationException(msg.apply(name,  "must not be '.' or '..'"));
        if (!name.toLowerCase(Locale.ROOT).equals(name))
            throw new ValidationException(msg.apply(name,  "must be lowercase"));
    }

    public static void validateStrictName(String name) throws ValidationException {
        validateStrictName(name, false);
    }
}
