package org.elasticsearch.plugin.zentity;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

public class ParamsUtil {
    /**
     * Parse a string as a boolean, where empty values are interpreted as "true". Similar to
     * {@link RestRequest#paramAsBoolean}.
     *
     * @param val The raw param value.
     * @return The parsed bool.
     */
    private static boolean asBoolean(String val) {
        // Treat empty string as true because that allows the presence of the url parameter to mean "turn this on"
        if (val != null && val.length() == 0) {
            return true;
        } else {
            return Booleans.parseBoolean(val);
        }
    }

    /**
     * Get a parameter, which may or may not be present, from multiple sets of parameters.
     *
     * @param key The parameter key.
     * @param params The primary set of parameters.
     * @param defaultParams A backup set of parameters, if the parameter is not found in the primary.
     * @return An optional string of the parameter's value.
     */
    private static Optional<String> opt(String key, Map<String, String> params, Map<String, String> defaultParams) {
        return Optional
            .ofNullable(params.get(key))
            .or(() -> Optional.ofNullable(defaultParams.get(key)));
    }

    private static <T> T opt(String key, T defaultValue, Function<String, T> mapper, Map<String, String> params, Map<String, String> defaultParams) {
        return opt(key, params, defaultParams)
            .map((val) -> {
                try {
                    return mapper.apply(val);
                } catch (Exception ex) {
                    throw new BadRequestException("Failed to parse parameter [" + key + "] with value [" + val + "]", ex);
                }
            })
            .orElse(defaultValue);
    }

    public static String optString(String key, String defaultValue, Map<String, String> params, Map<String, String> defaultParams) {
        return opt(key, params, defaultParams).orElse(defaultValue);
    }

    public static Boolean optBoolean(String key, Boolean defaultValue, Map<String, String> params, Map<String, String> defaultParams) {
        return opt(key, defaultValue, ParamsUtil::asBoolean, params, defaultParams);
    }

    public static Integer optInteger(String key, Integer defaultValue, Map<String, String> params, Map<String, String> defaultParams) {
        return opt(key, defaultValue, Integer::parseInt, params, defaultParams);
    }

    public static TimeValue optTimeValue(String key, TimeValue defaultValue, Map<String, String> params, Map<String, String> defaultParams) {
        return opt(key, defaultValue, s -> TimeValue.parseTimeValue(s, key), params, defaultParams);
    }

    /**
     * Read many parameters from a {@link RestRequest} into a {@link Map}. It is necessary to read all possible params
     * in a {@link org.elasticsearch.rest.BaseRestHandler BaseRestHandler's} prepare method to avoid throwing
     * a validation error.
     *
     * @param req A request.
     * @param params All the parameters to read from the request.
     * @return A map from the parameter name to the parameter value in the request.
     */
    public static Map<String, String> readAll(RestRequest req, String ...params) {
        Map<String, String> paramsMap = new TreeMap<>();

        for (String param : params) {
            paramsMap.put(param, req.param(param));
        }

        return paramsMap;
    }
}
