package io.zentity.resolution.input.scope;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Scope {

    private Exclude exclude = new Exclude();
    private Include include = new Include();

    public Scope() {
    }

    public Exclude exclude() {
        return this.exclude;
    }

    public Include include() {
        return this.include;
    }

    public void deserialize(JsonNode json, Model model) throws ValidationException, IOException {
        if (!json.isNull() && !json.isObject())
            throw new ValidationException("The 'scope' field of the request body must be an object.");

        // Parse and validate the "scope.exclude" and "scope.include" fields of the request body.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            switch (name) {
                case "exclude":
                    this.exclude.deserialize(json.get("exclude"), model);
                    break;
                case "include":
                    this.include.deserialize(json.get("include"), model);
                    break;
                default:
                    throw new ValidationException("'scope." + name + "' is not a recognized field.");
            }
        }

    }

    public void deserialize(String json, Model model) throws ValidationException, IOException {
        deserialize(Json.MAPPER.readTree(json), model);
    }
}

