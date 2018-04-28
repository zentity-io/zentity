package io.zentity.resolution.input.scope;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Include extends ScopeField {

    public Include() {
        super();
    }

    @Override
    public void deserialize(JsonNode json, Model model) throws ValidationException, IOException {
        if (!json.isNull() && !json.isObject())
            throw new ValidationException("The 'scope.include' field of the request body must be an object.");

        // Parse and validate the "scope.include" fields of the request body.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            switch (name) {
                case "attributes":
                    this.attributes = parseAttributes("include", model, json.get("attributes"));
                    break;
                case "resolvers":
                    this.resolvers = parseResolvers("include", json.get("resolvers"));
                    break;
                case "indices":
                    this.indices = parseIndices("include", json.get("indices"));
                    break;
                default:
                    throw new ValidationException("'scope.include." + name + "' is not a recognized field.");
            }
        }
    }

}
