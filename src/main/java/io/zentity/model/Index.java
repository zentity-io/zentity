package io.zentity.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class Index {

    public static final Set<String> REQUIRED_FIELDS = new HashSet<>(
            Arrays.asList("fields")
    );
    private static final Pattern REGEX_EMPTY = Pattern.compile("^\\s*$");

    private final String name;
    private Map<String, IndexField> fields;
    private Map<String, Map<String, IndexField>> attributeIndexFieldsMap = new HashMap<>();

    public Index(String name, JsonNode json) throws ValidationException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public Index(String name, String json) throws ValidationException, IOException {
        validateName(name);
        this.name = name;
        this.deserialize(json);
    }

    public String name() {
        return this.name;
    }

    public Map<String, Map<String, IndexField>> attributeIndexFieldsMap() {
        return this.attributeIndexFieldsMap;
    }

    public Map<String, IndexField> fields() {
        return this.fields;
    }

    public void fields(JsonNode value) throws ValidationException {
        validateFields(value);
        Map<String, IndexField> fields = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> children = value.fields();
        while (children.hasNext()) {
            Map.Entry<String, JsonNode> child = children.next();
            String fieldName = child.getKey();
            JsonNode fieldObject = child.getValue();
            validateField(fieldName, fieldObject);
            fields.put(fieldName, new IndexField(this.name, fieldName, fieldObject));
        }
        this.fields = fields;
        this.rebuildAttributeIndexFieldsMap();
    }

    private void validateName(String value) throws ValidationException {
        if (REGEX_EMPTY.matcher(value).matches())
            throw new ValidationException("'indices' has an index with an empty name.");
    }

    private void validateField(String fieldName, JsonNode fieldObject) throws ValidationException {
        if (fieldName.equals(""))
            throw new ValidationException("'indices." + this.name + ".fields' has a field with an empty name.");
        if (!fieldObject.isObject())
            throw new ValidationException("'indices." + this.name + ".fields." + fieldName + "' must be an object.");
        if (fieldObject.size() == 0)
            throw new ValidationException("'indices." + this.name + ".fields." + fieldName + "' must not be empty.");
    }

    private void validateFields(JsonNode value) throws ValidationException {
        if (!value.isObject())
            throw new ValidationException("'indices." + this.name + ".fields' must be an object.");
        if (value.size() == 0)
            throw new ValidationException("'indices." + this.name + ".fields' must not be empty.");
    }

    private void validateObject(JsonNode object) throws ValidationException {
        if (!object.isObject())
            throw new ValidationException("'indices." + this.name + "' must be an object.");
        if (object.size() == 0)
            throw new ValidationException("'indices." + this.name + "' is empty.");
    }

    /**
     * Create a reverse index of attribute names to index fields for faster lookup of index fields by attributes
     * during a resolution job.
     */
    private void rebuildAttributeIndexFieldsMap() {
        this.attributeIndexFieldsMap = new HashMap<>();
        for (String indexFieldName : this.fields().keySet()) {
            String attributeName = this.fields().get(indexFieldName).attribute();
            if (!this.attributeIndexFieldsMap.containsKey(attributeName))
                this.attributeIndexFieldsMap.put(attributeName, new HashMap<>());
            if (!this.attributeIndexFieldsMap.get(attributeName).containsKey(indexFieldName))
                this.attributeIndexFieldsMap.get(attributeName).put(indexFieldName, this.fields.get(indexFieldName));
        }
    }

    /**
     * Deserialize, validate, and hold the state of an index object of an entity model.
     * Expected structure of the json variable:
     * <pre>
     * {
     *   "fields": {
     *     INDEX_FIELD_NAME: INDEX_FIELD_OBJECT
     *     ...
     *   }
     * }
     * </pre>
     *
     * @param json Index object of an entity model.
     * @throws ValidationException
     */
    public void deserialize(JsonNode json) throws ValidationException {
        validateObject(json);

        // Validate the existence of required fields.
        for (String field : REQUIRED_FIELDS)
            if (!json.has(field))
                throw new ValidationException("'indices." + this.name + "' is missing required field '" + field + "'.");

        // Validate and hold the state of fields.
        Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();
            switch (name) {
                case "fields":
                    this.fields(value);
                    break;
                default:
                    throw new ValidationException("'indices." + this.name + "." + name + "' is not a recognized field.");
            }
        }
    }

    public void deserialize(String json) throws ValidationException, IOException {
        deserialize(new ObjectMapper().readTree(json));
    }

}
