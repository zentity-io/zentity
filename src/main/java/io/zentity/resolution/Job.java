package io.zentity.resolution;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Job {

    // Constants
    public static final boolean DEFAULT_INCLUDE_ATTRIBUTES = true;
    public static final boolean DEFAULT_INCLUDE_HITS = true;
    public static final boolean DEFAULT_INCLUDE_QUERIES = false;
    public static final boolean DEFAULT_INCLUDE_SOURCE = true;
    public static final int DEFAULT_MAX_DOCS_PER_QUERY = 1000;
    public static final int DEFAULT_MAX_HOPS = 100;
    public static final boolean DEFAULT_PRETTY = false;
    public static final boolean DEFAULT_PROFILE = false;
    private static final Pattern ATTRIBUTE_FIELD = Pattern.compile("\\{\\{\\s*(field)\\s*}}");
    private static final Pattern ATTRIBUTE_VALUE = Pattern.compile("\\{\\{\\s*(value)\\s*}}");
    private final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    private final JsonStringEncoder encoder = new JsonStringEncoder();

    // Job configuration
    private HashMap<String, HashMap<String, String>> attributes = new HashMap<>();
    private HashMap<String, HashMap<String, String>> indices = new HashMap<>();
    private HashMap<String, HashSet<Object>> inputAttributes = new HashMap<>();
    private HashMap<String, ArrayList<String>> resolvers = new HashMap<>();
    private boolean includeAttributes = DEFAULT_INCLUDE_ATTRIBUTES;
    private boolean includeHits = DEFAULT_INCLUDE_QUERIES;
    private boolean includeQueries = DEFAULT_INCLUDE_QUERIES;
    private boolean includeSource = DEFAULT_INCLUDE_SOURCE;
    private int maxDocsPerQuery = DEFAULT_MAX_DOCS_PER_QUERY;
    private int maxHops = DEFAULT_MAX_HOPS;
    private boolean pretty = DEFAULT_PRETTY;
    private boolean profile = DEFAULT_PROFILE;

    // Job state
    private HashSet<String> attributesQueried = new HashSet<>();
    private NodeClient client;
    private HashSet<String> docIds = new HashSet<>();
    private List<String> hits = new ArrayList<>();
    private int hop = 0;
    private List<String> queries = new ArrayList<>();
    private boolean ran = false;

    public Job(NodeClient client) {
        this.client = client;
    }

    /**
     * Resets the variables that hold the state of the job, in case the same Job object is reused.
     */
    private void resetState() {
        this.attributesQueried = new HashSet<>();
        this.docIds = new HashSet<>();
        this.hits = new ArrayList<>();
        this.hop = 0;
        this.queries = new ArrayList<>();
    }

    public HashMap<String, HashSet<Object>> getInputAttributes() {
        return this.inputAttributes;
    }

    public void setInputAttributes(HashMap<String, HashSet<Object>> inputAttributes) {
        this.inputAttributes = inputAttributes;
    }

    public boolean getIncludeAttributes() {
        return this.includeAttributes;
    }

    public void setIncludeAttributes(boolean includeAttributes) {
        this.includeAttributes = includeAttributes;
    }

    public boolean getIncludeHits() {
        return this.includeHits;
    }

    public void setIncludeHits(boolean includeHits) {
        this.includeHits = includeHits;
    }

    public boolean getIncludeQueries() {
        return this.includeQueries;
    }

    public void setIncludeQueries(boolean includeQueries) {
        this.includeQueries = includeQueries;
    }

    public boolean getIncludeSource() {
        return this.includeSource;
    }

    public void setIncludeSource(boolean includeSource) {
        this.includeSource = includeSource;
    }

    public HashMap<String, HashMap<String, String>> getIndices() {
        return this.indices;
    }

    public void setIndices(HashMap<String, HashMap<String, String>> indices) {
        this.indices = indices;
    }

    public boolean getPretty() {
        return this.pretty;
    }

    public void setPretty(boolean pretty) {
        this.pretty = pretty;
    }

    public HashMap<String, HashMap<String, String>> getAttributes() {
        return this.attributes;
    }

    public void setAttributes(HashMap<String, HashMap<String, String>> attributes) {
        this.attributes = attributes;
    }

    public int getMaxHops() {
        return this.maxHops;
    }

    public void setMaxHops(int maxHops) {
        this.maxHops = maxHops;
    }

    public int getMaxDocsPerQuery() {
        return this.maxDocsPerQuery;
    }

    public void setMaxDocsPerQuery(int maxDocsPerQuery) {
        this.maxDocsPerQuery = maxDocsPerQuery;
    }

    public Boolean getProfile() {
        return this.profile;
    }

    public void setProfile(Boolean profile) {
        this.profile = profile;
    }

    public HashMap<String, ArrayList<String>> getResolvers() {
        return this.resolvers;
    }

    public void setResolvers(HashMap<String, ArrayList<String>> resolvers) {
        this.resolvers = resolvers;
    }

    private String jsonStringEscape(String value) {
        return new String(encoder.quoteAsString(value));
    }

    private String jsonStringQuote(String value) {
        return "\"" + value + "\"";
    }

    private String jsonStringFormat(String value) {
        return jsonStringQuote(jsonStringEscape(value));
    }

    /**
     * Determine if a matcher of an attribute exists in an index.
     *
     * @param index     The name of the index to reference in the entity model.
     * @param attribute The name of the attribute to reference in the entity model.
     * @param matcher   The name of the matcher to reference in the entity model.
     * @return Boolean decision.
     */
    private boolean matcherSupported(String index, String attribute, String matcher) {
        // The input must have the attribute.
        if (!this.inputAttributes.containsKey(attribute))
            return false;
        // The index must have the attribute and matcher.
        if (!this.indices.get(index).containsKey(attribute + "." + matcher))
            return false;
        return true;
    }

    /**
     * Determine if the fields used by the matchers of a resolver exist in an index and the current input.
     *
     * @param index    The name of the index to reference in the entity model.
     * @param resolver The name of the resolver to reference in the entity model.
     * @return Boolean decision.
     */
    private boolean resolverSupported(String index, String resolver) {
        for (String attribute : this.resolvers.get(resolver)) {
            // The input must have each field in the resolver.
            if (!this.inputAttributes.containsKey(attribute))
                return false;
            // The input must have at least one value for each field in the resolver.
            if (this.inputAttributes.get(attribute).size() == 0)
                return false;
            // The index must have at least one matcher of each attribute in the resolver.
            boolean matcherFound = false;
            for (String matcher : this.attributes.get(attribute).keySet()) {
                if (matcherSupported(index, attribute, matcher))
                    matcherFound = true;
                // The input must have at least one value that has not yet been queried for each field in the resolver.
                boolean allQueried = true;
                for (Object value : this.inputAttributes.get(attribute)) {
                    String attributeHash = String.join(":", index, resolver, attribute, matcher, value.toString());
                    if (!this.attributesQueried.contains(attributeHash))
                        allQueried = false;
                    break;
                }
                if (allQueried)
                    return false;
            }
            if (!matcherFound)
                return false;
        }
        return true;
    }

    /**
     * Given a matcher template from the "attributes" field of an entity model, replace the {{ field }} and {{ value }}
     * variables.
     *
     * @param attribute The name of the attribute to reference in the entity model.
     * @param matcher   The name of the matcher whose query template will be pulled from the entity model.
     * @param index     The name of the index to reference in the entity model.
     * @param value     The value to populate in the query template.
     * @return A "bool" clause that references the desired field and value.
     */
    private String populateMatcherQueryTemplate(String attribute, String matcher, String index, String value) {
        String attributeMatcher = attribute + "." + matcher;
        String indexField = this.indices.get(index).get(attributeMatcher);
        String matcherTemplate = this.attributes.get(attribute).get(matcher);
        matcherTemplate = ATTRIBUTE_FIELD.matcher(matcherTemplate).replaceAll(indexField);
        matcherTemplate = ATTRIBUTE_VALUE.matcher(matcherTemplate).replaceAll(value);
        return matcherTemplate;
    }

    /**
     * Submit a search query to Elasticsearch.
     *
     * @param index The name of the index to search.
     * @param query The query to search.
     * @return The search response returned by Elasticsearch.
     * @throws IOException
     */
    private SearchResponse search(String index, String query) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                .getNamedXContents()), query)) {
            searchSourceBuilder.parseXContent(parser);
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        return searchRequestBuilder.setIndices(index).setSource(searchSourceBuilder).execute().actionGet();
    }

    /**
     * Given a set of attribute values, determine which queries to submit to which indices then submit them and recurse.
     *
     * @throws IOException
     */
    private void traverse() throws IOException {

        // Prepare to collect attributes from the results of these queries as the inputs to subsequent queries.
        HashMap<String, HashSet<Object>> nextInputAttributes = new HashMap<>();

        // Construct a query for each index that maps to a resolver.
        for (String index : this.indices.keySet()) {

            // Construct a subquery for each resolver that maps to the index.
            ArrayList<String> resolverClauses = new ArrayList<>();
            for (String resolver : this.resolvers.keySet()) {

                // Can we use this resolver on this index and this input?
                if (!resolverSupported(index, resolver))
                    continue;

                // Construct a "should" clause for each matcher of this resolver.
                ArrayList<String> attributeClauses = new ArrayList<>();
                for (String attribute : this.resolvers.get(resolver)) {
                    ArrayList<String> matcherClauses = new ArrayList<>();
                    for (String matcher : this.attributes.get(attribute).keySet()) {

                        // Can we use this matcher on this index and this input?
                        if (!matcherSupported(index, attribute, matcher))
                            continue;

                        // Construct a clause for each input value for this attribute.
                        ArrayList<String> valueClauses = new ArrayList<>();
                        for (Object value : this.inputAttributes.get(attribute)) {

                            // Skip value if it's blank.
                            if (value == null || value.equals(""))
                                continue;

                            // Mark this value as having been queried for this index, resolver, attribute, and matcher.
                            String valueString = value.toString();
                            String attributeHash = String.join(":", index, resolver, attribute, matcher, valueString);
                            this.attributesQueried.add(attributeHash);

                            // Populate the {{ field }} and {{ value }} variables of the matcher template.
                            valueClauses.add(populateMatcherQueryTemplate(attribute, matcher, index, valueString));
                        }
                        if (valueClauses.size() == 0)
                            continue;

                        // Combine each value clause into a single "should" clause.
                        String valuesClause = String.join(",", valueClauses);
                        if (valueClauses.size() > 1)
                            valuesClause = "{\"bool\":{\"should\":[" + valuesClause + "]}}";
                        matcherClauses.add(valuesClause);
                    }
                    if (matcherClauses.size() == 0)
                        continue;

                    // Combine each matcher clause into a single "should" clause.
                    String matchersClause = String.join(",", matcherClauses);
                    if (matcherClauses.size() > 1)
                        matchersClause = "{\"bool\":{\"should\":[" + matchersClause + "]}}";
                    attributeClauses.add(matchersClause);
                }
                if (attributeClauses.size() == 0)
                    continue;

                // Combine each attribute clause into a single "filter" clause.
                String attributesClause = String.join(",", attributeClauses);
                if (attributeClauses.size() > 1)
                    attributesClause = "{\"bool\":{\"filter\":[" + attributesClause + "]}}";
                resolverClauses.add(attributesClause);
            }

            // Skip this query if there are no resolver clauses.
            if (resolverClauses.size() == 0)
                continue;

            // Combine each resolver clause into a single "should" clause.
            String resolversClause = String.join(",", resolverClauses);
            if (resolverClauses.size() > 1)
                resolversClause = "{\"bool\":{\"should\":[" + resolversClause + "]}}";

            // Construct query for this index.
            String query;
            if (this.docIds.size() > 0) {
                String idsFilter = "\"must_not\":{\"ids\":{\"values\":[" + String.join(",", this.docIds) + "]}}";
                resolversClause = "{\"bool\":{" + idsFilter + ",\"filter\":" + resolversClause + "}}";
            }
            if (this.profile)
                query = "{\"query\": " + resolversClause + ",\"size\": " + this.maxDocsPerQuery + ",\"profile\":true}";
            else
                query = "{\"query\": " + resolversClause + ",\"size\": " + this.maxDocsPerQuery + "}";

            // Submit query to Elasticsearch.
            SearchResponse response = this.search(index, query);

            // Read response from Elasticsearch.
            String responseBody = response.toString();
            JsonNode responseData = this.mapper.readTree(responseBody);

            // Store request and response.
            if (this.includeQueries || this.profile) {
                JsonNode responseDataCopy = responseData.deepCopy();
                ObjectNode responseDataCopyObj = (ObjectNode) responseDataCopy;
                if (responseDataCopyObj.has("hits"))
                    responseDataCopyObj.remove("hits");
                String logged = "{\"request\":" + query + ",\"response\":" + responseDataCopyObj + "}";
                this.queries.add(logged);
            }

            // Read the hits
            if (!responseData.has("hits"))
                continue;
            if (!responseData.get("hits").has("hits"))
                continue;
            for (JsonNode doc : responseData.get("hits").get("hits")) {

                // Skip doc if already fetched. Otherwise mark doc as fetched and then proceed.
                String _id = jsonStringFormat(doc.get("_id").textValue());
                if (this.docIds.contains(_id))
                    continue;
                this.docIds.add(_id);

                // Gather attributes from doc. Store them in the "_attributes" field of the doc,
                // and include them in the attributes for subsequent queries.
                TreeMap<String, Object> docAttributes = new TreeMap<>();
                for (String attributeMatcher : this.indices.get(index).keySet()) {
                    String indexSubfield = this.indices.get(index).get(attributeMatcher);
                    String indexField = indexSubfield.split("\\.")[0];
                    String attribute = attributeMatcher.split("\\.")[0];
                    if (doc.get("_source").has(indexField)) {
                        Object value;
                        if (doc.get("_source").get(indexField).isBoolean())
                            value = doc.get("_source").get(indexField).booleanValue();
                        else if (doc.get("_source").get(indexField).isDouble())
                            value = doc.get("_source").get(indexField).isDouble();
                        else if (doc.get("_source").get(indexField).isFloat())
                            value = doc.get("_source").get(indexField).floatValue();
                        else if (doc.get("_source").get(indexField).isInt())
                            value = doc.get("_source").get(indexField).intValue();
                        else if (doc.get("_source").get(indexField).isLong())
                            value = doc.get("_source").get(indexField).longValue();
                        else if (doc.get("_source").get(indexField).isShort())
                            value = doc.get("_source").get(indexField).shortValue();
                        else if (doc.get("_source").get(indexField).isNull())
                            value = "";
                        else
                            value = doc.get("_source").get(indexField).asText();
                        docAttributes.put(attribute, value);
                        if (!nextInputAttributes.containsKey(attribute))
                            nextInputAttributes.put(attribute, new HashSet<>());
                        nextInputAttributes.get(attribute).add(value);
                        if (!this.inputAttributes.containsKey(attribute))
                            this.inputAttributes.put(attribute, new HashSet<>());
                        this.inputAttributes.get(attribute).add(value);
                    }
                }

                // Modify doc metadata.
                if (this.includeHits) {
                    ObjectNode docObjNode = (ObjectNode) doc;
                    docObjNode.remove("_score");
                    docObjNode.put("_hop", this.hop);
                    if (!this.includeSource)
                        docObjNode.remove("_source");
                    if (this.includeAttributes) {
                        ObjectNode docAttributesObjNode = docObjNode.putObject("_attributes");
                        for (String attribute : docAttributes.keySet()) {
                            Object value = docAttributes.get(attribute);
                            if (value.getClass() == Boolean.class)
                                docAttributesObjNode.put(attribute, (Boolean) value);
                            else if (value.getClass() == Double.class)
                                docAttributesObjNode.put(attribute, (Double) value);
                            else if (value.getClass() == Float.class)
                                docAttributesObjNode.put(attribute, (Float) value);
                            else if (value.getClass() == Integer.class)
                                docAttributesObjNode.put(attribute, (Integer) value);
                            else if (value.getClass() == Long.class)
                                docAttributesObjNode.put(attribute, (Long) value);
                            else if (value.getClass() == Short.class)
                                docAttributesObjNode.put(attribute, (Short) value);
                            else if (value.getClass() == null)
                                docAttributesObjNode.put(attribute, "");
                            else
                                docAttributesObjNode.put(attribute, (String) value);
                        }
                    }

                    // Store doc in response.
                    this.hits.add(doc.toString());
                }
            }
        }

        // Stop traversing if we've reached max depth.
        if (this.maxHops > -1 && this.hop >= this.maxHops) {
            return;
        }
        // Stop traversing if there are no more attributes to query.
        if (nextInputAttributes.keySet().size() == 0) {
            return;
        }
        // Update hop count and traverse.
        this.hop++;
        this.traverse();
    }

    /**
     * Run the entity resolution job.
     *
     * @return A JSON string to be returned as the body of the response to a client.
     * @throws IOException
     */
    public String run() throws IOException {
        try {

            // Reset the state of the job if reusing this Job object.
            if (this.ran)
                this.resetState();

            // Start timer and begin job
            long startTime = System.nanoTime();
            this.traverse();
            long took = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

            // Format response
            ArrayList<String> responseParts = new ArrayList<>();
            responseParts.add("\"took\":" + Long.toString(took));
            if (this.includeHits)
                responseParts.add("\"hits\":{\"total\":" + this.hits.size() + ",\"hits\":[" + String.join(",", this.hits) + "]}");
            if (this.includeQueries || this.profile)
                responseParts.add("\"queries\":[" + queries + "]}");
            String response = "{" + String.join(",", responseParts) + "}";
            if (this.pretty)
                response = this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(response));
            return response;

        } finally {
            this.ran = true;
        }
    }

}
