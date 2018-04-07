package io.zentity.resolution;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.zentity.model.Attribute;
import io.zentity.model.Matcher;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
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
    private Model model;
    private HashMap<String, HashSet<Object>> inputAttributes = new HashMap<>();
    private boolean includeAttributes = DEFAULT_INCLUDE_ATTRIBUTES;
    private boolean includeHits = DEFAULT_INCLUDE_QUERIES;
    private boolean includeQueries = DEFAULT_INCLUDE_QUERIES;
    private boolean includeSource = DEFAULT_INCLUDE_SOURCE;
    private int maxDocsPerQuery = DEFAULT_MAX_DOCS_PER_QUERY;
    private int maxHops = DEFAULT_MAX_HOPS;
    private boolean pretty = DEFAULT_PRETTY;
    private boolean profile = DEFAULT_PROFILE;

    // Job state
    private NodeClient client;
    private HashMap<String, HashSet<String>> docIds = new HashMap<>();
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
        this.docIds = new HashMap<>();
        this.hits = new ArrayList<>();
        this.hop = 0;
        this.queries = new ArrayList<>();
        this.ran = false;
    }

    public HashMap<String, HashSet<Object>> getInputAttributes() {
        return this.inputAttributes;
    }

    public void inputAttributes(HashMap<String, HashSet<Object>> inputAttributes) {
        this.inputAttributes = inputAttributes;
    }

    public boolean includeAttributes() {
        return this.includeAttributes;
    }

    public void includeAttributes(boolean includeAttributes) {
        this.includeAttributes = includeAttributes;
    }

    public boolean includeHits() {
        return this.includeHits;
    }

    public void includeHits(boolean includeHits) {
        this.includeHits = includeHits;
    }

    public boolean includeQueries() {
        return this.includeQueries;
    }

    public void includeQueries(boolean includeQueries) {
        this.includeQueries = includeQueries;
    }

    public boolean includeSource() {
        return this.includeSource;
    }

    public void includeSource(boolean includeSource) {
        this.includeSource = includeSource;
    }

    public Model model() {
        return this.model;
    }

    public void model(Model model) {
        this.model = model;
    }

    public int maxHops() {
        return this.maxHops;
    }

    public void maxHops(int maxHops) {
        this.maxHops = maxHops;
    }

    public int maxDocsPerQuery() {
        return this.maxDocsPerQuery;
    }

    public void maxDocsPerQuery(int maxDocsPerQuery) {
        this.maxDocsPerQuery = maxDocsPerQuery;
    }

    public boolean pretty() {
        return this.pretty;
    }

    public void pretty(boolean pretty) {
        this.pretty = pretty;
    }

    public Boolean profile() {
        return this.profile;
    }

    public void profile(Boolean profile) {
        this.profile = profile;
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
     * Determine if we can construct a query for a given resolver on a given index with a given input.
     * Each attribute of the resolver must be mapped to a field of the index and have a matcher defined for it.
     *
     * @param indexName    The name of the index to reference in the entity model.
     * @param resolverName The name of the resolver to reference in the entity model.
     * @return Boolean decision.
     */
    private boolean canQuery(String indexName, String resolverName) {

        // Each attribute of the resolver must pass these conditions:
        for (String attributeName : this.model.resolvers().get(resolverName).attributes()) {

            // The input must have the attribute.
            if (!this.inputAttributes.containsKey(attributeName))
                return false;

            // The input must have at least one value for the attribute.
            if (this.inputAttributes.get(attributeName).isEmpty())
                return false;

            // The index must have at least one index field mapped to the attribute.
            if (!this.model.indices().get(indexName).attributeIndexFieldsMap().containsKey(attributeName))
                return false;
            if (this.model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).isEmpty())
                return false;

            // The index field must have a matcher defined for it.
            boolean hasMatcher = false;
            for (String indexFieldName : this.model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).keySet()) {
                if (this.model.indices().get(indexName).fields().get(indexFieldName).matcher() != null) {
                    hasMatcher = true;
                    break;
                }
            }
            if (!hasMatcher)
                return false;
        }
        return true;
    }

    /**
     * Given a clause from the "matchers" field of an entity model, replace the {{ field }} and {{ value }} variables.
     *
     * @param matcher        The matcher object.
     * @param indexFieldName The name of the index field to populate in the clause.
     * @param value          The value to populate in the clause.
     * @return A "bool" clause that references the desired field and value.
     */
    private String populateMatcherClause(Matcher matcher, String indexFieldName, String value) {
        String matcherClause = matcher.clause();
        matcherClause = ATTRIBUTE_FIELD.matcher(matcherClause).replaceAll(indexFieldName);
        matcherClause = ATTRIBUTE_VALUE.matcher(matcherClause).replaceAll(value);
        return matcherClause;
    }

    /**
     * Submit a search query to Elasticsearch.
     *
     * @param indexName The name of the index to search.
     * @param query     The query to search.
     * @return The search response returned by Elasticsearch.
     * @throws IOException
     */
    private SearchResponse search(String indexName, String query) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                .getNamedXContents()), query)) {
            searchSourceBuilder.parseXContent(parser);
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        return searchRequestBuilder.setIndices(indexName).setSource(searchSourceBuilder).execute().actionGet();
    }

    /**
     * Given a set of attribute values, determine which queries to submit to which indices then submit them and recurse.
     *
     * @throws IOException
     */
    private void traverse() throws IOException, ValidationException {

        // Prepare to collect attributes from the results of these queries as the inputs to subsequent queries.
        HashMap<String, HashSet<Object>> nextInputAttributes = new HashMap<>();
        Boolean newHits = false;

        // Construct a query for each index that maps to a resolver.
        for (String indexName : this.model.indices().keySet()) {

            // Track _ids for this index.
            if (!this.docIds.containsKey(indexName))
                this.docIds.put(indexName, new HashSet<>());

            // Construct a "should" clause for each resolver that maps to the index.
            ArrayList<String> resolverClauses = new ArrayList<>();
            for (String resolverName : this.model.resolvers().keySet()) {

                // Can we use this resolver on this index and this input?
                boolean canQuery = canQuery(indexName, resolverName);
                if (!canQuery)
                    continue;

                // Construct a "filter" clause for each attribute of this resolver.
                ArrayList<String> attributeClauses = new ArrayList<>();
                for (String attributeName : this.model.resolvers().get(resolverName).attributes()) {

                    // Construct a "should" clause for each index field mapped to this attribute.
                    ArrayList<String> indexFieldClauses = new ArrayList<>();
                    for (String indexFieldName : this.model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).keySet()) {

                        // Can we use this index field?
                        boolean hasMatcher = this.model.indices().get(indexName).fields().get(indexFieldName).matcher() != null;
                        if (!hasMatcher)
                            continue;

                        // Construct a clause for each input value for this attribute.
                        ArrayList<String> valueClauses = new ArrayList<>();
                        for (Object value : this.inputAttributes.get(attributeName)) {

                            // Skip value if it's blank.
                            if (value == null || value.equals(""))
                                continue;

                            // Populate the {{ field }} and {{ value }} variables of the matcher template.
                            String matcherName = this.model.indices().get(indexName).fields().get(indexFieldName).matcher();
                            Matcher matcher = this.model.matchers().get(matcherName);
                            valueClauses.add(populateMatcherClause(matcher, indexFieldName, value.toString()));
                        }
                        if (valueClauses.size() == 0)
                            continue;

                        // Combine each value clause into a single "should" clause.
                        String valuesClause = String.join(",", valueClauses);
                        if (valueClauses.size() > 1)
                            valuesClause = "{\"bool\":{\"should\":[" + valuesClause + "]}}";
                        indexFieldClauses.add(valuesClause);
                    }
                    if (indexFieldClauses.size() == 0)
                        continue;

                    // Combine each matcher clause into a single "should" clause.
                    String indexFieldsClause = String.join(",", indexFieldClauses);
                    if (indexFieldClauses.size() > 1)
                        indexFieldsClause = "{\"bool\":{\"should\":[" + indexFieldsClause + "]}}";
                    attributeClauses.add(indexFieldsClause);
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
            HashSet<String> ids = this.docIds.get(indexName);
            if (ids.size() > 0) {
                String idsFilter = "\"must_not\":{\"ids\":{\"values\":[" + String.join(",", ids) + "]}}";
                resolversClause = "{\"bool\":{" + idsFilter + ",\"filter\":" + resolversClause + "}}";
            }
            if (this.profile)
                query = "{\"query\": " + resolversClause + ",\"size\": " + this.maxDocsPerQuery + ",\"profile\":true}";
            else
                query = "{\"query\": " + resolversClause + ",\"size\": " + this.maxDocsPerQuery + "}";

            // Submit query to Elasticsearch.
            SearchResponse response = this.search(indexName, query);

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
                if (this.docIds.get(indexName).contains(_id))
                    continue;
                this.docIds.get(indexName).add(_id);

                // Gather attributes from the doc. Store them in the "_attributes" field of the doc,
                // and include them in the attributes for subsequent queries.
                TreeMap<String, JsonNode> docAttributes = new TreeMap<>();
                for (String indexFieldName : this.model.indices().get(indexName).fields().keySet()) {
                    String attributeName = this.model.indices().get(indexName).fields().get(indexFieldName).attribute();
                    String attributeType = this.model.attributes().get(attributeName).type();
                    // The index field name might not refer to the _source property.
                    // If it's not in the _source, remove the last part of the index field name from the dot notation.
                    // Index field names can reference multi-fields, which are not returned in the _source.
                    if (!nextInputAttributes.containsKey(attributeName))
                        nextInputAttributes.put(attributeName, new HashSet<>());
                    if (!doc.get("_source").has(indexFieldName))
                        indexFieldName = indexFieldName.split("\\.")[0];
                    if (doc.get("_source").has(indexFieldName)) {
                        JsonNode valueNode = doc.get("_source").get(indexFieldName);
                        docAttributes.put(attributeName, valueNode);
                        Object value = Attribute.convertType(attributeType, valueNode);
                        nextInputAttributes.get(attributeName).add(value);
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
                        for (String attributeName : docAttributes.keySet()) {
                            JsonNode values = docAttributes.get(attributeName);
                            docAttributesObjNode.set(attributeName, values);
                        }
                    }

                    // Store doc in response.
                    this.hits.add(doc.toString());
                }
            }
        }

        // Stop traversing if we've reached max depth.
        boolean maxDepthReached = this.maxHops > -1 && this.hop >= this.maxHops;
        if (maxDepthReached)
            return;

        // Update input attributes for the next queries.
        for (String attributeName : nextInputAttributes.keySet()) {
            if (!this.inputAttributes.containsKey(attributeName))
                this.inputAttributes.put(attributeName, new HashSet<>());
            for (Object value : nextInputAttributes.get(attributeName)) {
                if (!this.inputAttributes.get(attributeName).contains(value)) {
                    this.inputAttributes.get(attributeName).add(value);
                    newHits = true;
                }
            }
        }

        // Stop traversing if there are no more attributes to query.
        if (!newHits)
            return;

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
    public String run() throws IOException, ValidationException {
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
                responseParts.add("\"queries\":[" + queries + "]");
            String response = "{" + String.join(",", responseParts) + "}";
            if (this.pretty)
                response = this.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(response));
            return response;

        } finally {
            this.ran = true;
        }
    }

}
