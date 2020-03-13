package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.Index;
import io.zentity.model.Matcher;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.Attribute;
import io.zentity.resolution.input.Input;
import io.zentity.resolution.input.Term;
import io.zentity.resolution.input.value.StringValue;
import io.zentity.resolution.input.value.Value;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static io.zentity.common.Patterns.COLON;

public class Job {

    // Constants
    public static final boolean DEFAULT_INCLUDE_ATTRIBUTES = true;
    public static final boolean DEFAULT_INCLUDE_ERROR_TRACE = true;
    public static final boolean DEFAULT_INCLUDE_EXPLANATION = false;
    public static final boolean DEFAULT_INCLUDE_HITS = true;
    public static final boolean DEFAULT_INCLUDE_QUERIES = false;
    public static final boolean DEFAULT_INCLUDE_SCORE = false;
    public static final boolean DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM = false;
    public static final boolean DEFAULT_INCLUDE_SOURCE = true;
    public static final boolean DEFAULT_INCLUDE_VERSION = false;
    public static final int DEFAULT_MAX_DOCS_PER_QUERY = 1000;
    public static final int DEFAULT_MAX_HOPS = 100;
    public static final String DEFAULT_MAX_TIME_PER_QUERY = "10s";
    public static final boolean DEFAULT_PRETTY = false;
    public static final boolean DEFAULT_PROFILE = false;

    // Constants (optional search parameters)
    public static Boolean DEFAULT_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS = null;
    public static Integer DEFAULT_SEARCH_BATCHED_REDUCE_SIZE = null;
    public static Integer DEFAULT_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS = null;
    public static Integer DEFAULT_SEARCH_PRE_FILTER_SHARD_SIZE = null;
    public static String DEFAULT_SEARCH_PREFERENCE = null;
    public static Boolean DEFAULT_SEARCH_REQUEST_CACHE = null;

    // Job configuration
    private Input input;
    private boolean includeAttributes = DEFAULT_INCLUDE_ATTRIBUTES;
    private boolean includeErrorTrace = DEFAULT_INCLUDE_ERROR_TRACE;
    private boolean includeExplanation = DEFAULT_INCLUDE_EXPLANATION;
    private boolean includeHits = DEFAULT_INCLUDE_HITS;
    private boolean includeQueries = DEFAULT_INCLUDE_QUERIES;
    private boolean includeScore = DEFAULT_INCLUDE_SCORE;
    private boolean includeSeqNoPrimaryTerm = DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM;
    private boolean includeSource = DEFAULT_INCLUDE_SOURCE;
    private boolean includeVersion = DEFAULT_INCLUDE_VERSION;
    private int maxDocsPerQuery = DEFAULT_MAX_DOCS_PER_QUERY;
    private int maxHops = DEFAULT_MAX_HOPS;
    private String maxTimePerQuery = DEFAULT_MAX_TIME_PER_QUERY;
    private boolean pretty = DEFAULT_PRETTY;
    private boolean profile = DEFAULT_PROFILE;

    // Job configuration (optional search parameters)
    private Boolean searchAllowPartialSearchResults = DEFAULT_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS;
    private Integer searchBatchedReduceSize = DEFAULT_SEARCH_BATCHED_REDUCE_SIZE;
    private Integer searchMaxConcurrentShardRequests = DEFAULT_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS;
    private Integer searchPreFilterShardSize = DEFAULT_SEARCH_PRE_FILTER_SHARD_SIZE;
    private String searchPreference = DEFAULT_SEARCH_PREFERENCE;
    private Boolean searchRequestCache = DEFAULT_SEARCH_REQUEST_CACHE;

    // Job state
    private Map<String, Attribute> attributes = new TreeMap<>();
    private NodeClient client;
    private Map<String, Set<String>> docIds = new TreeMap<>();
    private String error = null;
    private boolean failed = false;
    private List<String> hits = new ArrayList<>();
    private int hop = 0;
    private Set<String> missingIndices = new TreeSet<>();
    private List<String> queries = new ArrayList<>();
    private boolean ran = false;

    public Job(NodeClient client) {
        this.client = client;
    }

    public static String serializeException(Exception e, boolean includeErrorTrace) {
        List<String> errorParts = new ArrayList<>();
        if (e instanceof ElasticsearchException)
            errorParts.add("\"by\":\"elasticsearch\"");
        else
            errorParts.add("\"by\":\"zentity\"");
        errorParts.add("\"type\":\"" + e.getClass().getCanonicalName() + "\"");
        errorParts.add("\"reason\":\"" + e.getMessage() + "\"");
        if (includeErrorTrace) {
            StringWriter traceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(traceWriter));
            errorParts.add("\"stack_trace\":" + Json.quoteString(traceWriter.toString()) + "");
        }
        return String.join(",", errorParts);
    }

    public static String serializeLoggedQuery(Input input, int _hop, int _query, String indexName, String request, String response, List<String> resolvers, TreeMap<Integer, TreeMap<String, TreeMap>> resolversFilterTreeGrouped, List<String> termResolvers, TreeMap<String, TreeMap> termResolversFilterTree) throws JsonProcessingException {
        List<String> filtersLoggedList = new ArrayList<>();
        if (!resolvers.isEmpty() && !resolversFilterTreeGrouped.isEmpty()) {
            List<String> attributesResolversSummary = new ArrayList<>();
            for (String resolverName : resolvers) {
                List<String> resolversAttributes = new ArrayList<>();
                for (String attributeName : input.model().resolvers().get(resolverName).attributes())
                    resolversAttributes.add("\"" + attributeName + "\"");
                attributesResolversSummary.add("\"" + resolverName + "\":{\"attributes\":[" +  String.join(",", resolversAttributes) + "]}");
            }
            String attributesResolversFilterTreeLogged = Json.ORDERED_MAPPER.writeValueAsString(resolversFilterTreeGrouped);
            filtersLoggedList.add("\"attributes\":{\"tree\":" + attributesResolversFilterTreeLogged + ",\"resolvers\":{" + String.join(",", attributesResolversSummary) + "}}");
        } else {
            filtersLoggedList.add("\"attributes\":null");
        }
        if (!termResolvers.isEmpty() && !termResolversFilterTree.isEmpty()) {
            List<String> termsResolversSummary = new ArrayList<>();
            for (String resolverName : termResolvers) {
                List<String> resolverAttributes = new ArrayList<>();
                for (String attributeName : input.model().resolvers().get(resolverName).attributes())
                    resolverAttributes.add("\"" + attributeName + "\"");
                termsResolversSummary.add("\"" + resolverName + "\":{\"attributes\":[" +  String.join(",", resolverAttributes) + "]}");
            }
            String termResolversFilterTreeLogged = Json.ORDERED_MAPPER.writeValueAsString(termResolversFilterTree);
            filtersLoggedList.add("\"terms\":{\"tree\":{\"0\":" + termResolversFilterTreeLogged + "},\"resolvers\":{" + String.join(",", termsResolversSummary) + "}}");
        } else {
            filtersLoggedList.add("\"terms\":null");
        }
        String filtersLogged = String.join(",", filtersLoggedList);
        String searchLogged = "{\"request\":" + request + ",\"response\":" + response + "}";
        return "{\"_hop\":" + _hop + ",\"_query\":" + _query + ",\"_index\":\"" + indexName + "\",\"filters\":{" + filtersLogged + "},\"search\":" + searchLogged + "}";
    }

    public static String makeScriptFieldsClause(Input input, String indexName) throws ValidationException {
        List<String> scriptFieldClauses = new ArrayList<>();

        // Find any index fields that need to be included in the "script_fields" clause.
        // Currently this includes any index field that is associated with a "date" attribute,
        // which requires the "_source" value to be reformatted to a normalized format.
        Index index = input.model().indices().get(indexName);
        for (String attributeName : index.attributeIndexFieldsMap().keySet()) {
            switch (input.model().attributes().get(attributeName).type()) {
                case "date":

                    // Required params
                    String format;

                    // Make a "script" clause for each index field associated with this attribute.
                    for (String indexFieldName : index.attributeIndexFieldsMap().get(attributeName).keySet()) {
                        // Check if the required params are defined in the input attribute.
                        if (input.attributes().containsKey(attributeName) && input.attributes().get(attributeName).params().containsKey("format") && !input.attributes().get(attributeName).params().get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(input.attributes().get(attributeName).params().get("format")).matches()) {
                            format = input.attributes().get(attributeName).params().get("format");
                        } else {
                            // Otherwise check if the required params are defined in the model attribute.
                            Map<String, String> params = input.model().attributes().get(attributeName).params();
                            if (params.containsKey("format") && !params.get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                format = params.get("format");
                            } else {
                                // Otherwise check if the required params are defined in the matcher associated with the index field.
                                String matcherName = index.attributeIndexFieldsMap().get(attributeName).get(indexFieldName).matcher();
                                params = input.model().matchers().get(matcherName).params();
                                if (params.containsKey("format") && !params.get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                    format = params.get("format");
                                } else {
                                    // If we've gotten this far, that means that the required params for this attribute type
                                    // haven't been specified in any valid places.
                                    throw new ValidationException("'attributes." + attributeName + "' is a 'date' which required a 'format' to be specified in the params.");
                                }
                            }
                        }

                        // Make the "script" clause
                        String scriptSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
                        String scriptParams = "\"field\":\"" + indexFieldName + "\",\"format\":\"" + format + "\"";
                        String scriptFieldClause = "\"" + indexFieldName + "\":{\"script\":{\"lang\":\"painless\",\"source\":\"" + scriptSource + "\",\"params\":{" + scriptParams + "}}}";
                        scriptFieldClauses.add(scriptFieldClause);
                    }
                    break;

                default:
                    break;
            }
        }
        if (scriptFieldClauses.isEmpty())
            return null;
        return "\"script_fields\":{" + String.join(",", scriptFieldClauses) + "}";
    }

    /**
     * Determine if a field of an index has a matcher associated with that field.
     *
     * @param model          The entity model.
     * @param indexName      The name of the index to reference in the entity model.
     * @param indexFieldName The name of the index field to reference in the index.
     * @return Boolean decision.
     */
    public static boolean indexFieldHasMatcher(Model model, String indexName, String indexFieldName) {
        String matcherName = model.indices().get(indexName).fields().get(indexFieldName).matcher();
        if (matcherName == null)
            return false;
        if (model.matchers().get(matcherName) == null)
            return false;
        return true;
    }

    /**
     * Determine if we can construct a query for a given resolver on a given index with a given input.
     * Each attribute of the resolver must be mapped to a field of the index and have a matcher defined for it.
     *
     * @param model        The entity model.
     * @param indexName    The name of the index to reference in the entity model.
     * @param resolverName The name of the resolver to reference in the entity model.
     * @param attributes   The values for the input attributes.
     * @return Boolean decision.
     */
    public static boolean canQueryResolver(Model model, String indexName, String resolverName, Map<String, Attribute> attributes) {

        // Each attribute of the resolver must pass these conditions:
        for (String attributeName : model.resolvers().get(resolverName).attributes()) {

            // The input must have the attribute.
            if (!attributes.containsKey(attributeName))
                return false;

            // The input must have at least one value for the attribute.
            if (attributes.get(attributeName).values().isEmpty())
                return false;

            // The index must have at least one index field mapped to the attribute.
            if (!model.indices().get(indexName).attributeIndexFieldsMap().containsKey(attributeName))
                return false;
            if (model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).isEmpty())
                return false;

            // The index field must have a matcher defined for it.
            boolean hasMatcher = false;
            for (String indexFieldName : model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).keySet()) {
                if (indexFieldHasMatcher(model, indexName, indexFieldName)) {
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
     * Given a clause from the "matchers" field of an entity model, replace the {{ field }} and {{ value }} variables
     * and arbitrary parameters. If a parameter exists, the value
     *
     * @param matcher        The matcher object.
     * @param indexFieldName The name of the index field to populate in the clause.
     * @param value          The value of the attribute to populate in the clause.
     * @param params         The values of the parameters (if any) to pass to the matcher.
     * @return A "bool" clause that references the desired field and value.
     */
    public static String populateMatcherClause(Matcher matcher, String indexFieldName, String value, Map<String, String> params) throws ValidationException {
        String matcherClause = matcher.clause();
        for (String variable : matcher.variables().keySet()) {
            Pattern pattern = matcher.variables().get(variable);
            switch (variable) {
                case "field":
                    matcherClause = pattern.matcher(matcherClause).replaceAll(indexFieldName);
                    break;
                case "value":
                    matcherClause = pattern.matcher(matcherClause).replaceAll(value);
                    break;
                default:
                    java.util.regex.Matcher m = Patterns.VARIABLE_PARAMS.matcher(variable);
                    if (m.find()) {
                        String var = m.group(1);
                        if (!params.containsKey(var))
                            throw new ValidationException("'matchers." + matcher.name() + "' was given no value for '{{ " + variable + " }}'");
                        String paramValue = params.get(var);
                        matcherClause = pattern.matcher(matcherClause).replaceAll(paramValue);
                    }
                    break;
            }
        }
        return matcherClause;
    }

    /**
     * Given an entity model, an index name, a set of attribute values, and an attribute name,
     * find all index field names that are mapped to the attribute name and populate their matcher clauses.
     *
     * @param model         The entity model.
     * @param indexName     The name of the index to reference in the entity model.
     * @param attributes    The names and values of the input attributes.
     * @param attributeName The name of the attribute to reference in the attributeSet.
     * @param combiner      Combine clauses with "should" or "filter".
     * @return
     */
    public static List<String> makeIndexFieldClauses(Model model, String indexName, Map<String, Attribute> attributes, String attributeName, String combiner, boolean namedFilters, AtomicInteger _nameIdCounter) throws ValidationException {
        if (!combiner.equals("should") && !combiner.equals("filter"))
            throw new ValidationException("'" + combiner + "' is not a supported clause combiner.");
        List<String> indexFieldClauses = new ArrayList<>();
        for (String indexFieldName : model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).keySet()) {

            // Can we use this index field?
            if (!indexFieldHasMatcher(model, indexName, indexFieldName))
                continue;

            // Construct a clause for each input value for this attribute.
            String matcherName = model.indices().get(indexName).fields().get(indexFieldName).matcher();
            Matcher matcher = model.matchers().get(matcherName);
            List<String> valueClauses = new ArrayList<>();
            Attribute attribute = attributes.get(attributeName);

            // Determine which values to pass to the matcher parameters.
            // Order of precedence:
            //  - Input attribute params override model attribute params
            //  - Model attribute params override matcher attribute params
            Map<String, String> params = new TreeMap<>();
            params.putAll(matcher.params());
            params.putAll(model.attributes().get(attributeName).params());
            params.putAll(attributes.get(attributeName).params());

            for (Value value : attribute.values()) {

                // Skip value if it's blank.
                if (value.serialized() == null || value.serialized().equals(""))
                    continue;

                // Populate the {{ field }}, {{ value }}, and {{ param.* }} variables of the matcher template.
                String valueClause = populateMatcherClause(matcher, indexFieldName, value.serialized(), params);
                if (namedFilters) {

                    // Name the clause to determine why any matching document matched
                    String valueBase64 = Base64.getEncoder().encodeToString(value.serialized().getBytes());
                    String _name = attributeName + ":" + indexFieldName + ":" + matcherName + ":" + valueBase64 + ":" + _nameIdCounter.getAndIncrement();
                    valueClause = "{\"bool\":{\"_name\":\"" + _name + "\",\"filter\":" + valueClause + "}}";
                }
                valueClauses.add(valueClause);
            }
            if (valueClauses.size() == 0)
                continue;

            // Combine each value clause into a single "should" or "filter" clause.
            String valuesClause;
            if (valueClauses.size() > 1)
                valuesClause = "{\"bool\":{\"" + combiner + "\":[" + String.join(",", valueClauses) + "]}}";
            else
                valuesClause = valueClauses.get(0);
            indexFieldClauses.add(valuesClause);
        }
        return indexFieldClauses;
    }

    /**
     * Given an entity model, an index name, and a set of attribute values,
     * for each attribute name in the set of attributes, find all index field names that are mapped to the attribute
     * name and populate their matcher clauses.
     *
     * @param model      The entity model.
     * @param indexName  The name of the index to reference in the entity model.
     * @param attributes The names and values of the input attributes.
     * @param combiner   Combine clauses with "should" or "filter".
     * @return
     */
    public static List<String> makeAttributeClauses(Model model, String indexName, Map<String, Attribute> attributes, String combiner, boolean namedFilters, AtomicInteger _nameIdCounter) throws ValidationException {
        if (!combiner.equals("should") && !combiner.equals("filter"))
            throw new ValidationException("'" + combiner + "' is not a supported clause combiner.");
        List<String> attributeClauses = new ArrayList<>();
        for (String attributeName : attributes.keySet()) {

            // Construct a "should" or "filter" clause for each index field mapped to this attribute.
            List<String> indexFieldClauses = makeIndexFieldClauses(model, indexName, attributes, attributeName, combiner, namedFilters, _nameIdCounter);
            if (indexFieldClauses.size() == 0)
                continue;

            // Combine each matcher clause into a single "should" or "filter" clause.
            String indexFieldsClause;
            if (indexFieldClauses.size() > 1)
                indexFieldsClause = "{\"bool\":{\"" + combiner + "\":[" + String.join(",", indexFieldClauses) + "]}}";
            else
                indexFieldsClause = indexFieldClauses.get(0);
            attributeClauses.add(indexFieldsClause);
        }
        return attributeClauses;
    }

    /**
     * Populate the field names and values of the resolver clause of a query.
     *
     * @param model               The entity model.
     * @param indexName           The name of the index to reference in the entity model.
     * @param resolversFilterTree The filter tree for the resolvers to be queried.
     * @param attributes          The names and values for the input attributes.
     * @return A "bool" clause for all applicable resolvers.
     */
    public static String populateResolversFilterTree(Model model, String indexName, TreeMap<String, TreeMap> resolversFilterTree, Map<String, Attribute> attributes, boolean namedFilters, AtomicInteger _nameIdCounter) throws ValidationException {

        // Construct a "filter" clause for each attribute at this level of the filter tree.
        List<String> attributeClauses = new ArrayList<>();
        for (String attributeName : resolversFilterTree.keySet()) {

            // Construct a "should" clause for each index field mapped to this attribute.
            List<String> indexFieldClauses = makeIndexFieldClauses(model, indexName, attributes, attributeName, "should", namedFilters, _nameIdCounter);
            if (indexFieldClauses.size() == 0)
                continue;

            // Combine multiple matcher clauses into a single "should" clause.
            String indexFieldsClause;
            if (indexFieldClauses.size() > 1)
                indexFieldsClause = "{\"bool\":{\"should\":[" + String.join(",", indexFieldClauses) + "]}}";
            else
                indexFieldsClause = indexFieldClauses.get(0);

            // Populate any child filters.
            String filter = populateResolversFilterTree(model, indexName, resolversFilterTree.get(attributeName), attributes, namedFilters, _nameIdCounter);
            if (!filter.isEmpty())
                attributeClauses.add("{\"bool\":{\"filter\":[" + indexFieldsClause + "," + filter + "]}}");
            else
                attributeClauses.add(indexFieldsClause);

        }

        // Combine each attribute clause into a single "should" clause.
        int size = attributeClauses.size();
        if (size > 1)
            return "{\"bool\":{\"should\":[" + String.join(",", attributeClauses) + "]}}";
        else if (size == 1)
            return attributeClauses.get(0);
        else
            return "";
    }

    /**
     * Reorganize the attributes of all resolvers into a tree of Maps.
     *
     * @param resolversSorted The attributes for each resolver. Attributes are sorted first by priority and then lexicographically.
     * @return The attributes of all applicable resolvers nested in a tree.
     */
    public static TreeMap<String, TreeMap> makeResolversFilterTree(List<List<String>> resolversSorted) {
        TreeMap<String, TreeMap> filterTree = new TreeMap<>();
        filterTree.put("root", new TreeMap<>());
        for (List<String> resolverSorted : resolversSorted) {
            TreeMap<String, TreeMap> current = filterTree.get("root");
            for (String attributeName : resolverSorted) {
                if (!current.containsKey(attributeName))
                    current.put(attributeName, new TreeMap<String, TreeMap>());
                current = current.get(attributeName);
            }
        }
        return filterTree.get("root");
    }

    /**
     * Sort the attributes of each resolver in descending order by how many resolvers each attribute appears in,
     * and secondarily in ascending order by the name of the attribute.
     *
     * @param model     The entity model.
     * @param resolvers The names of the resolvers.
     * @param counts    For each attribute, the number of resolvers it appears in.
     * @return For each resolver, a list of attributes sorted first by priority and then lexicographically.
     */
    public static List<List<String>> sortResolverAttributes(Model model, List<String> resolvers, Map<String, Integer> counts) {
        List<List<String>> resolversSorted = new ArrayList<>();
        for (String resolverName : resolvers) {
            List<String> resolverSorted = new ArrayList<>();
            Map<Integer, TreeSet<String>> attributeGroups = new TreeMap<>();
            for (String attributeName : model.resolvers().get(resolverName).attributes()) {
                int count = counts.get(attributeName);
                if (!attributeGroups.containsKey(count))
                    attributeGroups.put(count, new TreeSet<>());
                attributeGroups.get(count).add(attributeName);
            }
            TreeSet<Integer> countsKeys = new TreeSet<>(Collections.reverseOrder());
            countsKeys.addAll(attributeGroups.keySet());
            for (int count : countsKeys)
                for (String attributeName : attributeGroups.get(count))
                    resolverSorted.add(attributeName);
            resolversSorted.add(resolverSorted);
        }
        return resolversSorted;
    }

    /**
     * Count how many resolvers each attribute appears in.
     * Attributes that appear in more resolvers should be higher in the query tree.
     *
     * @param model     The entity model.
     * @param resolvers The names of the resolvers to reference in the entity model.
     * @return For each attribute, the number of resolvers it appears in.
     */
    public static Map<String, Integer> countAttributesAcrossResolvers(Model model, List<String> resolvers) {
        Map<String, Integer> counts = new TreeMap<>();
        for (String resolverName : resolvers)
            for (String attributeName : model.resolvers().get(resolverName).attributes())
                counts.put(attributeName, counts.getOrDefault(attributeName, 0) + 1);
        return counts;
    }

    /**
     * Group resolvers by their level of weight.
     *
     * @param model The entity model.
     * @param resolvers The names of the resolvers to reference in the entity model.
     * @return For each weight level, the names of the resolvers in that weight level.
     */
    public static TreeMap<Integer, List<String>> groupResolversByWeight(Model model, List<String> resolvers) {
        TreeMap<Integer, List<String>> resolverGroups = new TreeMap<>();
        for (String resolverName : resolvers) {
            Integer weight = model.resolvers().get(resolverName).weight();
            if (!resolverGroups.containsKey(weight))
                resolverGroups.put(weight, new ArrayList<>());
            resolverGroups.get(weight).add(resolverName);
        }
        return resolverGroups;
    }

    /**
     * Resets the variables that hold the state of the job, in case the same Job object is reused.
     */
    private void resetState() {
        this.attributes = new TreeMap<>(this.input().attributes());
        this.docIds = new TreeMap<>();
        this.error = null;
        this.failed = false;
        this.hits = new ArrayList<>();
        this.hop = 0;
        this.missingIndices = new TreeSet<>();
        this.queries = new ArrayList<>();
        this.ran = false;
    }

    public boolean includeAttributes() {
        return this.includeAttributes;
    }

    public void includeAttributes(boolean includeAttributes) {
        this.includeAttributes = includeAttributes;
    }

    public boolean includeErrorTrace() {
        return this.includeErrorTrace;
    }

    public void includeErrorTrace(boolean includeErrorTrace) {
        this.includeErrorTrace = includeErrorTrace;
    }

    public boolean includeExplanation() {
        return this.includeExplanation;
    }

    public void includeExplanation(boolean includeExplanation) {
        this.includeExplanation = includeExplanation;
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

    public Boolean includeScore() {
        return this.includeScore;
    }

    public void includeScore(Boolean includeScore) { this.includeScore = includeScore; }

    public Boolean includeSeqNoPrimaryTerm() {
        return this.includeSeqNoPrimaryTerm;
    }

    public void includeSeqNoPrimaryTerm(Boolean includeSeqNoPrimaryTerm) { this.includeSeqNoPrimaryTerm = includeSeqNoPrimaryTerm; }

    public boolean includeSource() {
        return this.includeSource;
    }

    public void includeSource(boolean includeSource) {
        this.includeSource = includeSource;
    }

    public Boolean includeVersion() { return this.includeVersion;  }

    public void includeVersion(Boolean includeVersion) { this.includeVersion = includeVersion; }

    public int maxDocsPerQuery() {
        return this.maxDocsPerQuery;
    }

    public void maxDocsPerQuery(int maxDocsPerQuery) {
        this.maxDocsPerQuery = maxDocsPerQuery;
    }

    public int maxHops() {
        return this.maxHops;
    }

    public void maxHops(int maxHops) {
        this.maxHops = maxHops;
    }

    public String maxTimePerQuery() { return this.maxTimePerQuery; }

    public void maxTimePerQuery(String maxTimePerQuery) { this.maxTimePerQuery = maxTimePerQuery; }

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

    public Boolean searchAllowPartialSearchResults() {
        return this.searchAllowPartialSearchResults;
    }

    public void searchAllowPartialSearchResults(Boolean searchAllowPartialSearchResults) { this.searchAllowPartialSearchResults = searchAllowPartialSearchResults; }

    public Integer searchBatchedReduceSize() {
        return this.searchBatchedReduceSize;
    }

    public void searchBatchedReduceSize(Integer searchBatchedReduceSize) { this.searchBatchedReduceSize = searchBatchedReduceSize; }

    public Integer searchMaxConcurrentShardRequests() {
        return this.searchMaxConcurrentShardRequests;
    }

    public void searchMaxConcurrentShardRequests(Integer searchMaxConcurrentShardRequests) { this.searchMaxConcurrentShardRequests = searchMaxConcurrentShardRequests; }

    public Integer searchPreFilterShardSize() {
        return this.searchPreFilterShardSize;
    }

    public void searchPreFilterShardSize(Integer searchPreFilterShardSize) { this.searchPreFilterShardSize = searchPreFilterShardSize; }

    public String searchPreference() {
        return this.searchPreference;
    }

    public void searchPreference(String searchPreference) { this.searchPreference = searchPreference; }
    public Boolean searchRequestCache() {
        return this.searchRequestCache;
    }

    public void searchRequestCache(Boolean searchRequestCache) { this.searchRequestCache = searchRequestCache; }

    public Input input() {
        return this.input;
    }

    public void input(Input input) {
        this.input = input;
    }

    public boolean failed() {
        return this.failed;
    }

    public boolean ran() {
        return this.ran;
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
                .getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
            searchSourceBuilder.parseXContent(parser);
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName).setSource(searchSourceBuilder);
        if (this.searchAllowPartialSearchResults != null)
            searchRequestBuilder.setAllowPartialSearchResults(this.searchAllowPartialSearchResults);
        if (this.searchBatchedReduceSize != null)
            searchRequestBuilder.setBatchedReduceSize(this.searchBatchedReduceSize);
        if (this.searchMaxConcurrentShardRequests != null)
            searchRequestBuilder.setMaxConcurrentShardRequests(this.searchMaxConcurrentShardRequests);
        if (this.searchPreFilterShardSize != null)
            searchRequestBuilder.setPreFilterShardSize(this.searchPreFilterShardSize);
        if (this.searchPreference != null)
            searchRequestBuilder.setPreference(this.searchPreference);
        if (this.searchRequestCache != null)
            searchRequestBuilder.setRequestCache(this.searchRequestCache);
        if (this.maxTimePerQuery != null)
            searchRequestBuilder.setTimeout(TimeValue.parseTimeValue(this.maxTimePerQuery, "timeout"));
        return searchRequestBuilder.execute().actionGet();
    }

    /**
     * Given a set of attribute values, determine which queries to submit to which indices then submit them and recurse.
     *
     * @throws IOException
     * @throws ValidationException
     */
    private void traverse() throws IOException, ValidationException {

        // Prepare to collect attributes from the results of these queries as the inputs to subsequent queries.
        Map<String, Attribute> nextInputAttributes = new TreeMap<>();
        Boolean newHits = false;
        Boolean namedFilters = this.includeExplanation || this.includeScore;
        int _query = 0;

        // Construct a query for each index that maps to a resolver.
        for (String indexName : this.input.model().indices().keySet()) {

            // Skip this index if a prior hop determined the index to be missing.
            if (this.missingIndices.contains(indexName))
                continue;

            // Track _ids for this index.
            if (!this.docIds.containsKey(indexName))
                this.docIds.put(indexName, new TreeSet<>());

            // "_explanation" uses named queries, and each value of the "_name" fields must be unique.
            // Use a counter to prepend a unique and deterministic identifier for each "_name" field in the query.
            AtomicInteger _nameIdCounter = new AtomicInteger();

            // Determine which resolvers can be queried for this index.
            List<String> resolvers = new ArrayList<>();
            for (String resolverName : this.input.model().resolvers().keySet())
                if (canQueryResolver(this.input.model(), indexName, resolverName, this.attributes))
                    resolvers.add(resolverName);

            // Determine if we can query this index.
            boolean canQueryIds = this.hop == 0 && this.input().ids().containsKey(indexName) && !this.input().ids().get(indexName).isEmpty();
            boolean canQueryTerms = this.hop == 0 && !this.input.terms().isEmpty();
            boolean canQueryAttributes = resolvers.size() > 0;
            if (!canQueryAttributes && !canQueryIds && !canQueryTerms)
                continue;

            // Construct query for this index.
            String query;
            String queryClause;
            List<String> queryMustNotClauses = new ArrayList<>();
            String queryMustNotClause = "";
            List<String> queryFilterClauses = new ArrayList<>();
            String queryFilterClause = "";
            List<String> topLevelClauses = new ArrayList<>();
            topLevelClauses.add("\"_source\":true");

            // Exclude docs by _id
            Set<String> docIds = this.docIds.get(indexName);
            if (!docIds.isEmpty())
                queryMustNotClauses.add("{\"ids\":{\"values\":[" + String.join(",", docIds) + "]}}");

            // Create "scope.exclude.attributes" clauses. Combine them into a single "should" clause.
            if (!this.input.scope().exclude().attributes().isEmpty()) {
                List<String> attributeClauses = makeAttributeClauses(this.input.model(), indexName, this.input.scope().exclude().attributes(), "should", namedFilters, _nameIdCounter);
                int size = attributeClauses.size();
                if (size > 1)
                    queryMustNotClauses.add("{\"bool\":{\"should\":[" + String.join(",", attributeClauses) + "]}}");
                else if (size == 1)
                    queryMustNotClauses.add(attributeClauses.get(0));
            }

            // Construct the top-level "must_not" clause.
            if (queryMustNotClauses.size() > 1)
                queryMustNotClause = "\"must_not\":[" + String.join(",", queryMustNotClauses) + "]";
            else if (queryMustNotClauses.size() == 1)
                queryMustNotClause = "\"must_not\":" + queryMustNotClauses.get(0);

            // Construct "scope.include.attributes" clauses. Combine them into a single "filter" clause.
            if (!this.input.scope().include().attributes().isEmpty()) {
                List<String> attributeClauses = makeAttributeClauses(this.input.model(), indexName, this.input.scope().include().attributes(), "filter", namedFilters, _nameIdCounter);
                int size = attributeClauses.size();
                if (size > 1)
                    queryFilterClauses.add("{\"bool\":{\"filter\":[" + String.join(",", attributeClauses) + "]}}");
                else if (size == 1)
                    queryFilterClauses.add(attributeClauses.get(0));
            }

            // Construct the "ids" clause if this is the first hop and if any ids are specified for this index.
            String idsClause = "";
            if (canQueryIds) {
                Set<String> ids = this.input().ids().get(indexName);
                idsClause = "{\"bool\":{\"filter\":[{\"ids\":{\"values\":[" + String.join(",", ids) + "]}}]}}";
            }

            // Construct the resolvers clause for attribute values.
            String resolversClause = "";
            TreeMap<String, TreeMap> resolversFilterTree;
            TreeMap<Integer, TreeMap<String, TreeMap>> resolversFilterTreeGrouped = new TreeMap<>(Collections.reverseOrder());
            if (!this.attributes.isEmpty()) {

                // Group the resolvers by their weight level.
                TreeMap<Integer, List<String>> resolverGroups = groupResolversByWeight(this.input.model(), resolvers);

                // Construct a clause for each weight level in descending order of weight.
                List<Integer> weights = new ArrayList<>(resolverGroups.keySet());
                Collections.reverse(weights);
                int numWeightLevels = weights.size();
                for (int level = 0; level < numWeightLevels; level++) {
                    Integer weight = weights.get(level);
                    List<String> resolversGroup = resolverGroups.get(weight);
                    Map<String, Integer> counts = countAttributesAcrossResolvers(this.input.model(), resolversGroup);
                    List<List<String>> resolversSorted = sortResolverAttributes(this.input.model(), resolversGroup, counts);
                    resolversFilterTree = makeResolversFilterTree(resolversSorted);
                    resolversFilterTreeGrouped.put(numWeightLevels - level - 1, resolversFilterTree);
                    resolversClause = populateResolversFilterTree(this.input.model(), indexName, resolversFilterTree, this.attributes, namedFilters, _nameIdCounter);

                    // If there are multiple levels of weight, then each lower weight group of resolvers must ensure
                    // that every higher weight resolver either matches or does not exist.
                    List<String> parentResolversClauses = new ArrayList<>();
                    if (level > 0) {

                        // This is a lower weight group of resolvers.
                        // Every higher weight resolver either must match or must not exist.
                        for (int parentLevel = 0; parentLevel < level; parentLevel++) {
                            Integer parentWeight = weights.get(parentLevel);
                            List<String> parentResolversGroup = resolverGroups.get(parentWeight);
                            List<String> parentResolverClauses = new ArrayList<>();
                            for (String parentResolverName : parentResolversGroup) {

                                // Construct a clause that checks if any attribute of the resolver does not exist.
                                List<String> attributeExistsClauses = new ArrayList<>();
                                for (String attributeName : this.input.model().resolvers().get(parentResolverName).attributes())
                                    attributeExistsClauses.add("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"" + attributeName + "\"}}}}");
                                String attributesExistsClause = "";
                                if (attributeExistsClauses.size() > 1)
                                    attributesExistsClause = "{\"bool\":{\"should\":[" + String.join(",", attributeExistsClauses) + "]}}";
                                else if (attributeExistsClauses.size() == 1)
                                    attributesExistsClause = attributeExistsClauses.get(0);

                                // Construct a clause for the resolver.
                                List<String> parentResolverGroup = new ArrayList<>(Arrays.asList(parentResolverName));
                                Map<String, Integer> parentCounts = countAttributesAcrossResolvers(this.input.model(), parentResolverGroup);
                                List<List<String>> parentResolverSorted = sortResolverAttributes(this.input.model(), parentResolverGroup, parentCounts);
                                TreeMap<String, TreeMap> parentResolverFilterTree = makeResolversFilterTree(parentResolverSorted);
                                String parentResolverClause = populateResolversFilterTree(this.input.model(), indexName, parentResolverFilterTree, this.attributes, namedFilters, _nameIdCounter);

                                // Construct a "should" clause for the above two clauses.
                                parentResolverClauses.add("{\"bool\":{\"should\":[" + attributesExistsClause + "," + parentResolverClause + "]}}");
                            }
                            if (parentResolverClauses.size() > 1)
                                parentResolversClauses.add("{\"bool\":{\"filter\":[" + String.join(",", parentResolverClauses) + "]}}");
                            else if (parentResolverClauses.size() == 1)
                                parentResolversClauses.add(parentResolverClauses.get(0));
                        }
                    }

                    // Combine the resolvers clause and parent resolvers clause in a "filter" query if necessary.
                    if (parentResolversClauses.size() > 0)
                        resolversClause = "{\"bool\":{\"filter\":[" + resolversClause + "," + String.join(",", parentResolversClauses) + "]}}";
                }
            }

            // Construct the resolvers clause for any terms in the first hop.
            // Convert each term into each attribute value that matches its type.
            // Don't tier the resolvers by weights. Weights should be used only when the attribute values are certain.
            // In this case, terms are not certain to be attribute values of the entity until they match,
            // unlike structured attribute search where the attributes are assumed be known.
            List<String> termResolvers = new ArrayList<>();
            TreeMap<String, TreeMap> termResolversFilterTree = new TreeMap<>();
            if (canQueryTerms) {
                String termResolversClause = "";

                // Get the names of each attribute of each in-scope resolver.
                TreeSet<String> resolverAttributes = new TreeSet<>();
                for (String resolverName : this.input().model().resolvers().keySet())
                    resolverAttributes.addAll(this.input().model().resolvers().get(resolverName).attributes());

                // For each attribute, attempt to convert each term to a value of that attribute.
                // If the term does not match the attribute type, or if the term cannot be converted to a value
                // of that attribute, then skip the term and move on.
                //
                // Date attributes will require a format, but the format could be declared in the input attributes,
                // the model attributes, or the model matchers in descending order of precedence. If the pa
                Map<String, TreeSet<Value>> termValues = new TreeMap<>();
                for (String attributeName : resolverAttributes) {
                    String attributeType = this.input().model().attributes().get(attributeName).type();
                    for (Term term : this.input().terms()) {
                        try {
                            switch (attributeType) {
                                case "boolean":
                                    if (term.isBoolean()) {
                                        termValues.putIfAbsent(attributeName, new TreeSet<>());
                                        termValues.get(attributeName).add(term.booleanValue());
                                    }
                                    break;
                                case "date":
                                    // Determine which date format to use to parse the term.
                                    Index index = this.input.model().indices().get(indexName);
                                    // Check if the "format" param is defined in the input attribute.
                                    if (this.input.attributes().containsKey(attributeName) && this.input.attributes().get(attributeName).params().containsKey("format") && !this.input.attributes().get(attributeName).params().get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(this.input.attributes().get(attributeName).params().get("format")).matches()) {
                                        String format = this.input.attributes().get(attributeName).params().get("format");
                                        if (term.isDate(format)) {
                                            termValues.putIfAbsent(attributeName, new TreeSet<>());
                                            termValues.get(attributeName).add(term.dateValue());
                                        }
                                    } else {
                                        // Otherwise check if the "format" param is defined in the model attribute.
                                        Map<String, String> params = this.input.model().attributes().get(attributeName).params();
                                        if (params.containsKey("format") && !params.get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                            String format = params.get("format");
                                            if (term.isDate(format)) {
                                                termValues.putIfAbsent(attributeName, new TreeSet<>());
                                                termValues.get(attributeName).add(term.dateValue());
                                            }
                                        } else {
                                            // Otherwise check if the "format" param is defined in the matcher
                                            // associated with any index field associated with the attribute.
                                            // Add any date values that successfully parse.
                                            for (String indexFieldName : index.attributeIndexFieldsMap().get(attributeName).keySet()) {
                                                String matcherName = index.attributeIndexFieldsMap().get(attributeName).get(indexFieldName).matcher();
                                                params = input.model().matchers().get(matcherName).params();
                                                if (params.containsKey("format") && !params.get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                                    String format = params.get("format");
                                                    if (term.isDate(format)) {
                                                        termValues.putIfAbsent(attributeName, new TreeSet<>());
                                                        termValues.get(attributeName).add(term.dateValue());
                                                    }
                                                } else {
                                                    // If we've gotten this far, then this term can't be converted
                                                    // to a date value. Skip it and move on.
                                                }
                                            }
                                        }
                                    }
                                    break;
                                case "number":
                                    if (term.isNumber()) {
                                        termValues.putIfAbsent(attributeName, new TreeSet<>());
                                        termValues.get(attributeName).add(term.numberValue());
                                    }
                                    break;
                                case "string":
                                    termValues.putIfAbsent(attributeName, new TreeSet<>());
                                    termValues.get(attributeName).add(term.stringValue());
                                    break;
                                default:
                                    break;
                            }
                        } catch (ValidationException | IOException e) {
                            // continue;
                        }
                    }
                }

                // Include any known attribute values in this clause.
                // This is necessary if a request has both "attributes" and "terms".
                if (!this.attributes.isEmpty()) {
                    for (String attributeName : this.attributes.keySet()) {
                        for (Value value : this.attributes.get(attributeName).values()) {
                            termValues.putIfAbsent(attributeName, new TreeSet<>());
                            termValues.get(attributeName).add(value);
                        }
                    }
                }

                // Convert the values as if it was an input Attribute.
                Map<String, Attribute> termAttributes = new TreeMap<>();
                for (String attributeName : termValues.keySet()) {
                    String attributeType = this.input().model().attributes().get(attributeName).type();
                    List<String> jsonValues = new ArrayList<>();
                    for (Value value : termValues.get(attributeName)) {
                        if (value instanceof StringValue)
                            jsonValues.add(Json.quoteString(value.serialized()));
                        else
                            jsonValues.add(value.serialized());
                    }
                    // Pass params from the input "attributes" if any were defined.
                    String attributesJson;
                    if (this.input.attributes().containsKey(attributeName) && !this.input.attributes().get(attributeName).params().isEmpty()) {
                        Set<String> params = new TreeSet<>();
                        for (String paramName : this.input.attributes().get(attributeName).params().keySet()) {
                            String paramValue = this.input.attributes().get(attributeName).params().get(paramName);
                            params.add("\"" + paramName + "\":" + "\"" + paramValue + "\"");
                        }
                        String paramsJson = "{" + String.join(",", params) + "}";
                        attributesJson = "{\"values\":[" + String.join(",", jsonValues) + "],\"params\":" + paramsJson + "}";
                    } else {
                        attributesJson = "{\"values\":[" + String.join(",", jsonValues) + "]}";
                    }
                    termAttributes.put(attributeName, new Attribute(attributeName, attributeType, attributesJson));
                }

                // Determine which resolvers can be queried for this index using these attributes.
                for (String resolverName : this.input.model().resolvers().keySet())
                    if (canQueryResolver(this.input.model(), indexName, resolverName, termAttributes))
                        termResolvers.add(resolverName);

                // Construct the resolvers clause for term attribute values.
                if (termResolvers.size() > 0) {
                    Map<String, Integer> counts = countAttributesAcrossResolvers(this.input.model(), termResolvers);
                    List<List<String>> termResolversSorted = sortResolverAttributes(this.input.model(), termResolvers, counts);
                    termResolversFilterTree = makeResolversFilterTree(termResolversSorted);
                    termResolversClause = populateResolversFilterTree(this.input.model(), indexName, termResolversFilterTree, termAttributes, namedFilters, _nameIdCounter);
                }

                // Combine the two resolvers clauses in a "filter" clause if both exist.
                // If only the termResolversClause exists, set resolversClause to termResolversClause.
                // If neither clause exists, do nothing because resolversClause already does not exist.
                if (!resolversClause.isEmpty() && !termResolversClause.isEmpty())
                    queryFilterClauses.add("{\"bool\":{\"filter\":[" + resolversClause + "," + termResolversClause + "]}}");
                else if (!termResolversClause.isEmpty())
                    resolversClause = termResolversClause;
            }

            // Combine the ids clause and resolvers clause in a "should" clause if necessary.
            if (!idsClause.isEmpty() && !resolversClause.isEmpty())
                queryFilterClauses.add("{\"bool\":{\"should\":[" + idsClause + "," + resolversClause + "]}}");
            else if (!idsClause.isEmpty())
                queryFilterClauses.add(idsClause);
            else if (!resolversClause.isEmpty())
                queryFilterClauses.add(resolversClause);

            // Construct the "query" clause.
            if (!queryMustNotClause.isEmpty() && queryFilterClauses.size() > 0) {

                // Construct the top-level "filter" clause. Combine this clause and the top-level "must_not" clause
                // in a "bool" clause and add it to the "query" field.
                if (queryFilterClauses.size() > 1)
                    queryFilterClause = "\"filter\":[" + String.join(",", queryFilterClauses) + "]";
                else
                    queryFilterClause = "\"filter\":" + queryFilterClauses.get(0);
                queryClause = "\"query\":{\"bool\":{" + queryMustNotClause + "," + queryFilterClause + "}}";

            } else if (!queryMustNotClause.isEmpty()) {

                // Wrap only the top-level "must_not" clause in a "bool" clause and add it to the "query" field.
                queryClause = "\"query\":{\"bool\":{" + queryMustNotClause + "}}";

            } else if (queryFilterClauses.size() > 0) {

                // Construct the top-level "filter" clause and add only this clause to the "query" field.
                // This prevents a redundant "bool"."filter" wrapper clause when the top-level "must_not" clause
                // does not exist.
                if (queryFilterClauses.size() > 1)
                    queryFilterClause = "{\"bool\":{\"filter\":[" + String.join(",", queryFilterClauses) + "]}}";
                else
                    queryFilterClause = queryFilterClauses.get(0);
                queryClause = "\"query\":" + queryFilterClause;

            } else {

                // This should never be reached, and if somehow it did, Elasticsearch would return an error.
                queryClause = "\"query\":{}";
            }
            topLevelClauses.add(queryClause);

            // Construct the "script_fields" clause.
            String scriptFieldsClause = makeScriptFieldsClause(this.input, indexName);
            if (scriptFieldsClause != null)
                topLevelClauses.add(scriptFieldsClause);

            // Construct the "size" clause.
            topLevelClauses.add("\"size\":" + this.maxDocsPerQuery);

            // Construct the "profile" clause.
            if (this.profile)
                topLevelClauses.add("\"profile\":true");
            if (this.includeSeqNoPrimaryTerm)
                topLevelClauses.add("\"seq_no_primary_term\":true");
            if (this.includeVersion)
                topLevelClauses.add("\"version\":true");

            // Construct the final query.
            query = "{" + String.join(",", topLevelClauses) + "}";

            // Submit query to Elasticsearch.
            SearchResponse response = null;
            Exception responseError = null;
            try {
                response = this.search(indexName, query);
            } catch (IndexNotFoundException e) {
                // Don't fail the job if an index was missing.
                this.missingIndices.add(e.getIndex().getName());
                responseError = e;
            } catch (Exception e) {
                // Fail the job for any other error.
                this.failed = true;
                responseError = e;
            }

            // Read response from Elasticsearch.
            JsonNode responseData = null;
            if (response != null)
                responseData = Json.ORDERED_MAPPER.readTree(response.toString());

            // Log queries.
            if (this.includeQueries || this.profile) {
                String responseString;
                if (responseData != null) {
                    JsonNode responseDataCopy = responseData.deepCopy();
                    ObjectNode responseDataCopyObj = (ObjectNode) responseDataCopy;
                    if (responseDataCopyObj.has("hits")) {
                        ObjectNode responseDataCopyObjHits = (ObjectNode) responseDataCopyObj.get("hits");
                        if (responseDataCopyObjHits.has("hits"))
                            responseDataCopyObjHits.remove("hits");
                    }
                    responseString = responseDataCopyObj.toString();
                } else {
                    ElasticsearchException e = (ElasticsearchException) responseError;
                    String cause = Strings.toString(e.toXContent(jsonBuilder().startObject(), ToXContent.EMPTY_PARAMS).endObject());
                    responseString = "{\"error\":{\"root_cause\":[" + cause + "],\"type\":\"" + ElasticsearchException.getExceptionName(e) + "\",\"reason\":\"" + e.getMessage() + "\"},\"status\":" + e.status().getStatus() + "}";
                }
                String logged = serializeLoggedQuery(this.input, this.hop, _query, indexName, query, responseString, resolvers, resolversFilterTreeGrouped, termResolvers, termResolversFilterTree);
                this.queries.add(logged);
            }

            // Stop traversing if there was an error not due to a missing index.
            // Include the logged query in the response.
            if (this.failed) {
                this.error = serializeException(responseError, this.includeErrorTrace);
                return;
            }

            // Read the hits
            if (responseData == null)
                continue;
            if (!responseData.has("hits"))
                continue;
            if (!responseData.get("hits").has("hits"))
                continue;
            for (JsonNode doc : responseData.get("hits").get("hits")) {

                // Skip doc if already fetched. Otherwise mark doc as fetched and then proceed.
                String _id = Json.quoteString(doc.get("_id").textValue());
                if (this.docIds.get(indexName).contains(_id))
                    continue;
                this.docIds.get(indexName).add(_id);

                // Gather attributes from the doc. Store them in the "_attributes" field of the doc,
                // and include them in the attributes for subsequent queries.
                TreeMap<String, TreeSet<Value>> docAttributes = new TreeMap<>();
                TreeMap<String, JsonNode> docIndexFields = new TreeMap<>();
                for (String indexFieldName : this.input.model().indices().get(indexName).fields().keySet()) {
                    String attributeName = this.input.model().indices().get(indexName).fields().get(indexFieldName).attribute();
                    if (this.input.model().attributes().get(attributeName) == null)
                        continue;
                    String attributeType = this.input.model().attributes().get(attributeName).type();

                    // Get the attribute values from the doc.
                    if (doc.has("fields") && doc.get("fields").has(indexFieldName)) {

                        // Get the attribute value from the "fields" field if it exists there.
                        // This would include 'date' attribute types, for example.
                        JsonNode valueNode = doc.get("fields").get(indexFieldName);
                        if (valueNode.isNull() || valueNode.isMissingNode()) {
                            continue;
                        } else if (valueNode.isArray()) {
                            Iterator<JsonNode> valueNodeIterator = valueNode.elements();
                            while (valueNodeIterator.hasNext()) {
                                JsonNode vNode = valueNodeIterator.next();
                                if (vNode.isNull() || valueNode.isMissingNode())
                                    continue;
                                Value value = Value.create(attributeType, vNode);
                                if (!docAttributes.containsKey(attributeName))
                                    docAttributes.put(attributeName, new TreeSet<>());
                                if (!nextInputAttributes.containsKey(attributeName))
                                    nextInputAttributes.put(attributeName, new Attribute(attributeName, attributeType));
                                docAttributes.get(attributeName).add(value);
                                nextInputAttributes.get(attributeName).values().add(value);
                            }
                            if (valueNode.size() == 1)
                                docIndexFields.put(indexFieldName, valueNode.elements().next());
                            else
                                docIndexFields.put(indexFieldName, valueNode);
                        } else {
                            Value value = Value.create(attributeType, valueNode);
                            if (!docAttributes.containsKey(attributeName))
                                docAttributes.put(attributeName, new TreeSet<>());
                            if (!nextInputAttributes.containsKey(attributeName))
                                nextInputAttributes.put(attributeName, new Attribute(attributeName, attributeType));
                            docAttributes.get(attributeName).add(value);
                            nextInputAttributes.get(attributeName).values().add(value);
                            docIndexFields.put(indexFieldName, valueNode);
                        }

                    } else {

                        // Get the attribute value from the "_source" field.
                        // The index field name might not refer to the _source property.
                        // If it's not in the _source, remove the last part of the index field name from the dot notation.
                        // Index field names can reference multi-fields, which are not returned in the _source.
                        // If the document does not contain a given index field, skip that field.
                        JsonPointer path = this.input.model().indices().get(indexName).fields().get(indexFieldName).path();
                        JsonPointer pathParent = this.input.model().indices().get(indexName).fields().get(indexFieldName).pathParent();
                        JsonNode valueNode = doc.get("_source").at(path);
                        if (valueNode.isMissingNode()) {
                            if (pathParent != null)
                                valueNode = doc.get("_source").at(pathParent);
                            else
                                continue;
                        }
                        if (valueNode.isNull() || valueNode.isMissingNode())
                            continue;
                        docIndexFields.put(indexFieldName, valueNode);
                        if (valueNode.isArray()) {
                            Iterator<JsonNode> valueNodeIterator = valueNode.elements();
                            while (valueNodeIterator.hasNext()) {
                                JsonNode vNode = valueNodeIterator.next();
                                if (vNode.isNull() || valueNode.isMissingNode())
                                    continue;
                                Value value = Value.create(attributeType, vNode);
                                if (!docAttributes.containsKey(attributeName))
                                    docAttributes.put(attributeName, new TreeSet<>());
                                if (!nextInputAttributes.containsKey(attributeName))
                                    nextInputAttributes.put(attributeName, new Attribute(attributeName, attributeType));
                                docAttributes.get(attributeName).add(value);
                                nextInputAttributes.get(attributeName).values().add(value);
                            }
                        } else {
                            Value value = Value.create(attributeType, valueNode);
                            if (!docAttributes.containsKey(attributeName))
                                docAttributes.put(attributeName, new TreeSet<>());
                            if (!nextInputAttributes.containsKey(attributeName))
                                nextInputAttributes.put(attributeName, new Attribute(attributeName, attributeType));
                            docAttributes.get(attributeName).add(value);
                            nextInputAttributes.get(attributeName).values().add(value);
                        }
                    }
                }

                // Modify doc metadata.
                if (this.includeHits) {
                    ObjectNode docObjNode = (ObjectNode) doc;
                    docObjNode.remove("_score");
                    docObjNode.remove("fields");
                    docObjNode.put("_hop", this.hop);
                    docObjNode.put("_query", _query);
                    if (this.includeScore)
                        docObjNode.putNull("_score");
                    if (this.includeAttributes) {
                        ObjectNode docAttributesObjNode = docObjNode.putObject("_attributes");
                        for (String attributeName : docAttributes.keySet()) {
                            ArrayNode docAttributeArrNode = docAttributesObjNode.putArray(attributeName);
                            for (Value value : docAttributes.get(attributeName))
                                docAttributeArrNode.add(value.value());
                        }
                    }

                    // Determine why any matching documents matched if including "_score" or "_explanation".
                    Map<String, Double> bestAttributeConfidenceScores = new HashMap<>();
                    if (namedFilters && docObjNode.has("matched_queries") && docObjNode.get("matched_queries").size() > 0) {
                        ObjectNode docExpObjNode = docObjNode.putObject("_explanation");
                        ObjectNode docExpResolversObjNode = docExpObjNode.putObject("resolvers");
                        ArrayNode docExpMatchesArrNode = docExpObjNode.putArray("matches");
                        Set<String> expAttributes = new TreeSet<>();
                        Set<String> matchedQueries = new TreeSet<>();

                        // Remove the unique identifier from "_name" to remove duplicates.
                        for (JsonNode mqNode : docObjNode.get("matched_queries")) {
                            String[] _name = COLON.split(mqNode.asText());
                            _name = Arrays.copyOf(_name, _name.length - 1);
                            matchedQueries.add(String.join(":", _name));
                        }

                        // Create tuple-like objects that describe which attribute values matched which
                        // index field values using which matchers and matcher parameters.
                        Map<String, ArrayList<Double>> attributeConfidenceScores = new HashMap<>();
                        for (String mq : matchedQueries) {
                            String[] _name = COLON.split(mq);
                            String attributeName = _name[0];
                            String indexFieldName = _name[1];
                            String matcherName = _name[2];
                            String attributeValueSerialized = new String(Base64.getDecoder().decode(_name[3]));
                            String attributeType = this.input().model().attributes().get(attributeName).type();
                            if (attributeType.equals("string") || attributeType.equals("date"))
                                attributeValueSerialized = "\"" + attributeValueSerialized + "\"";
                            JsonNode attributeValueNode = Json.MAPPER.readTree("{\"attribute_value\":" + attributeValueSerialized + "}").get("attribute_value");
                            JsonNode matcherParamsNode;
                            if (input.attributes().containsKey(attributeName))
                                matcherParamsNode = Json.ORDERED_MAPPER.readTree(Json.ORDERED_MAPPER.writeValueAsString(input.attributes().get(attributeName).params()));
                            else if (input.model().matchers().containsKey(matcherName))
                                matcherParamsNode = Json.ORDERED_MAPPER.readTree(Json.ORDERED_MAPPER.writeValueAsString(input.model().matchers().get(matcherName).params()));
                            else
                                matcherParamsNode = Json.ORDERED_MAPPER.readTree("{}");
                            ObjectNode docExpDetailsObjNode = Json.ORDERED_MAPPER.createObjectNode();
                            docExpDetailsObjNode.put("attribute", attributeName);
                            docExpDetailsObjNode.put("target_field", indexFieldName);
                            docExpDetailsObjNode.put("target_value", docIndexFields.get(indexFieldName));
                            docExpDetailsObjNode.put("input_value", attributeValueNode);
                            docExpDetailsObjNode.put("input_matcher", matcherName);
                            docExpDetailsObjNode.putPOJO("input_matcher_params", matcherParamsNode);
                            docExpMatchesArrNode.add(docExpDetailsObjNode);
                            expAttributes.add(attributeName);

                            // Calculate the attribute confidence score for this match.
                            if (this.includeScore) {
                                Double abs = this.input().model().attributes().get(attributeName).score();
                                Double mqs = this.input().model().matchers().get(matcherName).quality();
                                Double iqs = this.input().model().indices().get(indexName).fields().get(indexFieldName).quality();
                                if (abs == null)
                                    continue;
                                Double score = abs;
                                if (mqs != null)
                                    score = ((score - 0.5) / (score - 0.0) * ((score * mqs) - score)) + score;
                                if (iqs != null)
                                    score = ((score - 0.5) / (score - 0.0) * ((score * iqs) - score)) + score;
                                attributeConfidenceScores.putIfAbsent(attributeName, new ArrayList<>());
                                attributeConfidenceScores.get(attributeName).add(score);
                            }
                        }

                        if (this.includeScore) {

                            // Deconflict multiple attribute confidence scores for the same attribute
                            // by selecting the highest score.
                            for (String attributeName : attributeConfidenceScores.keySet()) {
                                Double best = Collections.max(attributeConfidenceScores.get(attributeName));
                                bestAttributeConfidenceScores.put(attributeName, best);
                            }

                            // Combine the attribute confidence scores into a final document confidence score using
                            // conflation of probability distributions.
                            //
                            // https://arxiv.org/pdf/0808.1808v4.pdf
                            Double documentConfidenceScore;
                            ArrayList<Double> scores = new ArrayList<>();
                            ArrayList<Double> scoresInverse = new ArrayList<>();
                            for (String attributeName : bestAttributeConfidenceScores.keySet()) {
                                Double score = bestAttributeConfidenceScores.get(attributeName);
                                if (score == null)
                                    continue;
                                scores.add(score);
                                scoresInverse.add(1.0 - score);
                            }
                            if (scores.size() > 0) {
                                Double productScores = scores.stream().reduce(1.0, (a, b) -> a * b);
                                Double productScoresInverse = scoresInverse.stream().reduce(1.0, (a, b) -> a * b);
                                documentConfidenceScore = productScores / (productScores + productScoresInverse);
                                docObjNode.put("_score", documentConfidenceScore);
                            }
                        }

                        // Summarize matched resolvers
                        for (String resolverName : input.model().resolvers().keySet()) {
                            if (expAttributes.containsAll(input.model().resolvers().get(resolverName).attributes())) {
                                ObjectNode docExpResolverObjNode = docExpResolversObjNode.putObject(resolverName);
                                ArrayNode docExpResolverAttributesArrNode = docExpResolverObjNode.putArray("attributes");
                                for (String attributeName : input.model().resolvers().get(resolverName).attributes())
                                    docExpResolverAttributesArrNode.add(attributeName);
                            }
                        }
                        docObjNode.remove("matched_queries");
                        if (!this.includeExplanation)
                            docObjNode.remove("_explanation");
                    }

                    // Either remove "_source" or move "_source" under "_attributes".
                    if (!this.includeSource) {
                        docObjNode.remove("_source");
                    } else {
                        JsonNode _sourceNode = docObjNode.get("_source");
                        docObjNode.remove("_source");
                        docObjNode.set("_source", _sourceNode);
                    }

                    // Store doc in response.
                    this.hits.add(doc.toString());
                }
            }
            _query++;
        }

        // Stop traversing if we've reached max depth.
        boolean maxDepthReached = this.maxHops > -1 && this.hop >= this.maxHops;
        if (maxDepthReached)
            return;

        // Update input attributes for the next queries.
        for (String attributeName : nextInputAttributes.keySet()) {
            if (!this.attributes.containsKey(attributeName)) {
                String attributeType = this.input.model().attributes().get(attributeName).type();
                this.attributes.put(attributeName, new Attribute(attributeName, attributeType));
            }
            for (Value value : nextInputAttributes.get(attributeName).values()) {
                if (!this.attributes.get(attributeName).values().contains(value)) {
                    this.attributes.get(attributeName).values().add(value);
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
    public String run() throws IOException {
        try {

            // Reset the state of the job if reusing this Job object.
            if (this.ran)
                this.resetState();
            else
                this.attributes = new TreeMap<>(this.input.attributes());

            // Start timer and begin job
            String response;
            long startTime = System.nanoTime();
            try {
                this.traverse();
            } catch (Exception e) {
                this.failed = true;
                this.error = serializeException(e, this.includeErrorTrace);
            } finally {
                long took = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                // Format response
                List<String> responseParts = new ArrayList<>();
                responseParts.add("\"took\":" + Long.toString(took));
                if (this.error != null)
                    responseParts.add("\"error\":{" + this.error + "}");
                if (this.includeHits)
                    responseParts.add("\"hits\":{\"total\":" + this.hits.size() + ",\"hits\":[" + String.join(",", this.hits) + "]}");
                if (this.includeQueries || this.profile)
                    responseParts.add("\"queries\":[" + queries + "]");
                response = "{" + String.join(",", responseParts) + "}";
                if (this.pretty)
                    response = Json.ORDERED_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(Json.ORDERED_MAPPER.readTree(response));
            }
            return response;

        } finally {
            this.ran = true;
        }
    }

}
