/*
 * zentity
 * Copyright © 2018-2024 Dave Moore
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
package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.Attribute;
import io.zentity.resolution.input.Input;
import io.zentity.resolution.input.value.Value;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
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
    private Map<String, Map<String, Map<String, Map<String, Double>>>> attributeIdentityConfidenceScores = new HashMap<>();
    private Map<String, Attribute> attributes = new TreeMap<>();
    private NodeClient client;
    private Map<String, Set<String>> docIds = new TreeMap<>();
    private String error = null;
    private boolean failed = false;
    private List<String> hits = new ArrayList<>();
    private int hop = -1;
    private Boolean hopNewHits = false;
    private Map<String, Attribute> hopNextInputAttributes = new TreeMap<>();
    private List<Query> hopQueue = new ArrayList<>();
    private Set<String> missingIndices = new TreeSet<>();
    private List<String> queries = new ArrayList<>();
    private boolean ran = false;
    private long startTime = 0;
    private long took = 0;

    public Job(NodeClient client) {
        this.client = client;
    }

    /**
     * Serialize a caught exception object to a JSON-formatted string.
     * This becomes the response body of a failed resolution job.
     *
     * @param e                 The caught exception object.
     * @param includeErrorTrace Whether to include the stack trace in the response.
     * @return JSON-formatted string of the exception.
     */
    public static String serializeException(Exception e, boolean includeErrorTrace) {
        List<String> errorParts = new ArrayList<>();
        if (e instanceof ElasticsearchException || e instanceof XContentParseException)
            errorParts.add("\"by\":\"elasticsearch\"");
        else
            errorParts.add("\"by\":\"zentity\"");
        errorParts.add("\"type\":" + Json.quoteString(e.getClass().getCanonicalName()) + "");
        errorParts.add("\"reason\":" + Json.quoteString(e.getMessage()) + "");
        if (includeErrorTrace) {
            StringWriter traceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(traceWriter));
            errorParts.add("\"stack_trace\":" + Json.quoteString(traceWriter.toString()) + "");
        }
        return String.join(",", errorParts);
    }

    /**
     * Serialize a query, response, and associated metadata as a JSON-formatted string.
     * These are included in the "queries" field of the response object.
     *
     * @param input    The input of the resolution job.
     * @param _hop     The hop number in which the query was submitted.
     * @param query    The query object that zentity used to communicate with Elasticsearch.
     * @param response The response from Elasticsearch as a JSON-formatted string.
     * @return JSON-formatted string of the logged query.
     * @throws JsonProcessingException
     */
    public static String serializeLoggedQuery(Input input, int _hop, Query query, String response) throws JsonProcessingException {
        List<String> filtersLoggedList = new ArrayList<>();
        if (!query.resolvers().isEmpty() && !query.resolversFilterTreeGrouped().isEmpty()) {
            List<String> attributesResolversSummary = new ArrayList<>();
            for (String resolverName : query.resolvers()) {
                List<String> resolversAttributes = new ArrayList<>();
                for (String attributeName : input.model().resolvers().get(resolverName).attributes())
                    resolversAttributes.add("\"" + attributeName + "\"");
                attributesResolversSummary.add("\"" + resolverName + "\":{\"attributes\":[" +  String.join(",", resolversAttributes) + "]}");
            }
            String attributesResolversFilterTreeLogged = Json.ORDERED_MAPPER.writeValueAsString(query.resolversFilterTreeGrouped());
            filtersLoggedList.add("\"attributes\":{\"tree\":" + attributesResolversFilterTreeLogged + ",\"resolvers\":{" + String.join(",", attributesResolversSummary) + "}}");
        } else {
            filtersLoggedList.add("\"attributes\":null");
        }
        if (!query.termResolvers().isEmpty() && !query.termResolversFilterTree().isEmpty()) {
            List<String> termsResolversSummary = new ArrayList<>();
            for (String resolverName : query.termResolvers()) {
                List<String> resolverAttributes = new ArrayList<>();
                for (String attributeName : input.model().resolvers().get(resolverName).attributes())
                    resolverAttributes.add("\"" + attributeName + "\"");
                termsResolversSummary.add("\"" + resolverName + "\":{\"attributes\":[" +  String.join(",", resolverAttributes) + "]}");
            }
            String termResolversFilterTreeLogged = Json.ORDERED_MAPPER.writeValueAsString(query.termResolversFilterTree());
            filtersLoggedList.add("\"terms\":{\"tree\":{\"0\":" + termResolversFilterTreeLogged + "},\"resolvers\":{" + String.join(",", termsResolversSummary) + "}}");
        } else {
            filtersLoggedList.add("\"terms\":null");
        }
        String filtersLogged = String.join(",", filtersLoggedList);
        String searchLogged = "{\"request\":" + query.query() + ",\"response\":" + response + "}";
        return "{\"_hop\":" + _hop + ",\"_query\":" + query.number() + ",\"_index\":\"" + query.indexName() + "\",\"filters\":{" + filtersLogged + "},\"search\":" + searchLogged + "}";
    }

    /**
     * Extract attribute values from a document "_source" given the path to the index field.
     * Able to extract values from object keys, arrays, and object arrays.
     * Able to handle field names that contain periods.
     *
     * Example doc:
     *
     * {
     *   "a0": {
     *     "b0": {
     *       "c0": 0,
     *       "d0": 9
     *     }
     *   },
     *   "a1": {
     *     "b1": {
     *       "c1": [
     *         1, 2
     *       ],
     *       "d1": [
     *         9, 9
     *       ]
     *     }
     *   },
     *   "a2": {
     *     "b2": [
     *       {
     *         "c2": 3,
     *         "d2": 9
     *       },
     *       {
     *         "c2": 4,
     *         "d2": 9
     *       }
     *     ]
     *   },
     *   "a.3": {
     *     "b.3": [
     *       {
     *         "c.3": 5,
     *         "d.3": 9
     *       },
     *       {
     *         "c.3": 6,
     *         "d.3": 9
     *       }
     *     ]
     *   }
     * }
     *
     * Example paths, and the values extracted from the doc:
     *
     * Path         Values
     * -----------  ------
     * a0.b0.c0     0
     * a1.b1.c1     1, 2
     * a2.b2.c2     3, 4
     * a.3.b.3.c.3  5, 6
     *
     *
     * @param json   The "_source" of a document.
     * @param path   The path to the index field.
     * @param values Any attribute values found at the path of the document.
     * @return
     */
    public static ArrayList<JsonNode> extractValues(JsonNode json, String[] path, ArrayList<JsonNode> values) {
        if (json.isObject()) {
            String pathNext = "";
            for (int i = 0; i < path.length; i++) {
                pathNext = i == 0 ? path[0] : pathNext + "." + path[i];
                if (json.has(pathNext)) {
                    String[] pathRemaining = Arrays.copyOfRange(path, i + 1, path.length);
                    JsonNode jsonNext = json.get(pathNext);
                    values = extractValues(jsonNext, pathRemaining, values);
                    break;
                }
            }
        } else if (json.isArray()) {
            Iterator<JsonNode> jsonNextIterator = json.elements();
            while (jsonNextIterator.hasNext()) {
                JsonNode jsonNextItem = jsonNextIterator.next();
                values = extractValues(jsonNextItem, path, values);
            }
        } else {
            values.add(json);
        }
        return values;
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
     * Resets the variables that hold the state of the job, in case the same Job object is reused.
     */
    private void resetState() {
        this.attributeIdentityConfidenceScores = new HashMap<>();
        this.attributes = new TreeMap<>(this.input().attributes());
        this.docIds = new TreeMap<>();
        this.error = null;
        this.failed = false;
        this.hits = new ArrayList<>();
        this.hop = -1;
        this.hopNewHits = false;
        this.hopNextInputAttributes = new TreeMap<>();
        this.hopQueue = new ArrayList<>();
        this.missingIndices = new TreeSet<>();
        this.queries = new ArrayList<>();
        this.ran = false;
        this.startTime = 0;
        this.took = 0;
    }

    // Job configuration setters and getters

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

    public boolean namedFilters() {
        return (this.includeExplanation || this.includeScore);
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

    // Job state setters and getters

    public Map<String, Attribute> attributes() {
        return this.attributes;
    }

    public Map<String, Map<String, Map<String, Map<String, Double>>>> attributeIdentityConfidenceScores() {
        return this.attributeIdentityConfidenceScores;
    }

    public NodeClient client() {
        return this.client;
    }

    public Map<String, Set<String>> docIds() {
        return this.docIds;
    }

    private void error(String error) {
        this.error = error;
    }

    public void error(Exception error) {
        error(serializeException(error, includeErrorTrace()));
    }

    public String error() {
        return this.error;
    }

    public void failed(Boolean failed) {
        this.failed = failed;
    }

    public Boolean failed() {
        return this.failed;
    }

    public List<String> hits() {
        return this.hits;
    }

    public int hop() {
        return this.hop;
    }

    private void hopNewHits(Boolean hopNewHits) {
        this.hopNewHits = hopNewHits;
    }

    public Boolean hopNewHits() {
        return this.hopNewHits;
    }

    public Map<String, Attribute> hopNextInputAttributes() {
        return this.hopNextInputAttributes;
    }

    public List<Query> hopQueue() {
        return this.hopQueue;
    }

    public Input input() {
        return this.input;
    }

    public void input(Input input) {
        this.input = input;
    }

    public Set<String> missingIndices() {
        return this.missingIndices;
    }

    public List<String> queries() {
        return this.queries;
    }

    private void ran(boolean ran) {
        this.ran = ran;
    }

    public boolean ran() {
        return this.ran;
    }

    public long startTime() {
        return this.startTime;
    }

    public void took(long took) {
        this.took = took;
    }

    public long took() {
        return this.took;
    }

    /**
     * Combine a list of attribute identity confidence scores into a single composite identity confidence score using
     * conflation of probability distributions.
     *
     * https://arxiv.org/pdf/0808.1808v4.pdf
     *
     * If the list of attribute identity confidence scores contain both a 1.0 and a 0.0, this will lead to a division by
     * zero. When that happens, set the composite identity confidence score to 0.5, because there can be no certainty
     * when there are conflicting input scores that suggest both a complete confidence in a true match and a complete
     * confidence in a false match.
     *
     * @param attributeIdentityConfidenceScores A list of attribute identity confidence scores.
     * @return The composite identity confidence score.
     */
    public static Double calculateCompositeIdentityConfidenceScore(List<Double> attributeIdentityConfidenceScores) {
        Double compositeIdentityConfidenceScore = null;
        ArrayList<Double> scores = new ArrayList<>();
        ArrayList<Double> scoresInverse = new ArrayList<>();
        for (Double score : attributeIdentityConfidenceScores) {
            if (score == null)
                continue;
            scores.add(score);
            scoresInverse.add(1.0 - score);
        }
        if (scores.size() > 0) {
            Double productScores = scores.stream().reduce(1.0, (a, b) -> a * b);
            Double productScoresInverse = scoresInverse.stream().reduce(1.0, (a, b) -> a * b);
            compositeIdentityConfidenceScore = productScores / (productScores + productScoresInverse);
            if (compositeIdentityConfidenceScore.isNaN())
                compositeIdentityConfidenceScore = 0.5;
        }
        return compositeIdentityConfidenceScore;
    }

    /**
     * Calculate an attribute identity confidence score given a base score, a matcher quality score, and an index field
     * quality score. Any quality score of 0.0 will lead to a division by zero. When that happens, set the output score
     * to 0.0, because an attribute can give no confidence of an identity when any of the quality scores are 0.0.
     *
     * @param attributeIdentityConfidenceBaseScore The identity confidence base score of the attribute as defined in an entity model.
     * @param matcherQualityScore The quality score of the matcher as defined in an entity model.
     * @param indexFieldQualityScore The quality score of the index field as defined in an entity model.
     * @return The attribute identity confidence score as adjusted by the quality scores.
     */
    public static Double calculateAttributeIdentityConfidenceScore(Double attributeIdentityConfidenceBaseScore, Double matcherQualityScore, Double indexFieldQualityScore) {
        if (attributeIdentityConfidenceBaseScore == null)
            return null;
        Double score = attributeIdentityConfidenceBaseScore;
        if (matcherQualityScore != null)
            score = ((score - 0.5) / (score - 0.0) * ((score * matcherQualityScore) - score)) + score;
        if (indexFieldQualityScore != null)
            score = ((score - 0.5) / (score - 0.0) * ((score * indexFieldQualityScore) - score)) + score;
        if (score.isNaN())
            score = 0.0;
        return score;
    }

    /**
     * Get a cached attribute identity confidence score, or calculate and cache an attribute identity confidence score.
     * This function helps minimize calculations over the life of the resolution job.
     *
     * @param attributeName  The name of the attribute.
     * @param matcherName    The name of the matcher.
     * @param indexName      The name of the index.
     * @param indexFieldName The name of the index field.
     * @return The attribute identity confidence score, either as it was cached or as it was newly calculated.
     */
    private Double getAttributeIdentityConfidenceScore(Model model, String attributeName, String matcherName, String indexName, String indexFieldName) {

        // Return the cached match score if it exists.
        if (this.attributeIdentityConfidenceScores.containsKey(attributeName))
            if (this.attributeIdentityConfidenceScores.get(attributeName).containsKey(matcherName))
                if (this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).containsKey(indexName))
                    if (this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).get(indexName).containsKey(indexFieldName))
                        return this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).get(indexName).get(indexFieldName);

        // Calculate the match score, cache it, and return it.
        Double attributeIdentityConfidenceBaseScore = model.attributes().get(attributeName).score();
        Double matcherQualityScore = model.matchers().get(matcherName).quality();
        Double indexFieldQualityScore = model.indices().get(indexName).fields().get(indexFieldName).quality();
        if (attributeIdentityConfidenceBaseScore == null)
            return null;
        Double attributeIdentityConfidenceScore = calculateAttributeIdentityConfidenceScore(attributeIdentityConfidenceBaseScore, matcherQualityScore, indexFieldQualityScore);
        if (!this.attributeIdentityConfidenceScores.containsKey(attributeName))
            this.attributeIdentityConfidenceScores.put(attributeName, new HashMap<>());
        else if (!this.attributeIdentityConfidenceScores.get(attributeName).containsKey(matcherName))
            this.attributeIdentityConfidenceScores.get(attributeName).put(matcherName, new HashMap<>());
        else if (!this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).containsKey(indexName))
            this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).put(indexName, new HashMap<>());
        else if (!this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).get(indexName).containsKey(indexFieldName))
            this.attributeIdentityConfidenceScores.get(attributeName).get(matcherName).get(indexName).put(indexFieldName, attributeIdentityConfidenceScore);
        return attributeIdentityConfidenceScore;
    }

    /**
     * This action processes the response of an Elasticsearch query and then continues the job traversal.
     *
     * @param job           The resolution job.
     * @param query         The query that was submitted to Elasticsearch.
     * @param response      The response that Elasticsearch returned.
     * @param responseError The error that Elasticsearch returned (if any, otherwise null).
     * @param onComplete    The action to perform after the job completes.
     * @throws IOException
     * @throws ValidationException
     */
    private void onSearchComplete(Job job, Query query, SearchResponse response, Exception responseError, ActionListener<String> onComplete) throws IOException, ValidationException {

        // Read response from Elasticsearch.
        JsonNode responseData = null;
        if (response != null)
            responseData = Json.ORDERED_MAPPER.readTree(response.toString());

        // Log queries.
        if (job.includeQueries() || job.profile()) {
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
                if (responseError instanceof XContentParseException) {
                    XContentParseException e = (XContentParseException) responseError;
                    String cause = "{\"type\":\"parsing_exception\",\"reason\":\"" + e.getMessage() + "\",\"line\":" + e.getLineNumber() + ",\"col\":" + e.getColumnNumber() + "}";
                    responseString = "{\"error\":{\"root_cause\":[" + cause + "],\"type\":\"parsing_exception\",\"reason\":\"" + e.getMessage() + "\",\"line\":" + e.getLineNumber() + ",\"col\":" + e.getColumnNumber() + "},\"status\":400}";
                } else  {
                    ElasticsearchException e = (ElasticsearchException) responseError;
                    String cause = Strings.toString(e.toXContent(jsonBuilder().startObject(), ToXContent.EMPTY_PARAMS).endObject());
                    responseString = "{\"error\":{\"root_cause\":[" + cause + "],\"type\":\"" + ElasticsearchException.getExceptionName(e) + "\",\"reason\":\"" + e.getMessage() + "\"},\"status\":" + e.status().getStatus() + "}";
                }
            }
            String logged = serializeLoggedQuery(job.input(), job.hop(), query, responseString);
            job.queries().add(logged);
        }

        // Stop traversing if there was an error not due to a missing index.
        // Include the logged query in the response.
        if (job.failed()) {
            job.error(responseError);
            onComplete.onResponse(job.response());
            return;
        }

        // Traverse if there are no hits.
        boolean hits = true;
        if (responseData == null)
            hits = false;
        else if (!responseData.has("hits"))
            hits = false;
        else if (!responseData.get("hits").has("hits"))
            hits = false;
        if (!hits) {
            job.traverse(job, onComplete);
            return;
        }

        // Read the hits
        for (JsonNode doc : responseData.get("hits").get("hits")) {

            // Skip doc if already fetched. Otherwise mark doc as fetched and then proceed.
            String _id = Json.quoteString(doc.get("_id").textValue());
            if (job.docIds().get(query.indexName()).contains(_id))
                continue;
            String indexName = query.indexName();
            job.docIds().get(indexName).add(_id);

            // Gather attributes from the doc. Store them in the "_attributes" field of the doc,
            // and include them in the attributes for subsequent queries.
            TreeMap<String, TreeSet<Value>> docAttributes = new TreeMap<>();
            TreeMap<String, JsonNode> docIndexFields = new TreeMap<>();
            for (String indexFieldName : job.input().model().indices().get(indexName).fields().keySet()) {
                String attributeName = job.input().model().indices().get(indexName).fields().get(indexFieldName).attribute();
                if (job.input().model().attributes().get(attributeName) == null)
                    continue;
                String attributeType = job.input().model().attributes().get(attributeName).type();

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
                            if (!job.hopNextInputAttributes().containsKey(attributeName))
                                job.hopNextInputAttributes().put(attributeName, new Attribute(attributeName, attributeType));
                            docAttributes.get(attributeName).add(value);
                            job.hopNextInputAttributes().get(attributeName).values().add(value);
                        }
                        if (valueNode.size() == 1)
                            docIndexFields.put(indexFieldName, valueNode.elements().next());
                        else
                            docIndexFields.put(indexFieldName, valueNode);
                    } else {
                        Value value = Value.create(attributeType, valueNode);
                        if (!docAttributes.containsKey(attributeName))
                            docAttributes.put(attributeName, new TreeSet<>());
                        if (!job.hopNextInputAttributes().containsKey(attributeName))
                            job.hopNextInputAttributes().put(attributeName, new Attribute(attributeName, attributeType));
                        docAttributes.get(attributeName).add(value);
                        job.hopNextInputAttributes().get(attributeName).values().add(value);
                        docIndexFields.put(indexFieldName, valueNode);
                    }

                } else {

                    // Get the attribute value from the "_source" field.
                    // The index field name might not refer to the _source property.
                    // If it's not in the _source, remove the last part of the index field name from the dot notation.
                    // Index field names can reference multi-fields, which are not returned in the _source.
                    // If the document does not contain a given index field, skip that field.
                    String[] path = job.input().model().indices().get(indexName).fields().get(indexFieldName).path();
                    ArrayList<JsonNode> values = extractValues(doc.get("_source"), path, new ArrayList<>());
                    if (values.size() == 0)
                        continue;
                    ArrayNode valuesArrayNode = Json.ORDERED_MAPPER.createArrayNode();
                    for (JsonNode vNode : values) {
                        if (vNode.isNull() || vNode.isMissingNode())
                            continue;
                        Value value = Value.create(attributeType, vNode);
                        if (!docAttributes.containsKey(attributeName))
                            docAttributes.put(attributeName, new TreeSet<>());
                        if (!job.hopNextInputAttributes().containsKey(attributeName))
                            job.hopNextInputAttributes().put(attributeName, new Attribute(attributeName, attributeType));
                        docAttributes.get(attributeName).add(value);
                        valuesArrayNode.add(vNode);
                        job.hopNextInputAttributes().get(attributeName).values().add(value);
                    }
                    if (valuesArrayNode.size() == 1)
                        docIndexFields.put(indexFieldName, valuesArrayNode.get(0));
                    else
                        docIndexFields.put(indexFieldName, valuesArrayNode);
                }
            }

            // Modify doc metadata.
            if (job.includeHits()) {
                ObjectNode docObjNode = (ObjectNode) doc;
                docObjNode.remove("_score");
                docObjNode.remove("fields");
                docObjNode.put("_hop", job.hop());
                docObjNode.put("_query", query.number());
                if (job.includeScore())
                    docObjNode.putNull("_score");
                if (job.includeAttributes()) {
                    docObjNode.putObject("_attributes");
                    for (String attributeName : docAttributes.keySet()) {
                        ObjectNode docAttributesObjNode = (ObjectNode) docObjNode.get("_attributes");
                        String[] nameFields = job.input().model().attributes().get(attributeName).nameFields();
                        String lastNameField = nameFields[nameFields.length - 1];
                        if (nameFields.length > 1) {
                            // This attribute has a nested structure as indicated by the periods in its name.
                            // Structure the attribute object by nesting its name fields. The last field will contain
                            // the array of values.
                            //
                            // For example, the attribute "location.address.street" would become:
                            //
                            // {
                            //   "_attributes": {
                            //     "location": {
                            //       "address": {
                            //         "street": [ VALUE, ... ]
                            //       }
                            //     }
                            //   }
                            // }
                            for (int i = 0; i < nameFields.length - 1; i++) {
                                String nameField = nameFields[i];
                                if (!docAttributesObjNode.has(nameField))
                                    docAttributesObjNode.putObject(nameField);
                                docAttributesObjNode = (ObjectNode) docAttributesObjNode.get(nameField);

                            }
                        }
                        // The last name field of the attribute contains the array of values.
                        ArrayNode docAttributeArrNode = docAttributesObjNode.putArray(lastNameField);
                        for (Value value : docAttributes.get(attributeName))
                            docAttributeArrNode.add(value.value());
                    }
                }

                // Determine why any matching documents matched if including "_score" or "_explanation".
                List<Double> bestAttributeIdentityConfidenceScores = new ArrayList<>();
                if (job.namedFilters() && docObjNode.has("matched_queries") && docObjNode.get("matched_queries").size() > 0) {
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
                    Map<String, ArrayList<Double>> attributeIdentityConfidenceBaseScores = new HashMap<>();
                    for (String mq : matchedQueries) {
                        String[] _name = COLON.split(mq);
                        String attributeName = _name[0];
                        String indexFieldName = _name[1];
                        String matcherName = _name[2];
                        String attributeValueSerialized = new String(Base64.getDecoder().decode(_name[3]));
                        String attributeType = job.input().model().attributes().get(attributeName).type();
                        if (attributeType.equals("string") || attributeType.equals("date"))
                            attributeValueSerialized = Json.jsonStringQuote(attributeValueSerialized);
                        JsonNode attributeValueNode = Json.MAPPER.readTree("{\"attribute_value\":" + attributeValueSerialized + "}").get("attribute_value");
                        JsonNode matcherParamsNode;
                        if (job.input().attributes().containsKey(attributeName))
                            matcherParamsNode = Json.ORDERED_MAPPER.readTree(Json.ORDERED_MAPPER.writeValueAsString(job.input().attributes().get(attributeName).params()));
                        else if (job.input().model().matchers().containsKey(matcherName))
                            matcherParamsNode = Json.ORDERED_MAPPER.readTree(Json.ORDERED_MAPPER.writeValueAsString(job.input().model().matchers().get(matcherName).params()));
                        else
                            matcherParamsNode = Json.ORDERED_MAPPER.readTree("{}");

                        // Calculate the attribute identity confidence score for this match.
                        Double attributeIdentityConfidenceScore = null;
                        if (job.includeScore()) {
                            attributeIdentityConfidenceScore = job.getAttributeIdentityConfidenceScore(job.input().model(), attributeName, matcherName, indexName, indexFieldName);
                            if (attributeIdentityConfidenceScore != null) {
                                attributeIdentityConfidenceBaseScores.putIfAbsent(attributeName, new ArrayList<>());
                                attributeIdentityConfidenceBaseScores.get(attributeName).add(attributeIdentityConfidenceScore);
                            }
                        }

                        ObjectNode docExpDetailsObjNode = Json.ORDERED_MAPPER.createObjectNode();
                        docExpDetailsObjNode.put("attribute", attributeName);
                        docExpDetailsObjNode.put("target_field", indexFieldName);
                        docExpDetailsObjNode.put("target_value", docIndexFields.get(indexFieldName));
                        docExpDetailsObjNode.put("input_value", attributeValueNode);
                        docExpDetailsObjNode.put("input_matcher", matcherName);
                        docExpDetailsObjNode.putPOJO("input_matcher_params", matcherParamsNode);
                        if (job.includeScore())
                            if (attributeIdentityConfidenceScore == null)
                                docExpDetailsObjNode.putNull("score");
                            else
                                docExpDetailsObjNode.put("score", attributeIdentityConfidenceScore);
                        docExpMatchesArrNode.add(docExpDetailsObjNode);
                        expAttributes.add(attributeName);
                    }

                    if (job.includeScore()) {

                        // Deconflict multiple attribute confidence scores for the same attribute
                        // by selecting the highest score.
                        for (String attributeName : attributeIdentityConfidenceBaseScores.keySet()) {
                            Double best = Collections.max(attributeIdentityConfidenceBaseScores.get(attributeName));
                            bestAttributeIdentityConfidenceScores.add(best);
                        }

                        // Combine the attribute confidence scores into a composite identity confidence score.
                        Double documentConfidenceScore = calculateCompositeIdentityConfidenceScore(bestAttributeIdentityConfidenceScores);
                        if (documentConfidenceScore != null)
                            docObjNode.put("_score", documentConfidenceScore);
                    }

                    // Summarize matched resolvers
                    for (String resolverName : job.input().model().resolvers().keySet()) {
                        if (expAttributes.containsAll(job.input().model().resolvers().get(resolverName).attributes())) {
                            ObjectNode docExpResolverObjNode = docExpResolversObjNode.putObject(resolverName);
                            ArrayNode docExpResolverAttributesArrNode = docExpResolverObjNode.putArray("attributes");
                            for (String attributeName : job.input().model().resolvers().get(resolverName).attributes())
                                docExpResolverAttributesArrNode.add(attributeName);
                        }
                    }
                    docObjNode.remove("matched_queries");
                    if (!job.includeExplanation())
                        docObjNode.remove("_explanation");
                }

                // Either remove "_source" or move "_source" under "_attributes".
                if (!job.includeSource()) {
                    docObjNode.remove("_source");
                } else {
                    JsonNode _sourceNode = docObjNode.get("_source");
                    docObjNode.remove("_source");
                    docObjNode.set("_source", _sourceNode);
                }

                // Store doc in response.
                job.hits().add(doc.toString());
            }
        }
        job.traverse(job, onComplete);
    }

    /**
     * Build the search queue for a hop.
     *
     * The hop search queue contains the search requests that will be executed in the hop.
     *
     * @throws IOException
     * @throws ValidationException
     */
    private void buildHopQueue() throws IOException, ValidationException {

        // Construct a query for each index that maps to a resolver.
        int hopQueryNumber = 0;
        for (String indexName : this.input.model().indices().keySet()) {

            // Skip this index if a prior hop determined the index to be missing.
            if (this.missingIndices.contains(indexName))
                continue;

            // Track _ids for this index.
            if (!this.docIds.containsKey(indexName))
                this.docIds.put(indexName, new TreeSet<>());

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
            Query query = new Query(this, hopQueryNumber, indexName, resolvers, canQueryIds, canQueryTerms);
            this.hopQueue.add(query);
            hopQueryNumber++;
        }
    }

    /**
     * Initialize the next hop.
     *
     * A "hop" is an attempt to search each relevant index once for new information on the entity.
     *
     * @throws IOException
     * @throws ValidationException
     */
    private void nextHop() throws IOException, ValidationException {
        this.hop++;
        this.hopNewHits = false;
        this.hopNextInputAttributes = new TreeMap<>();
        this.buildHopQueue();
    }

    /**
     * Given a set of attribute values, determine which queries to submit to which indices then submit them and recurse.
     *
     * @param job        The current job, to be passed to recursive methods.
     * @param onComplete The action to perform after completing the recursion.
     * @throws IOException
     * @throws ValidationException
     */
    private void traverse(Job job, ActionListener<String> onComplete) throws IOException, ValidationException {
        if (job.hop() < 0) {

            // No hops have been initialized.
            // Start the first hop.
            job.nextHop();

        }

        if (job.hopQueue().size() == 0) {

            // The search queue for this hop is empty.
            // Stop traversing if we've reached max depth.
            boolean maxDepthReached = job.maxHops() > -1 && job.hop() >= job.maxHops();
            if (maxDepthReached) {
                onComplete.onResponse(job.response());
                return;
            }

            // Maximum depth has not been reached.
            // Update the input attributes for the next queries.
            for (String attributeName : job.hopNextInputAttributes().keySet()) {
                if (!job.attributes().containsKey(attributeName)) {
                    String attributeType = job.input().model().attributes().get(attributeName).type();
                    job.attributes().put(attributeName, new Attribute(attributeName, attributeType));
                }
                for (Value value : job.hopNextInputAttributes().get(attributeName).values()) {
                    if (!job.attributes().get(attributeName).values().contains(value)) {
                        job.attributes().get(attributeName).values().add(value);
                        job.hopNewHits(true);
                    }
                }
            }

            // Stop traversing if there are no more attributes to query.
            if (!job.hopNewHits()) {
                onComplete.onResponse(job.response());
                return;
            }

            // Start the next hop.
            job.nextHop();

        }

        if (job.hopQueue().size() > 0) {

            // The search queue for this hop has items. Perform the next search and then recurse.
            Query query = job.hopQueue().remove(0);

            // Submit the query to Elasticsearch.
            query.request().execute(new ActionListener<>() {

                @Override
                public void onResponse(SearchResponse response) {
                    try {

                        // Process the response from Elasticsearch.
                        job.onSearchComplete(job, query, response, null, onComplete);
                    } catch (Exception e) {

                        // An error occurred when processing the response from Elasticsearch.
                        onComplete.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {

                        // Elasticsearch returned an error.
                        Exception responseError;
                        if (e.getClass() == IndexNotFoundException.class) {

                            // Don't fail the job if an index was missing.
                            job.missingIndices().add(((IndexNotFoundException) e).getIndex().getName());
                            responseError = e;
                        } else {

                            // Fail the job for any other error.
                            job.failed(true);
                            responseError = e;
                        }

                        // Process the response from Elasticsearch.
                        job.onSearchComplete(job, query, null, responseError, onComplete);
                    } catch (Exception ee) {

                        // An error occurred when processing the response from Elasticsearch.
                        onComplete.onFailure(ee);
                    }
                }
            });

        } else {
            job.traverse(job, onComplete);
        }
    }

    /**
     * Serialize the outputs of the job to be returned as the payload of a response.
     *
     * @return A JSON-formatted string.
     */
    public String response() throws JsonProcessingException {
        String response;
        List<String> responseParts = new ArrayList<>();
        responseParts.add("\"took\":" + this.took);
        if (this.error != null)
            responseParts.add("\"error\":{" + this.error + "}");
        if (this.includeHits)
            responseParts.add("\"hits\":{\"total\":" + this.hits.size() + ",\"hits\":[" + String.join(",", this.hits) + "]}");
        if (this.includeQueries || this.profile)
            responseParts.add("\"queries\":[" + queries + "]");
        response = "{" + String.join(",", responseParts) + "}";
        if (this.pretty)
            response = Json.pretty(response);
        return response;
    }

    /**
     * Run the entity resolution job.
     *
     * @param onComplete    The action to perform after completing the resolution job.
     */
    public void run(ActionListener<String> onComplete) {

        // Prepare to run the job.
        try {

            // Reset the state of the job if reusing this Job object.
            if (this.ran)
                this.resetState();
            else
                this.attributes = new TreeMap<>(this.input.attributes());

            // Start the timer and begin the job.
            this.startTime = System.nanoTime();
            Job job = this;
            job.traverse(job, new ActionListener<>() {

                @Override
                public void onResponse(String o) {
                    try {

                        // The job completed. Prepare and send the response.
                        job.took(TimeUnit.MILLISECONDS.convert(System.nanoTime() - job.startTime(), TimeUnit.NANOSECONDS));
                        job.ran(true);
                        onComplete.onResponse(job.response());
                    } catch (Exception e) {

                        // An error occurred when preparing or sending the response.
                        onComplete.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {

                        // The job failed. Prepare and send a response.
                        job.took(TimeUnit.MILLISECONDS.convert(System.nanoTime() - job.startTime(), TimeUnit.NANOSECONDS));
                        job.ran(true);
                        job.failed(true);
                        job.error(e);
                        onComplete.onResponse(job.response());
                    } catch (Exception ee) {

                        // An error occurred when preparing or sending the response.
                        onComplete.onFailure(ee);
                    }
                }
            });

        } catch (Exception e) {

            // An error occurred when preparing to run the job.
            this.ran(true);
            onComplete.onFailure(e);
        }
    }
}
