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
package io.zentity.resolution;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.*;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class Query {

    private static final Logger logger = LogManager.getLogger(Query.class);

    private final String indexName;
    private final int number;
    private final String query;
    private final SearchRequestBuilder request;
    private final List<String> resolvers;
    private TreeMap<String, TreeMap> resolversFilterTree = new TreeMap<>();
    private TreeMap<Integer, TreeMap<String, TreeMap>> resolversFilterTreeGrouped = new TreeMap<>();
    private List<String> termResolvers = new ArrayList<>();
    private TreeMap<String, TreeMap> termResolversFilterTree = new TreeMap<>();

    /**
     * Builds the "script_fields" clause of an Elasticsearch query.
     * This is required by some zentity attribute types such as the "date" type.
     *
     * @param input     The input of the resolution job.
     * @param indexName The index name of the query.
     * @return A JSON-formatted string of the "script_fields" clause of an Elasticsearch query.
     * @throws ValidationException
     */
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
     * Given a clause from the "matchers" field of an entity model, replace the {{ field }} and {{ value }} variables
     * and arbitrary parameters. If a parameter exists, replace the {{ params.PARAM_NAME }} variable with its value.
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
     * @return A JSON-formatted string containing all populated matcher "bool" clauses for an index field.
     */
    public static List<String> makeIndexFieldClauses(Model model, String indexName, Map<String, Attribute> attributes, String attributeName, String combiner, boolean namedFilters, AtomicInteger _nameIdCounter) throws ValidationException {
        if (!combiner.equals("should") && !combiner.equals("filter"))
            throw new ValidationException("'" + combiner + "' is not a supported clause combiner.");
        List<String> indexFieldClauses = new ArrayList<>();
        for (String indexFieldName : model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).keySet()) {

            // Can we use this index field?
            if (!Job.indexFieldHasMatcher(model, indexName, indexFieldName))
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
     * @return A list of JSON-formatted strings each containing the populated matcher "bool" clauses for each attribute.
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
     * Build a search request for Elasticsearch.
     *
     * @param indexName The name of the index to search.
     * @param query     The query to search.
     * @return The built search request.
     * @throws IOException
     */
    public static SearchRequestBuilder buildSearchRequest(Job job, String indexName, String query) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule
                .getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
            searchSourceBuilder.parseXContent(parser);
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(job.client(), SearchAction.INSTANCE);
        searchRequestBuilder.setIndices(indexName).setSource(searchSourceBuilder);
        if (job.searchAllowPartialSearchResults() != null)
            searchRequestBuilder.setAllowPartialSearchResults(job.searchAllowPartialSearchResults());
        if (job.searchBatchedReduceSize() != null)
            searchRequestBuilder.setBatchedReduceSize(job.searchBatchedReduceSize());
        if (job.searchMaxConcurrentShardRequests() != null)
            searchRequestBuilder.setMaxConcurrentShardRequests(job.searchMaxConcurrentShardRequests());
        if (job.searchPreFilterShardSize() != null)
            searchRequestBuilder.setPreFilterShardSize(job.searchPreFilterShardSize());
        if (job.searchPreference() != null)
            searchRequestBuilder.setPreference(job.searchPreference());
        if (job.searchRequestCache() != null)
            searchRequestBuilder.setRequestCache(job.searchRequestCache());
        if (job.maxTimePerQuery() != null)
            searchRequestBuilder.setTimeout(TimeValue.parseTimeValue(job.maxTimePerQuery(), "timeout"));
        return searchRequestBuilder;
    }

    public Query(Job job, int number, String indexName, List<String> resolvers, Boolean canQueryIds, Boolean canQueryTerms) throws ValidationException, IOException {
        this.indexName = indexName;
        this.number = number;
        this.resolvers = resolvers;

        // "_explanation" uses named queries, and each value of the "_name" fields must be unique.
        // Use a counter to prepend a unique and deterministic identifier for each "_name" field in the query.
        AtomicInteger _nameIdCounter = new AtomicInteger();

        // Construct query
        String queryClause;
        List<String> queryMustNotClauses = new ArrayList<>();
        String queryMustNotClause = "";
        List<String> queryFilterClauses = new ArrayList<>();
        String queryFilterClause = "";
        List<String> topLevelClauses = new ArrayList<>();
        topLevelClauses.add("\"_source\":true");

        // Exclude docs by _id
        Set<String> docIds = job.docIds().get(indexName);
        if (!docIds.isEmpty())
            queryMustNotClauses.add("{\"ids\":{\"values\":[" + String.join(",", docIds) + "]}}");

        // Create "scope.exclude.attributes" clauses. Combine them into a single "should" clause.
        if (!job.input().scope().exclude().attributes().isEmpty()) {
            List<String> attributeClauses = makeAttributeClauses(job.input().model(), indexName, job.input().scope().exclude().attributes(), "should", job.namedFilters(), _nameIdCounter);
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
        if (!job.input().scope().include().attributes().isEmpty()) {
            List<String> attributeClauses = makeAttributeClauses(job.input().model(), indexName, job.input().scope().include().attributes(), "filter", job.namedFilters(), _nameIdCounter);
            int size = attributeClauses.size();
            if (size > 1)
                queryFilterClauses.add("{\"bool\":{\"filter\":[" + String.join(",", attributeClauses) + "]}}");
            else if (size == 1)
                queryFilterClauses.add(attributeClauses.get(0));
        }

        // Construct the "ids" clause if this is the first hop and if any ids are specified for this index.
        String idsClause = "";
        if (canQueryIds) {
            Set<String> ids = job.input().ids().get(indexName);
            idsClause = "{\"bool\":{\"filter\":[{\"ids\":{\"values\":[" + String.join(",", ids) + "]}}]}}";
        }

        // Construct the resolvers clause for attribute values.
        String resolversClause = "";
        if (!job.attributes().isEmpty()) {

            // Group the resolvers by their weight level.
            TreeMap<Integer, List<String>> resolverGroups = groupResolversByWeight(job.input().model(), resolvers);

            // Construct a clause for each weight level in descending order of weight.
            List<Integer> weights = new ArrayList<>(resolverGroups.keySet());
            Collections.reverse(weights);
            int numWeightLevels = weights.size();
            for (int level = 0; level < numWeightLevels; level++) {
                Integer weight = weights.get(level);
                List<String> resolversGroup = resolverGroups.get(weight);
                Map<String, Integer> counts = countAttributesAcrossResolvers(job.input().model(), resolversGroup);
                List<List<String>> resolversSorted = sortResolverAttributes(job.input().model(), resolversGroup, counts);
                this.resolversFilterTree = makeResolversFilterTree(resolversSorted);
                this.resolversFilterTreeGrouped.put(numWeightLevels - level - 1, this.resolversFilterTree);
                resolversClause = populateResolversFilterTree(job.input().model(), indexName, this.resolversFilterTree, job.attributes(), job.namedFilters(), _nameIdCounter);

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
                            for (String attributeName : job.input().model().resolvers().get(parentResolverName).attributes())
                                attributeExistsClauses.add("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"" + attributeName + "\"}}}}");
                            String attributesExistsClause = "";
                            if (attributeExistsClauses.size() > 1)
                                attributesExistsClause = "{\"bool\":{\"should\":[" + String.join(",", attributeExistsClauses) + "]}}";
                            else if (attributeExistsClauses.size() == 1)
                                attributesExistsClause = attributeExistsClauses.get(0);

                            // Construct a clause for the resolver.
                            List<String> parentResolverGroup = new ArrayList<>(Arrays.asList(parentResolverName));
                            Map<String, Integer> parentCounts = countAttributesAcrossResolvers(job.input().model(), parentResolverGroup);
                            List<List<String>> parentResolverSorted = sortResolverAttributes(job.input().model(), parentResolverGroup, parentCounts);
                            TreeMap<String, TreeMap> parentResolverFilterTree = makeResolversFilterTree(parentResolverSorted);
                            String parentResolverClause = populateResolversFilterTree(job.input().model(), indexName, parentResolverFilterTree, job.attributes(), job.namedFilters(), _nameIdCounter);

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
        if (canQueryTerms) {
            String termResolversClause = "";

            // Get the names of each attribute of each in-scope resolver.
            TreeSet<String> resolverAttributes = new TreeSet<>();
            for (String resolverName : job.input().model().resolvers().keySet())
                resolverAttributes.addAll(job.input().model().resolvers().get(resolverName).attributes());

            // For each attribute, attempt to convert each term to a value of that attribute.
            // If the term does not match the attribute type, or if the term cannot be converted to a value
            // of that attribute, then skip the term and move on.
            //
            // Date attributes will require a format, but the format could be declared in the input attributes,
            // the model attributes, or the model matchers in descending order of precedence. If the pa
            Map<String, TreeSet<Value>> termValues = new TreeMap<>();
            for (String attributeName : resolverAttributes) {
                String attributeType = job.input().model().attributes().get(attributeName).type();
                for (Term term : job.input().terms()) {
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
                                Index index = job.input().model().indices().get(indexName);
                                // Check if the "format" param is defined in the input attribute.
                                if (job.input().attributes().containsKey(attributeName) && job.input().attributes().get(attributeName).params().containsKey("format") && !job.input().attributes().get(attributeName).params().get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(job.input().attributes().get(attributeName).params().get("format")).matches()) {
                                    String format = job.input().attributes().get(attributeName).params().get("format");
                                    if (term.isDate(format)) {
                                        termValues.putIfAbsent(attributeName, new TreeSet<>());
                                        termValues.get(attributeName).add(term.dateValue());
                                    }
                                } else {
                                    // Otherwise check if the "format" param is defined in the model attribute.
                                    Map<String, String> params = job.input().model().attributes().get(attributeName).params();
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
                                            params = job.input().model().matchers().get(matcherName).params();
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
            if (!job.attributes().isEmpty()) {
                for (String attributeName : job.attributes().keySet()) {
                    for (Value value : job.attributes().get(attributeName).values()) {
                        termValues.putIfAbsent(attributeName, new TreeSet<>());
                        termValues.get(attributeName).add(value);
                    }
                }
            }

            // Convert the values as if it was an input Attribute.
            Map<String, Attribute> termAttributes = new TreeMap<>();
            for (String attributeName : termValues.keySet()) {
                String attributeType = job.input().model().attributes().get(attributeName).type();
                List<String> jsonValues = new ArrayList<>();
                for (Value value : termValues.get(attributeName)) {
                    if (value instanceof StringValue)
                        jsonValues.add(Json.quoteString(value.serialized()));
                    else
                        jsonValues.add(value.serialized());
                }
                // Pass params from the input "attributes" if any were defined.
                String attributesJson;
                if (job.input().attributes().containsKey(attributeName) && !job.input().attributes().get(attributeName).params().isEmpty()) {
                    Set<String> params = new TreeSet<>();
                    for (String paramName : job.input().attributes().get(attributeName).params().keySet()) {
                        String paramValue = job.input().attributes().get(attributeName).params().get(paramName);
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
            for (String resolverName : job.input().model().resolvers().keySet())
                if (Job.canQueryResolver(job.input().model(), indexName, resolverName, termAttributes))
                    this.termResolvers.add(resolverName);

            // Construct the resolvers clause for term attribute values.
            if (this.termResolvers.size() > 0) {
                Map<String, Integer> counts = countAttributesAcrossResolvers(job.input().model(), this.termResolvers);
                List<List<String>> termResolversSorted = sortResolverAttributes(job.input().model(), this.termResolvers, counts);
                this.termResolversFilterTree = makeResolversFilterTree(termResolversSorted);
                termResolversClause = populateResolversFilterTree(job.input().model(), indexName, this.termResolversFilterTree, termAttributes, job.namedFilters(), _nameIdCounter);
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
        String scriptFieldsClause = makeScriptFieldsClause(job.input(), indexName);
        if (scriptFieldsClause != null)
            topLevelClauses.add(scriptFieldsClause);

        // Construct the "size" clause.
        topLevelClauses.add("\"size\":" + job.maxDocsPerQuery());

        // Construct the "profile" clause.
        if (job.profile())
            topLevelClauses.add("\"profile\":true");
        if (job.includeSeqNoPrimaryTerm())
            topLevelClauses.add("\"seq_no_primary_term\":true");
        if (job.includeVersion())
            topLevelClauses.add("\"version\":true");

        // Construct the final query and add it to the search queue for this hop.
        this.query = "{" + String.join(",", topLevelClauses) + "}";
        this.request = buildSearchRequest(job, indexName, this.query);
    }

    public String indexName() {
        return this.indexName;
    }

    public String query() {
        return this.query;
    }

    public int number() {
        return this.number;
    }

    public SearchRequestBuilder request() {
        return this.request;
    }

    public List<String> resolvers() {
        return this.resolvers;
    }

    public TreeMap<String, TreeMap> resolversFilterTree() {
        return this.resolversFilterTree;
    }

    public TreeMap<Integer, TreeMap<String, TreeMap>> resolversFilterTreeGrouped() {
        return this.resolversFilterTreeGrouped;
    }

    public List<String> termResolvers() {
        return this.termResolvers;
    }

    public TreeMap<String, TreeMap> termResolversFilterTree() {
        return this.termResolversFilterTree;
    }
}
