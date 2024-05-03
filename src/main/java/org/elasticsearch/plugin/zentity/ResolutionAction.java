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
package org.elasticsearch.plugin.zentity;

import io.zentity.common.AsyncCollectionRunner;
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.Job;
import io.zentity.resolution.input.Input;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ResolutionAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(ResolutionAction.class);
    private static final int MAX_CONCURRENT_JOBS_PER_REQUEST = BulkAction.MAX_CONCURRENT_OPERATIONS_PER_REQUEST;

    // All parameters known to the request
    private static final String PARAM_ENTITY_TYPE = "entity_type";
    private static final String PARAM_PRETTY = "pretty";
    private static final String PARAM_INCLUDE_ATTRIBUTES = "_attributes";
    private static final String PARAM_INCLUDE_ERROR_TRACE = "error_trace";
    private static final String PARAM_INCLUDE_EXPLANATION = "_explanation";
    private static final String PARAM_INCLUDE_HITS = "hits";
    private static final String PARAM_INCLUDE_QUERIES = "queries";
    private static final String PARAM_INCLUDE_SCORE = "_score";
    private static final String PARAM_INCLUDE_SEQ_NO_PRIMARY_TERM = "_seq_no_primary_term";
    private static final String PARAM_INCLUDE_SOURCE = "_source";
    private static final String PARAM_INCLUDE_VERSION = "_version";
    private static final String PARAM_MAX_DOCS_PER_QUERY = "max_docs_per_query";
    private static final String PARAM_MAX_HOPS = "max_hops";
    private static final String PARAM_MAX_TIME_PER_QUERY = "max_time_per_query";
    private static final String PARAM_PROFILE = "profile";
    private static final String PARAM_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS = "search.allow_partial_search_results";
    private static final String PARAM_SEARCH_BATCHED_REDUCE_SIZE = "search.batched_reduce_size";
    private static final String PARAM_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS = "search.max_concurrent_shard_requests";
    private static final String PARAM_SEARCH_PRE_FILTER_SHARD_SIZE = "search.pre_filter_shard_size";
    private static final String PARAM_SEARCH_REQUEST_CACHE = "search.request_cache";
    private static final String PARAM_SEARCH_PREFERENCE = "search.preference";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "_zentity/resolution"),
            new Route(POST, "_zentity/resolution/_bulk"),
            new Route(POST, "_zentity/resolution/{entity_type}"),
            new Route(POST, "_zentity/resolution/{entity_type}/_bulk")
        );
    }

    @Override
    public String getName() {
        return "zentity_resolution_action";
    }

    /**
     * Retrieve a serialized entity model.
     *
     * @param client     The client that will communicate with Elasticsearch.
     * @param entityType The entity type.
     * @param onComplete The action to perform after retrieving the entity model.
     */
    static void getModelString(NodeClient client, String entityType, ActionListener<String> onComplete) {
        ModelsAction.getEntityModel(entityType, client, ActionListener.wrap(
                (res) -> {
                    if (!res.isExists())
                        throw new NotFoundException("Entity type '" + entityType + "' not found.");
                    String modelString = res.getSourceAsString();
                    onComplete.onResponse(modelString);
                },
                onComplete::onFailure
        ));
    }

    /**
     * Construct and return a Job object.
     *
     * @param client    The client that will communicate with Elasticsearch.
     * @param input     The input for the resolution job.
     * @param params    The parameters of the job.
     * @param reqParams The parameters of the request.
     * @return Job
     */
    static Job buildJob(NodeClient client, Input input, Map<String, String> params, Map<String, String> reqParams) {

        // Parse the request params that will be passed to the job configuration
        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, Job.DEFAULT_PRETTY, params, reqParams);
        final boolean includeAttributes = ParamsUtil.optBoolean(PARAM_INCLUDE_ATTRIBUTES, Job.DEFAULT_INCLUDE_ATTRIBUTES, params, reqParams);
        final boolean includeErrorTrace = ParamsUtil.optBoolean(PARAM_INCLUDE_ERROR_TRACE, Job.DEFAULT_INCLUDE_ERROR_TRACE, params, reqParams);
        final boolean includeExplanation = ParamsUtil.optBoolean(PARAM_INCLUDE_EXPLANATION, Job.DEFAULT_INCLUDE_EXPLANATION, params, reqParams);
        final boolean includeHits = ParamsUtil.optBoolean(PARAM_INCLUDE_HITS, Job.DEFAULT_INCLUDE_HITS, params, reqParams);
        final boolean includeQueries = ParamsUtil.optBoolean(PARAM_INCLUDE_QUERIES, Job.DEFAULT_INCLUDE_QUERIES, params, reqParams);
        final boolean includeScore = ParamsUtil.optBoolean(PARAM_INCLUDE_SCORE, Job.DEFAULT_INCLUDE_SCORE, params, reqParams);
        final boolean includeSeqNoPrimaryTerm = ParamsUtil.optBoolean(PARAM_INCLUDE_SEQ_NO_PRIMARY_TERM, Job.DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM, params, reqParams);
        final boolean includeSource = ParamsUtil.optBoolean(PARAM_INCLUDE_SOURCE, Job.DEFAULT_INCLUDE_SOURCE, params, reqParams);
        final boolean includeVersion = ParamsUtil.optBoolean(PARAM_INCLUDE_VERSION, Job.DEFAULT_INCLUDE_VERSION, params, reqParams);
        final int maxDocsPerQuery = ParamsUtil.optInteger(PARAM_MAX_DOCS_PER_QUERY, Job.DEFAULT_MAX_DOCS_PER_QUERY, params, reqParams);
        final int maxHops = ParamsUtil.optInteger(PARAM_MAX_HOPS, Job.DEFAULT_MAX_HOPS, params, reqParams);
        final String maxTimePerQuery = ParamsUtil.optString(PARAM_MAX_TIME_PER_QUERY, Job.DEFAULT_MAX_TIME_PER_QUERY, params, reqParams);
        final boolean profile = ParamsUtil.optBoolean(PARAM_PROFILE, Job.DEFAULT_PROFILE, params, reqParams);

        // Parse any optional search parameters that will be passed to the job configuration.
        final Boolean searchAllowPartialSearchResults = ParamsUtil.optBoolean(PARAM_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS, null, params, reqParams);
        final Integer searchBatchedReduceSize = ParamsUtil.optInteger(PARAM_SEARCH_BATCHED_REDUCE_SIZE, null, params, reqParams);
        final Integer searchMaxConcurrentShardRequests = ParamsUtil.optInteger(PARAM_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS, null, params, reqParams);
        final Integer searchPreFilterShardSize = ParamsUtil.optInteger(PARAM_SEARCH_PRE_FILTER_SHARD_SIZE, null, params, reqParams);
        final Boolean searchRequestCache = ParamsUtil.optBoolean(PARAM_SEARCH_REQUEST_CACHE, null, params, reqParams);
        final String searchPreference = ParamsUtil.optString(PARAM_SEARCH_PREFERENCE, null, params, reqParams);

        // Prepare the entity resolution job.
        Job job = new Job(client);
        job.input(input);
        job.includeAttributes(includeAttributes);
        job.includeErrorTrace(includeErrorTrace);
        job.includeExplanation(includeExplanation);
        job.includeHits(includeHits);
        job.includeQueries(includeQueries);
        job.includeScore(includeScore);
        job.includeSeqNoPrimaryTerm(includeSeqNoPrimaryTerm);
        job.includeSource(includeSource);
        job.includeVersion(includeVersion);
        job.maxDocsPerQuery(maxDocsPerQuery);
        job.maxHops(maxHops);
        job.maxTimePerQuery(maxTimePerQuery);
        job.pretty(pretty);
        job.profile(profile);

        // Optional search parameters
        job.searchAllowPartialSearchResults(searchAllowPartialSearchResults);
        job.searchBatchedReduceSize(searchBatchedReduceSize);
        job.searchMaxConcurrentShardRequests(searchMaxConcurrentShardRequests);
        job.searchPreFilterShardSize(searchPreFilterShardSize);
        job.searchPreference(searchPreference);
        job.searchRequestCache(searchRequestCache);
        return job;
    }

    static void buildJob(NodeClient client, String body, Map<String, String> params, Map<String, String> reqParams, ActionListener<Job> onComplete) {
        if (body == null || body.equals(""))
            throw new BadRequestException("Request body is missing.");

        String entityType = ParamsUtil.optString(PARAM_ENTITY_TYPE, null, params, reqParams);
        if (entityType == null || entityType.equals("")) {

            // If no entity type is given, check if the entity model is embedded in the request, and if so then use it.
            try {
                buildJob(client, new Input(body), body, params, reqParams, onComplete);
            } catch (Exception e) {
                onComplete.onFailure(e);
            }
        } else {

            // If an entity type is given, retrieve the entity model.
            getModelString(client, entityType, ActionListener.wrap(
                    (modelString) -> buildJob(client, new Model(modelString, true), body, params, reqParams, onComplete),
                    onComplete::onFailure
            ));
        }
    }

    static void buildJob(NodeClient client, Model model, String body, Map<String, String> params, Map<String, String> reqParams, ActionListener<Job> onComplete) throws IOException, ValidationException {
        if (body == null || body.equals(""))
            throw new BadRequestException("Request body is missing.");
        Input input = new Input(body, model);
        buildJob(client, input, body, params, reqParams, onComplete);
    }

    static void buildJob(NodeClient client, Input input, String body, Map<String, String> params, Map<String, String> reqParams, ActionListener<Job> onComplete) {
        if (body == null || body.equals(""))
            throw new BadRequestException("Request body is missing.");
        Job job = buildJob(client, input, params, reqParams);
        onComplete.onResponse(job);
    }

    /**
     * Execute Job.run()
     *
     * @param job        The job to run.
     * @param onComplete The action to perform after the job completes.
     */
    static void runJob(Job job, ActionListener<BulkAction.SingleResult> onComplete) {
        job.run(onComplete.delegateFailure(
            (ignored, res) -> {
                BulkAction.SingleResult jobResult = new BulkAction.SingleResult(res, job.failed());
                onComplete.onResponse(jobResult);
            }
        ));
    }

    /**
     * Construct a Job object, and then execute Job.run().
     *
     * @param client     The client that will communicate with Elasticsearch.
     * @param body       The request body.
     * @param params     The job params.
     * @param reqParams  The request params.
     * @param onComplete The action to perform after the job completes.
     */
    static void buildAndRunJob(NodeClient client, String body, Map<String, String> params, Map<String, String> reqParams, ActionListener<BulkAction.SingleResult> onComplete) {
        buildJob(client, body, params, reqParams, onComplete.delegateFailure(
            (ignored, job) -> runJob(job, onComplete)
        ));
    }

    static void delegateJobFailure(ActionListener<BulkAction.SingleResult> delegate, NodeClient client, Exception failure) {
        Job failedJob = new Job(client);
        failedJob.took(0);
        failedJob.failed(true);
        failedJob.error(failure);

        try {
            delegate.onResponse(new BulkAction.SingleResult(failedJob.response(), true));
        } catch (Exception err) {
            // An error occurred when preparing or sending the response.
            delegate.onFailure(err);
        }
    }

    /**
     * Run a collection of resolution jobs concurrently.
     *
     * @param client      The node client.
     * @param modelString The serialized entity model (null is acceptable).
     *                    Deserialized by each job to avoid mutating the model shared between jobs.
     * @param entries     The bulk tuple entries.
     * @param reqParams   The parameters map for the entire request.
     * @param listener    The listener for completion results.
     */
    static void executeBulk(NodeClient client, String modelString, List<Tuple<String, String>> entries, Map<String, String> reqParams, ActionListener<Collection<BulkAction.SingleResult>> listener) {
        BiConsumer<Tuple<String, String>, ActionListener<BulkAction.SingleResult>> jobRunner = (tuple, delegate) -> {
            ActionListener<Job> onJobBuilt = ActionListener.wrap(
                    (job) -> runJob(job, delegate),
                    (ex) -> delegateJobFailure(delegate, client, ex)
            );

            // Parse params
            String body = tuple.v2();
            Map<String, String> params;
            try {
                params = Json.toStringMap(tuple.v1());
            } catch (Exception e) {
                delegateJobFailure(delegate, client, e);
                return;
            }

            // Handle job building errors, but not job running as those should be considered fatal
            try {
                if (modelString == null) {
                    // This request did not have an entity model in the URL. Retrieve the entity model for this job.
                    buildJob(client, body, params, reqParams, onJobBuilt);
                } else if (params.get(PARAM_ENTITY_TYPE) != null && !java.util.Objects.equals(params.get(PARAM_ENTITY_TYPE), reqParams.get(PARAM_ENTITY_TYPE))) {
                    // This job overrides the entity model in the URL. Retrieve the right entity model for the job.
                    buildJob(client, body, params, reqParams, onJobBuilt);
                } else {
                    // This job uses the entity model from the URL.
                    buildJob(client, new Model(modelString, false), body, params, reqParams, onJobBuilt);
                }
            } catch (Exception e) {
                delegateJobFailure(delegate, client, e);
            }
        };

        // Treat all failures as fatal and fail the request as quickly as possible.
        // Jobs that have handleable errors should attempt to complete normally with a structured response.
        AsyncCollectionRunner<Tuple<String, String>, BulkAction.SingleResult> collectionRunner
            = new AsyncCollectionRunner<>(entries, jobRunner, MAX_CONCURRENT_JOBS_PER_REQUEST, true);

        collectionRunner.run(listener);
    }

    /**
     * Run a collection of resolution jobs concurrently.
     *
     * If an entity model was given in the URL, then retrieve the entity model once before running the jobs.
     * Otherwise retrieve the entity model for each job run.
     *
     * @param client The node client.
     * @param entries The bulk tuple entries.
     * @param reqParams The parameters map for the entire request.
     * @param onComplete The listener for completion results.
     */
    static void runBulk(NodeClient client, List<Tuple<String, String>> entries, Map<String, String> reqParams, ActionListener<BulkAction.BulkResult> onComplete) {
        final long startTime = System.nanoTime();

        ActionListener<Collection<BulkAction.SingleResult>> delegate = onComplete.delegateFailure(
                (ignored, results) -> {
                    List<String> items = results.stream().map((res) -> res.response).collect(Collectors.toList());
                    boolean errors = results.stream().anyMatch((res) -> res.failed);
                    long took = Duration.ofNanos(System.nanoTime() - startTime).toMillis();
                    onComplete.onResponse(new BulkAction.BulkResult(items, errors, took));
                }
        );

        String entityType = ParamsUtil.optString(PARAM_ENTITY_TYPE, null, emptyMap(), reqParams);
        if (entityType == null) {

            // An entity type was not given in the URL.
            // Each job may have its own entity model.
            executeBulk(client, null, entries, reqParams, delegate);
        } else {

            // An entity type was given in the URL.
            // One entity model will be used for all jobs (unless overridden by any jobs).
            // Retrieve the entity model once before running any jobs.
            getModelString(client, entityType, ActionListener.wrap(
                    (modelString) -> executeBulk(client, modelString, entries, reqParams, delegate),
                    onComplete::onFailure
            ));
        }
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Validate the request body.
        if (!restRequest.hasContent())
            throw new BadRequestException("Request body is missing.");

        // would be better to handle incoming content as a stream for bulk,
        // but alas, unsure how to use the StreamInput format
        final String body = restRequest.content().utf8ToString();

        // Read all possible parameters into a map so that the handler knows we've consumed them
        // and all other unknowns will be thrown as unrecognized
        Map<String, String> reqParams = ParamsUtil.readAll(
            restRequest,
            PARAM_ENTITY_TYPE,
            PARAM_PRETTY,
            PARAM_INCLUDE_ATTRIBUTES,
            PARAM_INCLUDE_ERROR_TRACE,
            PARAM_INCLUDE_EXPLANATION,
            PARAM_INCLUDE_HITS,
            PARAM_INCLUDE_QUERIES,
            PARAM_INCLUDE_SCORE,
            PARAM_INCLUDE_SEQ_NO_PRIMARY_TERM,
            PARAM_INCLUDE_SOURCE,
            PARAM_INCLUDE_VERSION,
            PARAM_MAX_DOCS_PER_QUERY,
            PARAM_MAX_HOPS,
            PARAM_MAX_TIME_PER_QUERY,
            PARAM_PROFILE,
            PARAM_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS,
            PARAM_SEARCH_BATCHED_REDUCE_SIZE,
            PARAM_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS,
            PARAM_SEARCH_PRE_FILTER_SHARD_SIZE,
            PARAM_SEARCH_REQUEST_CACHE,
            PARAM_SEARCH_PREFERENCE
        );

        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, Job.DEFAULT_PRETTY, reqParams, emptyMap());

        return channel -> {
            Consumer<Exception> errorHandler = (e) -> ZentityPlugin.sendResponseError(channel, logger, e);
            try {
                boolean isBulkRequest = restRequest.path().endsWith("/_bulk");
                if (isBulkRequest) {

                    // Run bulk jobs
                    List<Tuple<String, String>> entries = BulkAction.splitBulkEntries(body);
                    runBulk(client, entries, reqParams, ActionListener.wrap(
                        (bulkResult) -> {
                            String json = BulkAction.bulkResultToJson(bulkResult);
                            if (pretty)
                                json = Json.pretty(json);
                            channel.sendResponse(new RestResponse(RestStatus.OK, "application/json", json));
                        },
                        errorHandler
                    ));
                } else {

                    // Run single job
                    // Prepare the entity resolution job
                    buildAndRunJob(client, body, reqParams, emptyMap(), ActionListener.wrap(
                        (jobResult) -> {
                            if (jobResult.failed)
                                channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "application/json", jobResult.response));
                            else
                                channel.sendResponse(new RestResponse(RestStatus.OK, "application/json", jobResult.response));
                        },
                        errorHandler
                    ));
                }

            } catch (Exception e) {
                errorHandler.accept(e);
            }
        };
    }

}
