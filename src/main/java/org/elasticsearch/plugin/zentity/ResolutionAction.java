package org.elasticsearch.plugin.zentity;

import io.zentity.common.AsyncCollectionRunner;
import io.zentity.common.Json;
import io.zentity.common.StreamUtil;
import io.zentity.model.Model;
import io.zentity.resolution.Job;
import io.zentity.resolution.input.Input;
import joptsimple.internal.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.time.Duration;
import java.util.Arrays;
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

    private static final int MAX_CONCURRENT_JOBS_PER_REQUEST = 100;

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

    static void getInput(NodeClient client, String entityType, String body, ActionListener<Input> onComplete) {
        // Validate the request body.
        if (body == null || body.equals("")) {
            onComplete.onFailure(new BadRequestException("Request body is missing."));
            return;
        }

        // Parse and validate the job input.
        if (entityType == null || entityType.equals("")) {
            try {
                onComplete.onResponse(new Input(body));
                return;
            } catch (Exception ex) {
                onComplete.onFailure(ex);
                return;
            }
        }

        ModelsAction.getEntityModel(entityType, client, ActionListener.wrap(
            (res) -> {
                if (!res.isExists()) {
                    throw new NotFoundException("Entity type '" + entityType + "' not found.");
                }

                String model = res.getSourceAsString();
                Input input = new Input(body, new Model(model, true));
                onComplete.onResponse(input);
            },
            onComplete::onFailure
        ));
    }

    static Job buildJob(NodeClient client, Input input, Map<String, String> params, Map<String, String> reqParams) {
        // Parse the request params that will be passed to the job configuration
        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, Job.DEFAULT_INCLUDE_ATTRIBUTES, params, reqParams);
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
        final String entityType = ParamsUtil.optString(PARAM_ENTITY_TYPE, null, params, reqParams);
        getInput(client, entityType, body, ActionListener.wrap(
            (input) -> {
                Job job = buildJob(client, input, params, reqParams);
                onComplete.onResponse(job);
            },
            onComplete::onFailure
        ));
    }

    static void runJob(Job job, ActionListener<JobResult> onComplete) {
        job.run(ActionListener.delegateFailure(
            onComplete,
            (ignored, res) -> {
                JobResult jobResult = new JobResult(res, job.failed());
                onComplete.onResponse(jobResult);
            }
        ));
    }

    static void buildAndRunJob(NodeClient client, String body, Map<String, String> params, Map<String, String> reqParams, ActionListener<JobResult> onComplete) {
        buildJob(client, body, params, reqParams, ActionListener.delegateFailure(
            onComplete,
            (ignored, job) -> runJob(job, onComplete)
        ));
    }

    static void delegateJobFailure(ActionListener<JobResult> delegate, NodeClient client, Exception failure) {
        Job failedJob = new Job(client);
        failedJob.took(0);
        failedJob.failed(true);
        failedJob.error(failure);

        try {
            delegate.onResponse(new JobResult(failedJob.response(), true));
        } catch (Exception err) {
            // An error occurred when preparing or sending the response.
            delegate.onFailure(err);
        }
    }

    static void executeBulk(NodeClient client, List<Tuple<String, String>> entries, Map<String, String> reqParams, ActionListener<Collection<JobResult>> listener) {
        BiConsumer<Tuple<String, String>, ActionListener<JobResult>> jobRunner = (tuple, delegate) -> {
            Map<String, String> params;
            try {
                params = Json.toStringMap(tuple.v1());
            } catch (Exception e) {
                delegateJobFailure(delegate, client, e);
                return;
            }

            buildAndRunJob(client, tuple.v2(), params, reqParams, ActionListener.delegateResponse(
                delegate,
                (ignored, ex) -> delegateJobFailure(delegate, client, ex)
            ));
        };

        // Treat all failures as fatal and fail the request as quickly as possible
        // Jobs that have handleable errors should attempt to complete normally with a structured response
        AsyncCollectionRunner<Tuple<String, String>, JobResult> collectionRunner
            = new AsyncCollectionRunner<>(entries, jobRunner, MAX_CONCURRENT_JOBS_PER_REQUEST, true);

        collectionRunner.run(listener);
    }

    /**
     * Run a collection of jobs concurrently.
     *
     * @param client The node client.
     * @param entries The bulk tuple entries.
     * @param reqParams The parameters map for the entire request.
     * @param onComplete The listener for completion results.
     */
    static void runBulk(NodeClient client, List<Tuple<String, String>> entries, Map<String, String> reqParams, ActionListener<BulkResult> onComplete) {
        final long startTime = System.nanoTime();

        executeBulk(client, entries, reqParams, ActionListener.delegateFailure(
            onComplete,
            (ignored, results) -> {
                List<String> items = results.stream().map((res) -> res.response).collect(Collectors.toList());
                boolean errors = results.stream().anyMatch((res) -> res.failed);
                long took = Duration.ofNanos(System.nanoTime() - startTime).toMillis();
                onComplete.onResponse(new BulkResult(items, errors, took));
            }
        ));
    }

    static String bulkResultToJson(BulkResult result) {
        return "{" +
            Json.quoteString("took") + ":" + result.took +
            "," + Json.quoteString("errors") + ":" + result.errors +
            "," + Json.quoteString("items") + ":" + Strings.join(result.items, ",") +
            "}";
    }

    static List<Tuple<String, String>> splitBulkEntries(String body) {
        String[] lines = body.split("\\n");
        if (lines.length % 2 != 0) {
            throw new BadRequestException("Bulk request must have repeating pairs of params and resolution body on separate lines.");
        }

        return Arrays.stream(lines)
            .flatMap(StreamUtil.tupleFlatmapper())
            .collect(Collectors.toList());
    }


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        if (!restRequest.hasContent()) {
            // Validate the request body.
            throw new BadRequestException("Request body is missing.");
        }

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

        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, Job.DEFAULT_INCLUDE_ATTRIBUTES, reqParams, emptyMap());

        return channel -> {
            boolean isBulkRequest = restRequest.path().endsWith("_bulk");

            Consumer<Exception> errorHandler = (e) -> ZentityPlugin.sendResponseError(channel, logger, e);

            try {
                if (isBulkRequest) {
                    List<Tuple<String, String>> entries = splitBulkEntries(body);
                    runBulk(client, entries, reqParams, ActionListener.wrap(
                        (bulkResult) -> {
                            String json = bulkResultToJson(bulkResult);
                            if (pretty) {
                                json = Json.pretty(json);
                            }

                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", json));
                        },
                        errorHandler
                    ));
                } else {
                    // Prepare the entity resolution job.
                    buildAndRunJob(client, body, reqParams, emptyMap(), ActionListener.wrap(
                        (jobResult) -> {
                            if (jobResult.failed)
                                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "application/json", jobResult.response));
                            else
                                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", jobResult.response));
                        },
                        errorHandler
                    ));
                }

            } catch (Exception e) {
                errorHandler.accept(e);
            }
        };
    }

    /**
     * Small wrapper around a {@link Job} response.
     */
    static final class JobResult {
        final String response;
        final boolean failed;

        JobResult(String response, boolean failed) {
            this.response = response;
            this.failed = failed;
        }
    }

    /**
     * A wrapper for a collection of {@link Job} responses.
     */
    static final class BulkResult {
        final List<String> items;
        final boolean errors;
        final long took;

        BulkResult(List<String> items, boolean errors, long took) {
            this.items = items;
            this.errors = errors;
            this.took = took;
        }
    }
}
