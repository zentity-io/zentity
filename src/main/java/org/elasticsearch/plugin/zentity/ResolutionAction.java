package org.elasticsearch.plugin.zentity;

import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.Job;
import io.zentity.resolution.input.Input;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;


public class ResolutionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
                new Route(POST, "_zentity/resolution"),
                new Route(POST, "_zentity/resolution/{entity_type}")
        );
    }

    @Override
    public String getName() {
        return "zentity_resolution_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        String body = restRequest.content().utf8ToString();

        // Parse the request params that will be passed to the job configuration
        String entityType = restRequest.param("entity_type");
        Boolean includeAttributes = restRequest.paramAsBoolean("_attributes", Job.DEFAULT_INCLUDE_ATTRIBUTES);
        Boolean includeErrorTrace = restRequest.paramAsBoolean("error_trace", Job.DEFAULT_INCLUDE_ERROR_TRACE);
        Boolean includeExplanation = restRequest.paramAsBoolean("_explanation", Job.DEFAULT_INCLUDE_EXPLANATION);
        Boolean includeHits = restRequest.paramAsBoolean("hits", Job.DEFAULT_INCLUDE_HITS);
        Boolean includeQueries = restRequest.paramAsBoolean("queries", Job.DEFAULT_INCLUDE_QUERIES);
        Boolean includeScore = restRequest.paramAsBoolean("_score", Job.DEFAULT_INCLUDE_SCORE);
        Boolean includeSeqNoPrimaryTerm = restRequest.paramAsBoolean("_seq_no_primary_term", Job.DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM);
        Boolean includeSource = restRequest.paramAsBoolean("_source", Job.DEFAULT_INCLUDE_SOURCE);
        Boolean includeVersion = restRequest.paramAsBoolean("_version", Job.DEFAULT_INCLUDE_VERSION);
        int maxDocsPerQuery = restRequest.paramAsInt("max_docs_per_query", Job.DEFAULT_MAX_DOCS_PER_QUERY);
        int maxHops = restRequest.paramAsInt("max_hops", Job.DEFAULT_MAX_HOPS);
        String maxTimePerQuery = restRequest.param("max_time_per_query", Job.DEFAULT_MAX_TIME_PER_QUERY);
        Boolean pretty = restRequest.paramAsBoolean("pretty", Job.DEFAULT_PRETTY);
        Boolean profile = restRequest.paramAsBoolean("profile", Job.DEFAULT_PROFILE);

        // Parse any optional search parameters that will be passed to the job configuration.
        // Note: org.elasticsearch.rest.RestRequest doesn't allow null values as default values for integer parameters,
        // which is why the code below handles the integer parameters differently from the others.
        Boolean searchAllowPartialSearchResults = restRequest.paramAsBoolean("search.allow_partial_search_results", Job.DEFAULT_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS);
        Integer searchBatchedReduceSize = Job.DEFAULT_SEARCH_BATCHED_REDUCE_SIZE;
        if (restRequest.hasParam("search.batched_reduce_size"))
            searchBatchedReduceSize = Integer.parseInt(restRequest.param("search.batched_reduce_size"));
        Integer searchMaxConcurrentShardRequests = Job.DEFAULT_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS;
        if (restRequest.hasParam("search.max_concurrent_shard_requests"))
            searchMaxConcurrentShardRequests = Integer.parseInt(restRequest.param("search.max_concurrent_shard_requests"));
        Integer searchPreFilterShardSize = Job.DEFAULT_SEARCH_PRE_FILTER_SHARD_SIZE;
        if (restRequest.hasParam("search.pre_filter_shard_size"))
            searchPreFilterShardSize = Integer.parseInt(restRequest.param("search.pre_filter_shard_size"));
        String searchPreference = restRequest.param("search.preference", Job.DEFAULT_SEARCH_PREFERENCE);
        Boolean searchRequestCache = restRequest.paramAsBoolean("search.request_cache", Job.DEFAULT_SEARCH_REQUEST_CACHE);
        Integer finalSearchBatchedReduceSize = searchBatchedReduceSize;
        Integer finalSearchMaxConcurrentShardRequests = searchMaxConcurrentShardRequests;
        Integer finalSearchPreFilterShardSize = searchPreFilterShardSize;

        return channel -> {
            try {

                // Validate the request body.
                if (body == null || body.equals(""))
                    throw new ValidationException("Request body is missing.");

                // Prepare the entity resolution job.
                Job job = new Job(client);
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
                job.searchBatchedReduceSize(finalSearchBatchedReduceSize);
                job.searchMaxConcurrentShardRequests(finalSearchMaxConcurrentShardRequests);
                job.searchPreFilterShardSize(finalSearchPreFilterShardSize);
                job.searchPreference(searchPreference);
                job.searchRequestCache(searchRequestCache);

                // Parse and validate the job input.
                if (entityType == null || entityType.equals("")) {

                    // TODO: Duplicate of code below. Determine best way to use this code once.
                    Input input = new Input(body);
                    job.input(input);

                    // Run the entity resolution job.
                    String jobResponse = job.run();
                    if (job.failed())
                        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "application/json", jobResponse));
                    else
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", jobResponse));
                } else {
                    ModelsAction.getEntityModel(entityType, client, new RestActionListener<>(channel)  {

                        @Override
                        protected void processResponse(GetResponse getResponse) throws Exception {
                            if (!getResponse.isExists())
                                throw new NotFoundException("Entity type '" + entityType + "' not found.");
                            String model = getResponse.getSourceAsString();

                            // TODO: Duplicate of code above. Determine best way to use this code once.
                            Input input = new Input(body, new Model(model));
                            job.input(input);

                            // Run the entity resolution job.
                            String jobResponse = job.run();
                            if (job.failed())
                                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "application/json", jobResponse));
                            else
                                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", jobResponse));
                        }
                    });
                }

            } catch (ValidationException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            }
        };
    }
}
