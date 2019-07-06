package org.elasticsearch.plugin.zentity;

import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.Job;
import io.zentity.resolution.input.Input;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ResolutionAction extends BaseRestHandler {

    @Inject
    ResolutionAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "_zentity/resolution", this);
        controller.registerHandler(POST, "_zentity/resolution/{entity_type}", this);
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
        Boolean includeExplanation = restRequest.paramAsBoolean("_explanation", Job.DEFAULT_INCLUDE_EXPLANATION);
        Boolean includeHits = restRequest.paramAsBoolean("hits", Job.DEFAULT_INCLUDE_HITS);
        Boolean includeQueries = restRequest.paramAsBoolean("queries", Job.DEFAULT_INCLUDE_QUERIES);
        Boolean includeSource = restRequest.paramAsBoolean("_source", Job.DEFAULT_INCLUDE_SOURCE);
        int maxDocsPerQuery = restRequest.paramAsInt("max_docs_per_query", Job.DEFAULT_MAX_DOCS_PER_QUERY);
        int maxHops = restRequest.paramAsInt("max_hops", Job.DEFAULT_MAX_HOPS);
        Boolean pretty = restRequest.paramAsBoolean("pretty", Job.DEFAULT_PRETTY);
        Boolean profile = restRequest.paramAsBoolean("profile", Job.DEFAULT_PROFILE);

        return channel -> {
            try {

                // Validate the request body.
                if (body == null || body.equals(""))
                    throw new ValidationException("Request body is missing.");


                // Parse and validate the job input.
                Input input;
                if (entityType == null || entityType.equals("")) {
                    input = new Input(body, entityType);
                } else {
                    GetResponse getResponse = ModelsAction.getEntityModel(entityType, client);
                    if (!getResponse.isExists())
                        throw new NotFoundException("Entity type '" + entityType + "' not found.");
                    String model = getResponse.getSourceAsString();
                    input = new Input(body, new Model(model));
                }
                if (input.model() == null)
                    throw new ValidationException("An entity model was not given for this request.");

                // Prepare the entity resolution job.
                Job job = new Job(client);
                job.includeAttributes(includeAttributes);
                job.includeExplanation(includeExplanation);
                job.includeHits(includeHits);
                job.includeQueries(includeQueries);
                job.includeSource(includeSource);
                job.maxDocsPerQuery(maxDocsPerQuery);
                job.maxHops(maxHops);
                job.pretty(pretty);
                job.profile(profile);
                job.input(input);

                // Run the entity resolution job.
                String response = job.run();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", response));

            } catch (ValidationException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (NotFoundException e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.NOT_FOUND, e));
            }
        };
    }
}
