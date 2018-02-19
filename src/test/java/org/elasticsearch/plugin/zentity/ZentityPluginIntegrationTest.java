package org.elasticsearch.plugin.zentity;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class ZentityPluginIntegrationTest extends ESIntegTestCase {

    private boolean clusterReconfigured = false;

    private void setupCluster() {
        if (!this.clusterReconfigured) {
            ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put("discovery.zen.minimum_master_nodes", 1))
                    .get();
            this.clusterReconfigured = true;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ZentityPlugin.class);
    }

    public void testPluginIsLoaded() {
        setupCluster();
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugins(true).get();
        for (NodeInfo nodeInfo : response.getNodes()) {
            boolean pluginFound = false;
            for (PluginInfo pluginInfo : nodeInfo.getPlugins().getPluginInfos()) {
                if (pluginInfo.getName().equals(ZentityPlugin.class.getName())) {
                    pluginFound = true;
                    break;
                }
            }
            assertThat(pluginFound, is(true));
        }
    }
}