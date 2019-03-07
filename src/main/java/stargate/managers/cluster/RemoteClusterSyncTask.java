/*
   Copyright 2018 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package stargate.managers.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.transport.TransportManager;

/**
 *
 * @author iychoi
 */
public class RemoteClusterSyncTask extends TimerTask {
    private static final Log LOG = LogFactory.getLog(RemoteClusterSyncTask.class);
    
    private static final long DEFAULT_TASK_DELAY_SEC = 60*60;
    private static final long DEFAULT_TASK_PERIOD_SEC = 60*60;
    
    private long taskDelaySec = DEFAULT_TASK_DELAY_SEC;
    private long taskPeriodSec = DEFAULT_TASK_PERIOD_SEC;
    
    private ClusterManager clusterManager;
    private TransportManager transportManager;
    
    public RemoteClusterSyncTask(ClusterManager clusterManager, TransportManager transportManager) {
        if(clusterManager == null) {
            throw new IllegalArgumentException("clusterManager is null");
        }
        
        if(transportManager == null) {
            throw new IllegalArgumentException("transportManager is null");
        }
        
        this.clusterManager = clusterManager;
        this.transportManager = transportManager;
    }
    
    public void sync() throws IOException, DriverNotInitializedException {
        try {
            if(this.clusterManager.isLeaderNode()) {
                List<Cluster> syncedRemoteClusters = new ArrayList<Cluster>();
            
                Collection<Cluster> remoteClusters = this.clusterManager.getRemoteClusters();
                for(Cluster remoteCluster : remoteClusters) {
                    LOG.debug(String.format("Synchronizing remote cluster - %s", remoteCluster.getName()));

                    Cluster syncedRemoteCluster = this.transportManager.getRemoteCluster(remoteCluster);
                    LOG.debug(String.format("Remote cluster is synced %s", syncedRemoteCluster.getName()));

                    syncedRemoteClusters.add(syncedRemoteCluster);
                }

                this.clusterManager.updateRemoteClusters(syncedRemoteClusters);
            }
        } catch (IOException ex) {
            throw ex;
        }
    }

    @Override
    public void run() {
        try {
            sync();
        } catch (IOException ex) {
            LOG.error("IOException", ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
        }
    }
    
    public long getDelayMillisec() {
        return DateTimeUtils.getMilliseconds(TimeUnit.SECONDS, this.taskDelaySec);
    }
    
    public long getPeriodMillisec() {
        return DateTimeUtils.getMilliseconds(TimeUnit.SECONDS, this.taskPeriodSec);
    }
}
