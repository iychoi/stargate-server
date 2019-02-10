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
package stargate.managers.transport;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.schedule.DistributedTask;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class DataTransferTask extends DistributedTask {
    
    private static final Log LOG = LogFactory.getLog(DataTransferTask.class);
    
    class DataTransferTaskCallable implements Callable<TransferResult> {

        private TransferEvent event;
                
        DataTransferTaskCallable(TransferEvent event) {
            if(event == null) {
                throw new IllegalArgumentException("event is null");
            }
            
            this.event = event;
        }

        @Override
        public TransferResult call() {
            LOG.debug(String.format("Processing a data transfer task for - %s", this.event.toString()));
            
            Collection<String> dataSourceNodeNames = this.event.getDataSourceNodeNames();
                
            if(dataSourceNodeNames.size() != 1) {
                LOG.warn(String.format("There are multiple sources for %s", this.event.getDataObjectURI().toUri().toASCIIString()));
            }

            String dataSourceNodeName = null;
            for(String nodeName : dataSourceNodeNames) {
                dataSourceNodeName = nodeName;
                break;
            }
            
            try {
                StargateService stargateInstance = StargateService.getInstance();
                TransportManager transportManager = stargateInstance.getTransportManager();
                ClusterManager clusterManager = stargateInstance.getClusterManager();
                
                DataObjectURI uri = this.event.getDataObjectURI();
                String clusterName = uri.getClusterName();
                Cluster remoteCluster = clusterManager.getRemoteCluster(clusterName);
                
                Node remoteNode = remoteCluster.getNode(dataSourceNodeName);
                
                transportManager.transferDataChunk(remoteNode, this.event.getHash());
                return new TransferResult(this.event, dataSourceNodeName, DateTimeUtils.getTimestamp(), true);
            } catch (ServiceNotStartedException ex) {
                LOG.error(ex);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
            } catch (IOException ex) {
                LOG.error(ex);
            }
            return new TransferResult(this.event, dataSourceNodeName, DateTimeUtils.getTimestamp(), false);
        }
    }
    
    public DataTransferTask(String nodeName, TransferEvent event) {
        super("DataTransferTask", null, event, nodeName);
        
        DataTransferTaskCallable runnable = new DataTransferTaskCallable(event);
        super.setCallable(runnable);
    }
    
    public TransferResult getTransferResult() throws IOException {
        Future<?> future = super.getFuture();
        
        try {
            // wait
            Collection<TransferResult> results = (Collection<TransferResult>) future.get();
            LOG.debug("Waiting for the task is done");
            
            for(TransferResult result : results) {
                return result;
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
