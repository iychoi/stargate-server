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

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.manager.ManagerConfig;
import stargate.managers.transport.layout.ContactNodeSelectionAlgorithms;
import stargate.managers.transport.layout.TransferLayoutAlgorithms;

/**
 *
 * @author iychoi
 */
public class TransportManagerConfig extends ManagerConfig {
    
    private TransferLayoutAlgorithms layoutAlgorithm = TransferLayoutAlgorithms.TRANSFER_LAYOUT_ALGORITHM_STATIC;
    private ContactNodeSelectionAlgorithms nodeSelectionAlgorithm = ContactNodeSelectionAlgorithms.CONTACT_NODE_SELECTION_ALGORITHM_ROUNDROBIN;
    
    public static TransportManagerConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (TransportManagerConfig) JsonSerializer.fromJsonFile(file, TransportManagerConfig.class);
    }
    
    public static TransportManagerConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransportManagerConfig) JsonSerializer.fromJson(json, TransportManagerConfig.class);
    }
    
    public TransportManagerConfig() {
    }
    
    @JsonProperty("transfer_layout")
    public void setLayoutAlgorithm(TransferLayoutAlgorithms layoutAlgorithm) {
        if(layoutAlgorithm == null) {
            throw new IllegalArgumentException("layoutAlgorithm is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.layoutAlgorithm = layoutAlgorithm;
    }
    
    @JsonProperty("transfer_layout")
    public TransferLayoutAlgorithms getLayoutAlgorithm() {
        return this.layoutAlgorithm;
    }
    
    @JsonProperty("contact_node_selection")
    public void setContactNodeSelectionAlgorithm(ContactNodeSelectionAlgorithms nodeSelectionAlgorithm) {
        if(nodeSelectionAlgorithm == null) {
            throw new IllegalArgumentException("nodeSelectionAlgorithm is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.nodeSelectionAlgorithm = nodeSelectionAlgorithm;
    }
    
    @JsonProperty("contact_node_selection")
    public ContactNodeSelectionAlgorithms getContactNodeSelectionAlgorithm() {
        return this.nodeSelectionAlgorithm;
    }
}
