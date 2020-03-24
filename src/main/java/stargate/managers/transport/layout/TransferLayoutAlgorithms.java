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
package stargate.managers.transport.layout;

import java.io.IOException;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.service.AbstractService;
import stargate.managers.transport.TransportManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public enum TransferLayoutAlgorithms {
    TRANSFER_LAYOUT_ALGORITHM_MASTER_COPY ("mastercopy"),
    TRANSFER_LAYOUT_ALGORITHM_FAVORITE_ALWAYS ("favorite_always"),
    TRANSFER_LAYOUT_ALGORITHM_FAVORITE_FIRST ("favorite_first"),
    TRANSFER_LAYOUT_ALGORITHM_FAIR ("fair");
    
    private String strVal;
    
    TransferLayoutAlgorithms(String strVal) {
        this.strVal = strVal;
    }
    
    public String getStringVal() {
        return this.strVal;
    }
    
    public static TransferLayoutAlgorithms fromStringVal(String strVal) {
        for(TransferLayoutAlgorithms type : TransferLayoutAlgorithms.values()) {
            if(type.getStringVal().equalsIgnoreCase(strVal)) {
                return type;
            }
        }
        return null;
    }
    
    public AbstractTransferLayoutAlgorithm createInstance(AbstractService service, TransportManager manager, AbstractBigKeyValueStore bigKeyValueStore, AbstractContactNodeSelectionAlgorithm contactNodeSelectionAlgorithm) throws IOException {
        if(!(service instanceof StargateService)) {
            throw new IllegalArgumentException("service is not an instance of StargateService");
        }
        
        return createInstance((StargateService) service, manager, bigKeyValueStore, contactNodeSelectionAlgorithm);
    }
    
    public AbstractTransferLayoutAlgorithm createInstance(StargateService service, TransportManager manager, AbstractBigKeyValueStore bigKeyValueStore, AbstractContactNodeSelectionAlgorithm contactNodeSelectionAlgorithm) throws IOException {
        switch(this) {
            case TRANSFER_LAYOUT_ALGORITHM_MASTER_COPY:
                return new MasterCopyTransferLayoutAlgorithm(service, manager, bigKeyValueStore, contactNodeSelectionAlgorithm);
            case TRANSFER_LAYOUT_ALGORITHM_FAVORITE_ALWAYS:
                return new FavoriteAlwaysTransferLayoutAlgorithm(service, manager, bigKeyValueStore, contactNodeSelectionAlgorithm);
            case TRANSFER_LAYOUT_ALGORITHM_FAVORITE_FIRST:
                return new FavoriteFirstTransferLayoutAlgorithm(service, manager, bigKeyValueStore, contactNodeSelectionAlgorithm);
            case TRANSFER_LAYOUT_ALGORITHM_FAIR:
                return new FairTransferLayoutAlgorithm(service, manager, bigKeyValueStore, contactNodeSelectionAlgorithm);
            default:
                throw new IOException(String.format("Cannot find transfer layout algorithm %s", this.name()));
        }
    }
}
