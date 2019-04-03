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

import stargate.commons.dataobject.DataObjectURI;

/**
 *
 * @author iychoi
 */
public abstract class AbstractTransferTask implements Runnable {
    
    protected String name;
    protected TransportManager manager;
    protected DataObjectURI uri;
    protected String hash;
    protected long offset;
    protected TransferTaskPriority priority;
    protected long creationTime;
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return this.name;
    }
    
    public TransportManager getTransportManager() {
        return this.manager;
    }
    
    public DataObjectURI getURI() {
        return this.uri;
    }
    
    public String getHash() {
        return this.hash;
    }
    
    public long getOffset() {
        return this.offset;
    }
    
    public void setPriority(TransferTaskPriority priority) {
        this.priority = priority;
    }
    
    public TransferTaskPriority getPriority() {
        return this.priority;
    }
    
    public long getCreationTime() {
        return this.creationTime;
    }
    
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }
}
