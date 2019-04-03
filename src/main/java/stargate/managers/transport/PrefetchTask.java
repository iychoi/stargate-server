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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.DateTimeUtils;

/**
 *
 * @author iychoi
 */
public class PrefetchTask extends AbstractTransferTask {
    
    private static final Log LOG = LogFactory.getLog(PrefetchTask.class);
    
    protected RecipeChunk recipeChunk;

    public PrefetchTask(String name, TransportManager manager, DataObjectURI uri, String hash, long offset) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null");
        }
        
        if(manager == null) {
            throw new IllegalArgumentException("manager is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        this.name = name;
        this.manager = manager;
        this.uri = uri;
        this.hash = hash;
        this.offset = offset;
        this.priority = TransferTaskPriority.PREFETCH_TASK_PRIORITY_LOW;
        this.creationTime = DateTimeUtils.getTimestamp();
    }
    
    @Override
    public void run() {
        try {
            LOG.debug(String.format("Prefetch task (name: %s, priority: %s) %s - %s (%d)", this.name, this.priority.name(), this.uri.toUri().toASCIIString(), this.hash, this.offset));
            manager.cacheRemoteDataChunk(this.uri, this.hash);
        } catch (IOException ex) {
            LOG.error("IOException", ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
        }
    }
}
