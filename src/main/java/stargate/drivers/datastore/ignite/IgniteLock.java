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
package stargate.drivers.datastore.ignite;

import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import stargate.commons.datastore.AbstractLock;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteLock extends AbstractLock {

    private static final Log LOG = LogFactory.getLog(IgniteLock.class);
    
    private IgniteDataStoreDriver driver;
    private IgniteDataStoreDriverConfig config;
    private IgniteDriver igniteDriver;
    private Ignite ignite;
    private String name;
    private org.apache.ignite.IgniteLock reentrantLock;
    
    public IgniteLock(IgniteDataStoreDriver driver, IgniteDataStoreDriverConfig config, IgniteDriver igniteDriver, String name) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(igniteDriver == null) {
            throw new IllegalArgumentException("igniteDriver is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.driver = driver;
        this.config = config;
        this.igniteDriver = igniteDriver;
        this.ignite = igniteDriver.getIgnite();
        this.name = name;
        
        this.reentrantLock = this.ignite.reentrantLock(name, true, false, true); // failoverSafe, fair, create
    }
    
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void lock() {
        this.reentrantLock.lock();
    }

    @Override
    public boolean tryLock() {
        return this.reentrantLock.tryLock();
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        return this.reentrantLock.tryLock(timeout, unit);
    }

    @Override
    public void unlock() {
        this.reentrantLock.unlock();
    }
}
