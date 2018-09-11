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
package stargate.drivers.ignite;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

/**
 *
 * @author iychoi
 */
public class IgniteDriver {

    private static final Log LOG = LogFactory.getLog(IgniteDriver.class);
    
    public static final String PERSISTENT_REGION_NAME = "PERSISTENT_REGION";
    public static final String VOLATILE_REGION_NAME = "VOLATILE_REGION";
    
    private static IgniteDriver instance;
    
    private boolean initialized = false;
    private int initCount = 0;
    private Ignite igniteInstance;
    private Thread activeCheckThread;
    private Lock clusterActivationLock = new ReentrantLock();
    private Condition clusterActivationCondition = this.clusterActivationLock.newCondition();
    private boolean checkActive = true;
    
    public static IgniteDriver getInstance() {
        synchronized (IgniteDriver.class) {
            if(instance == null) {
                instance = new IgniteDriver();
            }
            return instance;
        }
    }
    
    IgniteDriver() {
    }
    
    public synchronized void init() throws IOException {
        if(!this.initialized) {
            LOG.info("Initializing Ignite Master Driver");
            
            IgniteConfiguration igniteConfig = new IgniteConfiguration();
            
            // discovery
            IgniteDiscoverySpi discoveryConfig = getTCPMulticastIPFinderConfig();
            igniteConfig.setDiscoverySpi(discoveryConfig);
            
            // datastore
            DataStorageConfiguration dataStoreConfig = getDataStoreConfig();
            igniteConfig.setDataStorageConfiguration(dataStoreConfig);
            
            // task ordering
            CollisionSpi colConfig = getQueueConfig();
            igniteConfig.setCollisionSpi(colConfig);
            
            this.igniteInstance = Ignition.start(igniteConfig);
            
            LOG.info("Stargate is waiting for activating Ignite cluster");
            runChecker();

            this.clusterActivationLock.lock();
            try {
                this.clusterActivationCondition.await();
            } catch (InterruptedException ex) {
                LOG.error("activation waiting is interrupted");
            } finally {
                this.clusterActivationLock.unlock();
            }
            
            this.initialized = true;
        }
        
        this.initCount++;
    }
    
    private void runChecker() {
        this.checkActive = true;
        this.activeCheckThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // to make condition await
                    Thread.sleep(1000);
                    boolean first = true;
                    
                    while(checkActive) {
                        IgniteCluster cluster = igniteInstance.cluster();
                        boolean active = cluster.active();
                        if(active) {
                            LOG.info("Detected Ignite cluster is active!");
                            clusterActivationLock.lock();
                            try {
                                LOG.info("Ignite cluster is active!");
                                clusterActivationCondition.signal();
                            } finally {
                                clusterActivationLock.unlock();
                            }
                            break;
                        } else {
                            if(first) {
                                LOG.info("Ignite cluster is inactive!");
                                first = false;
                            }
                            Thread.sleep(1000);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error(ex);
                }
            }
        });
        this.activeCheckThread.start();
    }
    
    public synchronized void uninit() throws IOException {
        this.initCount--;
        
        this.checkActive = false;
        if(this.activeCheckThread != null) {
            if(this.activeCheckThread.isAlive()) {
                this.activeCheckThread.interrupt();
            }
            this.activeCheckThread = null;
        }
        
        if(this.initCount <= 0) {
            if(!this.initialized) {
                if(this.igniteInstance != null) {
                    this.igniteInstance.close();
                    this.igniteInstance = null;
                }
                
                this.initialized = false;

                LOG.info("Ignite Master Driver is uninitialized");
            }
        }
    }
    
    public boolean isStarted() {
        return this.initialized;
    }
    
    private DataStorageConfiguration getDataStoreConfig() {
        // DATA STORAGE
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        
        // persistent
        DataRegionConfiguration persistentDataRegConf = new DataRegionConfiguration();
        persistentDataRegConf.setName(PERSISTENT_REGION_NAME);
        persistentDataRegConf.setPersistenceEnabled(true);
        
        // volatile
        DataRegionConfiguration volatileDataRegConf = new DataRegionConfiguration();
        volatileDataRegConf.setName(VOLATILE_REGION_NAME);
        volatileDataRegConf.setPersistenceEnabled(false);
        
        dsCfg.setDataRegionConfigurations(volatileDataRegConf);
        dsCfg.setDefaultDataRegionConfiguration(persistentDataRegConf);
        
        return dsCfg;
    }
    
    private IgniteDiscoverySpi getTCPMulticastIPFinderConfig() {
        // DISCOVERY SPI
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        
        TcpDiscoveryIpFinder tdif = new TcpDiscoveryMulticastIpFinder();
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 47500);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.1", 47501);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.1", 47502);
        addresses.add(addr1);
        addresses.add(addr2);
        addresses.add(addr3);
        tdif.registerAddresses(addresses);
        
        discoSpi.setIpFinder(tdif);
        return discoSpi;
    }
    
    private CollisionSpi getQueueConfig() {
        // PRIORITY ORDERING
        PriorityQueueCollisionSpi collisionSpi = new PriorityQueueCollisionSpi();
        return collisionSpi;
    }
    
    public Ignite getIgnite() {
        return this.igniteInstance;
    }
}
