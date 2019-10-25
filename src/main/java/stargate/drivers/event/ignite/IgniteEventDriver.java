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
package stargate.drivers.event.ignite;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiPredicate;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.event.AbstractEventDriver;
import stargate.commons.event.AbstractEventDriverConfig;
import stargate.commons.event.AbstractEventHandler;
import stargate.commons.event.BulkStargateEvent;
import stargate.commons.event.StargateEvent;
import stargate.commons.event.StargateEventType;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteEventDriver extends AbstractEventDriver {

    private static final Log LOG = LogFactory.getLog(IgniteEventDriver.class);
    
    private IgniteEventDriverConfig config;
    private IgniteDriver igniteDriver;
    private IgniteMessaging msg;
    private boolean listenEvent = true;
    private Map<StargateEventType, Set<AbstractEventHandler>> eventHandlers = new HashMap<StargateEventType, Set<AbstractEventHandler>>();
    private final Object eventHandlersSyncObj = new Object();
    private ExecutorService eventHandlerThreadPool;
    private ExecutorService eventSenderThreadPool;
    
    private final String STARGATE_TOPIC = "STARGATE_TOPIC";
    private final String STARGATE_BULK_TOPIC = "STARGATE_BULK_TOPIC";
    
    public IgniteEventDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteEventDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteEventDriverConfig");
        }
        
        this.config = (IgniteEventDriverConfig) config;
    }
    
    public IgniteEventDriver(AbstractEventDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteEventDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteEventDriverConfig");
        }
        
        this.config = (IgniteEventDriverConfig) config;
    }
    
    public IgniteEventDriver(IgniteEventDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.debug("Initializing Ignite Event Driver");
        
        this.eventHandlerThreadPool = Executors.newFixedThreadPool(this.config.getEventHandlerThreads());
        this.eventSenderThreadPool = Executors.newFixedThreadPool(this.config.getEventSenderThreads());
        
        this.igniteDriver = IgniteDriver.getInstance();
        this.igniteDriver.init();
        
        setIgniteEventHandler();
    }

    @Override
    public synchronized void uninit() throws IOException {
        this.listenEvent = false;
        this.eventHandlers.clear();
        this.eventHandlerThreadPool.shutdownNow();
        this.eventHandlerThreadPool = null;
        this.eventSenderThreadPool.shutdownNow();
        this.eventSenderThreadPool = null;
        
        if(this.igniteDriver != null && this.igniteDriver.isStarted()) {
            this.igniteDriver.uninit();
        }
        
        if(this.igniteDriver != null) {
            this.igniteDriver = null;
        }
        
        super.uninit();
    }
    
    private void setIgniteEventHandler() {
        this.listenEvent = true;
        
        Ignite ignite = this.igniteDriver.getIgnite();
        this.msg = ignite.message();
        
        IgniteBiPredicate<UUID, String> ignitePredicate = new IgniteBiPredicate<UUID, String>() {
            @Override
            public boolean apply(UUID nodeId, String msg) {
                LOG.debug(String.format("Received an event from %s - %s", nodeId.toString(), msg));
                
                try {
                    // msg is an StargateEvent object
                    StargateEvent event = StargateEvent.createInstance(msg);
                    
                    Runnable r = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processEvent(event);
                            } catch (IOException ex) {
                                LOG.error("IOException", ex);
                            }
                        }
                    };
                    
                    eventHandlerThreadPool.execute(r);
            
                    // do not call synchronously
                    //processEvent(event);
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                }
                
                // continue listening
                return listenEvent;
            }
        };
        
        this.msg.localListen(STARGATE_TOPIC, ignitePredicate);
        
        // bulk
        IgniteBiPredicate<UUID, String> ignitePredicateForBulk = new IgniteBiPredicate<UUID, String>() {
            @Override
            public boolean apply(UUID nodeId, String msg) {
                LOG.debug(String.format("Received a bulk event from %s - %s", nodeId.toString(), msg));
                
                try {
                    // msg is a BulkStargateEvent object
                    BulkStargateEvent event = BulkStargateEvent.createInstance(msg);
                    
                    Runnable r = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processBulkEvent(event);
                            } catch (IOException ex) {
                                LOG.error("IOException", ex);
                            }
                        }
                    };
                    
                    eventHandlerThreadPool.execute(r);
            
                    // do not call synchronously
                    //processEvent(event);
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                }
                
                // continue listening
                return listenEvent;
            }
        };
        
        this.msg.localListen(STARGATE_BULK_TOPIC, ignitePredicateForBulk);
    }
    
    @Override
    public void addEventHandler(AbstractEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        StargateEventType[] acceptedTypes = eventHandler.getAcceptedTypes();
        if(acceptedTypes == null) {
            throw new IllegalArgumentException("acceptedTypes is null");
        }
        
        synchronized(this.eventHandlersSyncObj) {
            for(StargateEventType type : acceptedTypes) {
                Set<AbstractEventHandler> handlers = this.eventHandlers.get(type);
                if(handlers == null) {
                    handlers = new HashSet<AbstractEventHandler>();
                }

                handlers.add(eventHandler);
                this.eventHandlers.put(type, handlers);
            }
        }
    }

    @Override
    public void removeEventHandler(AbstractEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        StargateEventType[] acceptedTypes = eventHandler.getAcceptedTypes();
        if(acceptedTypes == null) {
            throw new IllegalArgumentException("acceptedTypes is null");
        }
        
        synchronized(this.eventHandlersSyncObj) {
            for(StargateEventType type : acceptedTypes) {
                Set<AbstractEventHandler> handlers = this.eventHandlers.get(type);
                if(handlers != null) {
                    handlers.remove(eventHandler);
                    this.eventHandlers.put(type, handlers);
                }
            }
        }
    }

    private void processEvent(StargateEvent event) throws IOException {
        if(event == null) {
            throw new IllegalArgumentException("event is null");
        }
        
        LOG.debug(String.format("Process an event from %s - %s", event.getSenderNodeName(), event.toJson()));
            
        Collection<String> receiverNodeNames = event.getReceiverNodeNames();
        String localNodeName = this.igniteDriver.getLocalNodeName();
        if(receiverNodeNames.contains(localNodeName)) {
            Set<AbstractEventHandler> handlers = null;
            
            synchronized(this.eventHandlersSyncObj) {
                StargateEventType eventType = event.getEventType();
                handlers = this.eventHandlers.get(eventType);
            }
            
            // for parallel
            if(handlers != null) {
                for(AbstractEventHandler handler : handlers) {
                    handler.raised(event);
                }
            }
        }
    }
    
    private void processBulkEvent(BulkStargateEvent event) throws IOException {
        if(event == null) {
            throw new IllegalArgumentException("event is null");
        }
        
        String localNodeName = this.igniteDriver.getLocalNodeName();
        
        for(StargateEvent sevent : event.getEvents()) {
            
            LOG.debug(String.format("Process an event from %s - %s", sevent.getSenderNodeName(), event.toJson()));
            Collection<String> receiverNodeNames = sevent.getReceiverNodeNames();
            if(receiverNodeNames.contains(localNodeName)) {
                Set<AbstractEventHandler> handlers = null;
                
                synchronized(this.eventHandlersSyncObj) {
                    StargateEventType eventType = sevent.getEventType();
                    handlers = this.eventHandlers.get(eventType);
                }

                // for parallel
                if(handlers != null) {
                    for(AbstractEventHandler handler : handlers) {
                        handler.raised(sevent);
                    }
                }
            }
        }
    }
    
    @Override
    public void raiseEvent(StargateEvent event) throws IOException, DriverNotInitializedException {
        if(event == null) {
            throw new IllegalArgumentException("event is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.debug(String.format("Raise an event : %s", event.getEventType().toString()));
                    msg.send(STARGATE_TOPIC, event.toJson());
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                }
            }
        };
        
        this.eventSenderThreadPool.execute(r);
    }
    
    @Override
    public void raiseEvents(Collection<StargateEvent> events) throws IOException, DriverNotInitializedException {
        if(events == null) {
            throw new IllegalArgumentException("events is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.debug("Raise a bulk event");
                    BulkStargateEvent event = new BulkStargateEvent(events);
                    msg.send(STARGATE_BULK_TOPIC, event.toJson());
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                }
            }
        };
        
        this.eventSenderThreadPool.execute(r);
    }
}
