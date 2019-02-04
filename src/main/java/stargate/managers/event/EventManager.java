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
package stargate.managers.event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.NullDriver;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class EventManager extends AbstractManager<NullDriver> {
    
    private static final Log LOG = LogFactory.getLog(EventManager.class);
    
    private static EventManager instance;
    private List<AbstractStargateEventHandler> stargateEventHandlers = new ArrayList<AbstractStargateEventHandler>();
    private BlockingQueue<StargateEvent> eventQueue = new LinkedBlockingDeque<StargateEvent>();
    private Thread eventDispatchThread;
    private boolean dispatchEvent = true;
    protected long lastUpdateTime;
    
    public static EventManager getInstance(StargateService service) throws ManagerNotInstantiatedException {
        synchronized (EventManager.class) {
            if(instance == null) {
                instance = new EventManager(service);
            }
            return instance;
        }
    }
    
    public static EventManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (EventManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("EventManager is not started");
            }
            return instance;
        }
    }
    
    EventManager(StargateService service) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        this.setService(service);
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
        
        runEventDispatchThread();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.dispatchEvent = false;
        this.eventQueue.clear();
        if(this.eventDispatchThread != null) {
            if(this.eventDispatchThread.isAlive()) {
                this.eventDispatchThread.interrupt();
            }
            this.eventDispatchThread = null;
        }
        
        this.stargateEventHandlers.clear();
        
        super.stop();
    }
    
    private void runEventDispatchThread() {
        this.dispatchEvent = true;
        this.eventDispatchThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(dispatchEvent) {
                        StargateEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
                        if(event != null) {
                            LOG.debug(String.format("Dequeued an event : %s", event.getEventType().toString()));
                            processStargateEvent(event);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error(ex);
                }
            }
        });
        this.eventDispatchThread.start();
    }
    
    private void processStargateEvent(StargateEvent event) throws IOException {
        for(AbstractStargateEventHandler handler: this.stargateEventHandlers) {
            if(handler.accept(event.getEventType())) {
                handler.raised(this, event);
            }
        }
    }
    
    public synchronized void raiseStargateEvent(StargateEvent event) throws InterruptedException {
        LOG.debug(String.format("Enqueue an event : %s", event.getEventType().toString()));
        this.eventQueue.put(event);
        
        // update
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
