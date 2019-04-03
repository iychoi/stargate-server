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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.dataobject.DataObjectURI;

/**
 *
 * @author iychoi
 */
public class TransferScheduler {
    
    private static final Log LOG = LogFactory.getLog(TransferScheduler.class);

    private static final int INITIAL_QUEUE_CAPACITY = 1000;
    
    private ExecutorService transferPoolExecutor;
    private Thread transferSchedulerThread;
    private boolean schedulerRun = true;
    private PriorityBlockingQueue<AbstractTransferTask> priorityTaskQueue;
    private Map<String, AbstractTransferTask> inTransferTasks = new ConcurrentHashMap<String, AbstractTransferTask>();
    private Semaphore taskIngestLock;
    private Map<String, List<PrefetchTask>> pendingPrefetchTasks;
    private Object pendingPrefetchTasksSyncObj = new Object();
    
    public TransferScheduler(int executorPoolSize, long pendingPrefetchTimeoutSec) {
        if(executorPoolSize <= 0) {
            throw new IllegalArgumentException("executorPoolSize is negative");
        }
        
        this.transferPoolExecutor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(executorPoolSize));
        this.priorityTaskQueue = new PriorityBlockingQueue<AbstractTransferTask>(INITIAL_QUEUE_CAPACITY, new TransferTaskComparator());
        this.taskIngestLock = new Semaphore(executorPoolSize);
        this.pendingPrefetchTasks = new PassiveExpiringMap<String, List<PrefetchTask>>(pendingPrefetchTimeoutSec, TimeUnit.SECONDS);
    }
    
    public void start() {
        this.schedulerRun = true;
        this.transferSchedulerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(schedulerRun) {
                        LOG.debug("Fetching a transfer from a queue");
                        AbstractTransferTask task = priorityTaskQueue.take();
                        String hash = task.getHash();
                        
                        if(inTransferTasks.containsKey(hash)) {
                            LOG.debug(String.format("Ignoring a transfer for %s - already in transfer", hash));
                        } else {
                            Runnable r = new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        LOG.debug(String.format("Running a transfer task for %s (%s)", hash, task.getPriority().getStrVal()));
                                        task.run();
                                    } finally {
                                        inTransferTasks.remove(hash);
                                        taskIngestLock.release();
                                    }
                                }
                            };

                            LOG.debug(String.format("Scheduling a transfer for %s (%s)", hash, task.getPriority().getStrVal()));
                            // register to inTransfer set
                            taskIngestLock.acquire();
                            inTransferTasks.put(hash, task);
                            // go
                            transferPoolExecutor.execute(r);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Unknown Exception", ex);
                }
            }
        });
        
        this.transferSchedulerThread.start();
    }
    
    public void stop() {
        this.schedulerRun = false;
        if(this.transferSchedulerThread != null) {
            if(this.transferSchedulerThread.isAlive()) {
                this.transferSchedulerThread.interrupt();
            }
            this.transferSchedulerThread = null;
        }
        
        this.transferPoolExecutor.shutdownNow();
        this.priorityTaskQueue.clear();
        this.inTransferTasks.clear();
    }
    
    public void schedule(AbstractTransferTask task) throws IOException {
        if(task instanceof OnDemandTransferTask) {
            schedule((OnDemandTransferTask) task);
        } else if(task instanceof PrefetchTask) {
            schedule((PrefetchTask) task);
        } else {
            throw new IOException("Unknown task");
        }
    }
    
    public void schedule(OnDemandTransferTask task) {
        LOG.debug("Putting an on-demand transfer into a queue");
        this.priorityTaskQueue.add(task);
        
        // remove prefetch tasks scheduled
        String uriString = task.getDataObjectURI().toUri().toASCIIString();
        synchronized(this.pendingPrefetchTasksSyncObj) {
            List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
            if(prefetchTasks != null) {
                List<PrefetchTask> toBeRemoved = new ArrayList<PrefetchTask>();

                for(PrefetchTask prefetchTask : prefetchTasks) {
                    if(prefetchTask.getHash().equals(task.getHash())) {
                        toBeRemoved.add(prefetchTask);
                    }
                }

                if(!toBeRemoved.isEmpty()) {
                    prefetchTasks.removeAll(toBeRemoved);
                }
            }
        }
    }
    
    public void schedule(PrefetchTask task) {
        String uriString = task.getDataObjectURI().toUri().toASCIIString();
        synchronized(this.pendingPrefetchTasksSyncObj) {
            List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
            if(prefetchTasks == null) {
                prefetchTasks = new ArrayList<PrefetchTask>();
                this.pendingPrefetchTasks.put(uriString, prefetchTasks);
            }

            prefetchTasks.add(task);
            
            // to prevent the item from expiring
            this.pendingPrefetchTasks.put(uriString, prefetchTasks);
        }
    }
    
    public void startPrefetch(DataObjectURI uri, long startOffset, long endOffset) {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }
        
        String uriString = uri.toUri().toASCIIString();
        synchronized(this.pendingPrefetchTasksSyncObj) {
            List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
            if(prefetchTasks != null) {
                LOG.debug(String.format("Putting prefetching transfer starting from offset %d to offset %d into a queue for %s", startOffset, endOffset, uriString));
                
                Iterator<PrefetchTask> iterator = prefetchTasks.iterator();
                while(iterator.hasNext()) {
                    PrefetchTask prefetchTask = iterator.next();
                    long offset = prefetchTask.getOffset();
                    if(offset >= startOffset && offset < endOffset) {
                        this.priorityTaskQueue.add(prefetchTask);
                        iterator.remove();
                    }
                }

                // to prevent the item from expiring                
                this.pendingPrefetchTasks.put(uriString, prefetchTasks);
            }
        }
    }
}
