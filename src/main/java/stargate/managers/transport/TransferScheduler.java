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
import stargate.commons.utils.DateTimeUtils;

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
    
    private int numPendingPrefetchTasks = 0;
    private int numQueuedTasks = 0;
    private int numInTransferTasks = 0;
    
    public TransferScheduler(int transferThreads, long pendingPrefetchTimeoutSec) {
        if(transferThreads <= 0) {
            throw new IllegalArgumentException("transferThreads is negative");
        }
        
        this.transferPoolExecutor = new ThreadPoolExecutor(transferThreads, transferThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(transferThreads));
        this.priorityTaskQueue = new PriorityBlockingQueue<AbstractTransferTask>(INITIAL_QUEUE_CAPACITY, new TransferTaskComparator());
        this.taskIngestLock = new Semaphore(transferThreads);
        this.pendingPrefetchTasks = new PassiveExpiringMap<String, List<PrefetchTask>>(pendingPrefetchTimeoutSec, TimeUnit.SECONDS);
    }
    
    public synchronized void start() {
        LOG.debug("Transfer scheduler starts");
        
        this.schedulerRun = true;
        this.transferSchedulerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.debug("Transfer scheduler thread is starting");
                    while(schedulerRun) {
                        taskIngestLock.acquire();
                        
                        LOG.debug("Fetching a transfer from a queue");
                        AbstractTransferTask task = priorityTaskQueue.take();
                        String hash = task.getHash();
                        
                        numQueuedTasks--;
                        
                        
                        if(inTransferTasks.containsKey(hash)) {
                            LOG.debug(String.format("Ignoring a transfer for %s - already in transfer", hash));
                            taskIngestLock.release();
                        } else {
                            Runnable r = new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        String priStr = task.getPriority().getStrVal();
                                        LOG.debug(String.format("Running a transfer task for %s (%s)", hash, priStr));
                                        task.setStartedTime(DateTimeUtils.getTimestamp());
                                        task.run();
                                        
                                        String creationTime = DateTimeUtils.getDateTimeString(task.getCreationTime());
                                        String scheduledTime = DateTimeUtils.getDateTimeString(task.getScheduledTime());
                                        String startedTime = DateTimeUtils.getDateTimeString(task.getStartedTime());
                                        LOG.info(String.format("Transfer task for %s (%s) finished - created(%s), scheduled(%s), started(%s)", hash, priStr, creationTime, scheduledTime, startedTime));
                                    } finally {
                                        inTransferTasks.remove(hash);
                                        numInTransferTasks--;
                                        
                                        taskIngestLock.release();
                                        
                                        printCurrentStates();
                                    }
                                }
                            };

                            LOG.debug(String.format("Scheduling a transfer for %s (%s)", hash, task.getPriority().getStrVal()));
                            // register to inTransfer set
                            inTransferTasks.put(hash, task);
                            
                            numInTransferTasks++;
                            
                            // go
                            transferPoolExecutor.execute(r);
                            
                            printCurrentStates();
                        }
                    }
                    LOG.debug("Transfer scheduler thread is terminating");
                } catch (Exception ex) {
                    LOG.error("Unknown Exception", ex);
                }
            }
        });
        
        this.transferSchedulerThread.start();
    }
    
    public synchronized void stop() {
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
        
        LOG.debug("Transfer scheduler finished");
    }
    
    public synchronized void schedule(AbstractTransferTask task) throws IOException {
        if(task instanceof OnDemandTransferTask) {
            schedule((OnDemandTransferTask) task);
        } else if(task instanceof PrefetchTask) {
            schedule((PrefetchTask) task);
        } else {
            throw new IOException("Unknown task");
        }
    }
    
    public synchronized void schedule(OnDemandTransferTask task) {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        LOG.debug("Putting an on-demand transfer into a queue");
        task.setScheduledTime(DateTimeUtils.getTimestamp());
        this.priorityTaskQueue.add(task);
        this.numQueuedTasks++;
        
        // remove prefetch tasks scheduled
        String uriString = task.getDataObjectURI().toUri().toASCIIString();
        List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
        if(prefetchTasks != null) {
            Iterator<PrefetchTask> iterator = prefetchTasks.iterator();
            while(iterator.hasNext()) {
                PrefetchTask prefetchTask = iterator.next();
                if(prefetchTask.getHash().equals(task.getHash())) {
                    // remove
                    iterator.remove();
                    this.numPendingPrefetchTasks--;
                }
            }

            if(prefetchTasks.isEmpty()) {
                this.pendingPrefetchTasks.remove(uriString);
            }
        }
        
        printCurrentStates();
    }
    
    public synchronized void schedule(PrefetchTask task) {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        String uriString = task.getDataObjectURI().toUri().toASCIIString();
        List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
        boolean taskExists = false;

        if(prefetchTasks == null) {
            prefetchTasks = new ArrayList<PrefetchTask>();
        } else {
            for(PrefetchTask prefetchTask : prefetchTasks) {
                if(prefetchTask.getHash().equals(task.getHash())) {
                    // has
                    taskExists = true;
                    break;
                }
            }
        }

        if(!taskExists) {
            prefetchTasks.add(task);
            this.numPendingPrefetchTasks++;

            this.pendingPrefetchTasks.put(uriString, prefetchTasks);
        }
        
        printCurrentStates();
    }
    
    public synchronized void startPrefetch(DataObjectURI uri, long startOffset, long prefetchWindowSize) {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }
        
        if(prefetchWindowSize < 0) {
            throw new IllegalArgumentException("prefetchWindowSize is negative");
        }
        
        long endOffset = 0;
        if(prefetchWindowSize == 0) {
            endOffset = Long.MAX_VALUE;
        } else {
            endOffset = startOffset + prefetchWindowSize;
        }
        
        String uriString = uri.toUri().toASCIIString();
        List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
        if(prefetchTasks != null) {
            LOG.debug(String.format("Putting prefetching transfer starting from offset %d to offset %d into a queue for %s", startOffset, endOffset, uriString));

            Iterator<PrefetchTask> iterator = prefetchTasks.iterator();
            while(iterator.hasNext()) {
                PrefetchTask prefetchTask = iterator.next();
                long offset = prefetchTask.getOffset();
                if(offset >= startOffset && offset < endOffset) {
                    prefetchTask.setScheduledTime(DateTimeUtils.getTimestamp());
                    this.priorityTaskQueue.add(prefetchTask);
                    this.numQueuedTasks++;

                    iterator.remove();
                    this.numPendingPrefetchTasks--;
                }
            }

            if(prefetchTasks.isEmpty()) {
                this.pendingPrefetchTasks.remove(uriString);
            } else {
                // to prevent the item from expiring
                this.pendingPrefetchTasks.put(uriString, prefetchTasks);
            }
        }
        
        printCurrentStates();
    }
    
    public void startPrefetch(DataObjectURI uri, long startOffset, long prefetchWindowSize, int count) {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }
        
        if(prefetchWindowSize < 0) {
            throw new IllegalArgumentException("prefetchWindowSize is negative");
        }
        
        if(count < 0) {
            throw new IllegalArgumentException("count is negative");
        }
        
        if(count == 0) {
            return;
        }
        
        long endOffset = 0;
        if(prefetchWindowSize == 0) {
            endOffset = Long.MAX_VALUE;
        } else {
            endOffset = startOffset + prefetchWindowSize;
        }
        
        String uriString = uri.toUri().toASCIIString();
        List<PrefetchTask> prefetchTasks = this.pendingPrefetchTasks.get(uriString);
        if(prefetchTasks != null) {
            LOG.debug(String.format("Putting prefetching transfer starting from offset %d to offset %d into a queue for %s", startOffset, endOffset, uriString));
            int remaining = count;

            Iterator<PrefetchTask> iterator = prefetchTasks.iterator();
            while(iterator.hasNext()) {
                PrefetchTask prefetchTask = iterator.next();
                long offset = prefetchTask.getOffset();
                if(offset >= startOffset && offset < endOffset) {
                    prefetchTask.setScheduledTime(DateTimeUtils.getTimestamp());
                    this.priorityTaskQueue.add(prefetchTask);
                    this.numQueuedTasks++;

                    iterator.remove();
                    this.numPendingPrefetchTasks--;

                    remaining--;

                    if(remaining <= 0) {
                        break;
                    }
                }
            }

            if(prefetchTasks.isEmpty()) {
                this.pendingPrefetchTasks.remove(uriString);
            } else {
                // to prevent the item from expiring
                this.pendingPrefetchTasks.put(uriString, prefetchTasks);
            }
        }
        
        printCurrentStates();
    }
    
    private void printCurrentStates() {
        LOG.info(String.format("Transfer(%d), Queued(%d), Pending Prefetch(%d)", numInTransferTasks, numQueuedTasks, numPendingPrefetchTasks));
    }
}
