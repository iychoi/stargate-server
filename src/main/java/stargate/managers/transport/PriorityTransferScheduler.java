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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class PriorityTransferScheduler {
    
    private static final Log LOG = LogFactory.getLog(PriorityTransferScheduler.class);

    private ExecutorService priorityTransferPoolExecutor;
    private Thread priorityTransferSchedulerThread;
    private boolean schedulerRun = true;
    private PriorityBlockingQueue<AbstractTransferTask> priorityQueue;
    private Map<String, AbstractTransferTask> inTransfer = new ConcurrentHashMap<String, AbstractTransferTask>();
    private Semaphore lock;
    
    public PriorityTransferScheduler(int executorPoolSize, int initialQueueCapacity) {
        if(executorPoolSize <= 0) {
            throw new IllegalArgumentException("executorPoolSize is negative");
        }
        
        if(initialQueueCapacity <= 0) {
            throw new IllegalArgumentException("initialQueueCapacity is negative");
        }
        
        this.priorityTransferPoolExecutor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(executorPoolSize));
        this.priorityQueue = new PriorityBlockingQueue<AbstractTransferTask>(initialQueueCapacity, new TransferTaskComparator());
        this.lock = new Semaphore(executorPoolSize);
    }
    
    public void start() {
        this.schedulerRun = true;
        this.priorityTransferSchedulerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(schedulerRun) {
                        LOG.debug("Fetching a transfer from a queue");
                        AbstractTransferTask task = priorityQueue.take();
                        
                        if(inTransfer.containsKey(task.getHash())) {
                            LOG.debug(String.format("Ignoring a transfer for %s - already in transfer", task.getHash()));
                        } else {
                            Runnable r = new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        LOG.debug(String.format("Running a transfer task for %s (%s)", task.getHash(), task.getPriority().getStrVal()));
                                        task.run();
                                    } finally {
                                        inTransfer.remove(task.getHash());
                                        lock.release();
                                    }
                                }
                            };

                            LOG.debug(String.format("Scheduling a transfer for %s (%s)", task.getHash(), task.getPriority().getStrVal()));
                            // register to inTransfer set
                            lock.acquire();
                            inTransfer.put(task.getHash(), task);
                            // go
                            priorityTransferPoolExecutor.execute(r);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Unknown Exception", ex);
                }
            }
        });
        
        this.priorityTransferSchedulerThread.start();
    }
    
    public void stop() {
        this.schedulerRun = false;
        if(this.priorityTransferSchedulerThread != null) {
            if(this.priorityTransferSchedulerThread.isAlive()) {
                this.priorityTransferSchedulerThread.interrupt();
            }
            this.priorityTransferSchedulerThread = null;
        }
        
        this.priorityTransferPoolExecutor.shutdownNow();
        this.priorityQueue.clear();
        this.inTransfer.clear();
    }
    
    public void schedule(AbstractTransferTask task) {
        LOG.debug("Putting a transfer in a queue");
        this.priorityQueue.add(task);
    }
}
