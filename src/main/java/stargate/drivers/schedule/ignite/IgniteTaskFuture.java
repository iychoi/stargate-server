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
package stargate.drivers.schedule.ignite;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.compute.ComputeTaskFuture;

/**
 *
 * @author iychoi
 */
public class IgniteTaskFuture<T> implements Future {
    
    private ComputeTaskFuture<T> igniteFuture;

    IgniteTaskFuture(ComputeTaskFuture<T> future) {
        this.igniteFuture = future;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.igniteFuture.cancel();
    }

    @Override
    public boolean isCancelled() {
        return this.igniteFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return this.igniteFuture.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return this.igniteFuture.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.igniteFuture.get(timeout, unit);
    }
}
