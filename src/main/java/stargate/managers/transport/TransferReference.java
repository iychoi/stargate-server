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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author iychoi
 */
public class TransferReference {

    private int referenceCount = 0;
    private CountDownLatch latch = new CountDownLatch(1);
    
    public TransferReference() {
    }
    
    public synchronized void finishTransfer() {
        if(this.latch.getCount() > 0) {
            this.latch.countDown();
        }
    }
    
    public synchronized void increaseReference() {
        this.referenceCount++;
    }
    
    public synchronized void decreaseReference() {
        if(this.referenceCount > 0) {
            this.referenceCount--;
        } else {
            throw new IllegalStateException("Reference count is already 0");
        }
    }
    
    public synchronized int getReferenceCount() {
        return this.referenceCount;
    }

    public void await(int timeout, TimeUnit timeUnit) throws InterruptedException {
        this.latch.await(timeout, timeUnit);
    }
}
