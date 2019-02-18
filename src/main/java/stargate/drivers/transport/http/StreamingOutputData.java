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
package stargate.drivers.transport.http;

import stargate.drivers.userinterface.http.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

/**
 *
 * @author iychoi
 */
public class StreamingOutputData implements StreamingOutput {

    private static final int BUFFER_SIZE = 64*1024; // 64k
    private InputStream is;
    private byte[] buffer;

    StreamingOutputData(InputStream is) {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }

        this.is = is;
        this.buffer = new byte[BUFFER_SIZE];
    }

    @Override
    public void write(OutputStream out) throws IOException, WebApplicationException {
        if(out == null) {
            throw new IllegalArgumentException("out is null");
        }

        try {
            int read = 0;
            while ((read = this.is.read(this.buffer)) > 0) {
                out.write(this.buffer, 0, read);
            }
            this.is.close();
        } catch (Exception ex) {
            throw new WebApplicationException(ex);
        }
    }
}
