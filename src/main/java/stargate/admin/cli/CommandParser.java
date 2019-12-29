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
package stargate.admin.cli;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class CommandParser {

    private URI serviceURI;
    private boolean debug = false;
    private List<String> positionalArgs = new ArrayList<String>();
    
    public CommandParser() {
        this.serviceURI = null;
    }
    
    public void parse(String[] args) {
        if(args != null && args.length != 0) {
            for(int i=0;i<args.length;i++) {
                
                if("-s".equals(args[i])) {
                    if(args.length > i + 1) {
                        try {
                            this.serviceURI = new URI(args[i+1]);
                        } catch (URISyntaxException ex) {
                            throw new IllegalArgumentException(ex);
                        }
                        i++;
                    }
                } else if("-d".equals(args[i])) {
                    this.debug = true;
                } else {
                    //positionalArgs
                    this.positionalArgs.add(args[i]);
                }
            }
        }
    }
    
    public URI getServiceURI() {
        return this.serviceURI;
    }
    
    public boolean isDebug() {
        return this.debug;
    }
    
    public String[] getPositionalArgs() {
        return this.positionalArgs.toArray(new String[0]);
    }
}
