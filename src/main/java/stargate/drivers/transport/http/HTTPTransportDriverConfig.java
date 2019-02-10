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

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.transport.AbstractTransportDriverConfig;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class HTTPTransportDriverConfig extends AbstractTransportDriverConfig {
    
    public static final String DEFAULT_SERVICE_HOSTNAME_PATTERN = ".*";
    public static final int DEFAULT_SERVICE_PORT = 31010;
    
    private String serviceHostNamePattern = DEFAULT_SERVICE_HOSTNAME_PATTERN;
    private int servicePort = DEFAULT_SERVICE_PORT;
    
    public static HTTPTransportDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (HTTPTransportDriverConfig) JsonSerializer.fromJsonFile(file, HTTPTransportDriverConfig.class);
    }
    
    public static HTTPTransportDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (HTTPTransportDriverConfig) JsonSerializer.fromJson(json, HTTPTransportDriverConfig.class);
    }
    
    public HTTPTransportDriverConfig() {
    }
    
    @JsonProperty("service_host_name_pattern")
    public void setServiceHostNamePattern(String serviceHostNamePattern) {
        if(serviceHostNamePattern == null || serviceHostNamePattern.isEmpty()) {
            throw new IllegalArgumentException("serviceHostNamePattern is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.serviceHostNamePattern = serviceHostNamePattern;
    }
    
    @JsonProperty("service_host_name_pattern")
    public String getServiceHostNamePattern() {
        return this.serviceHostNamePattern;
    }
    
    @JsonProperty("service_port")
    public void setServicePort(int port) {
        if(port <= 0) {
            throw new IllegalArgumentException("port is invalid");
        }
        
        super.checkMutableAndRaiseException();
        
        this.servicePort = port;
    }
    
    @JsonProperty("service_port")
    public int getServicePort() {
        return this.servicePort;
    }
}
