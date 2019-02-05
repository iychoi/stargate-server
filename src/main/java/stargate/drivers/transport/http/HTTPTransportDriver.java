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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Node;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.transport.AbstractTransportClient;
import stargate.commons.transport.AbstractTransportDriver;
import stargate.commons.transport.AbstractTransportDriverConfig;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.IPUtils;
import stargate.managers.transport.TransportManager;

/**
 *
 * @author iychoi
 */
public class HTTPTransportDriver extends AbstractTransportDriver {

    private static final Log LOG = LogFactory.getLog(HTTPTransportDriver.class);
    
    private static final int DEFAULT_LIVECHECK_SECONDS = 60;
    
    private HTTPTransportDriverConfig config;
    private HTTPTransportServer server;
    private boolean serverStarted = false;
    //<String, HTTPTransportClient>
    private LRUMap clients = new LRUMap();
    
    public HTTPTransportDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HTTPTransportDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of HTTPTransportDriverConfig");
        }
        
        this.config = (HTTPTransportDriverConfig) config;
    }
    
    public HTTPTransportDriver(AbstractTransportDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HTTPTransportDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of HTTPTransportDriverConfig");
        }
        
        this.config = (HTTPTransportDriverConfig) config;
    }
    
    public HTTPTransportDriver(HTTPTransportDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
    }

    @Override
    public synchronized void uninit() throws IOException {
        // stop server
        if(this.serverStarted) {
            if(this.server != null) {
                this.server.stop();
                this.server = null;
            }
            this.serverStarted = false;
        }
         
        // clear client pool
        Collection<HTTPTransportClient> values = (Collection<HTTPTransportClient>) this.clients.values();
        for(HTTPTransportClient client : values) {
            client.disconnect();
        }
        this.clients.clear();
        
        super.uninit();
    }
    
    @Override
    public synchronized void startServer() throws IOException {
        if(this.serverStarted) {
            throw new IllegalStateException("Server is already running");
        }
        
        // start web server
        this.server = new HTTPTransportServer(this, this.config.getServicePort());
        this.server.start();
        this.serverStarted = true;
    }
    
    @Override
    public synchronized void stopServer() throws IOException {
        if(!this.serverStarted) {
            throw new IllegalStateException("Server is not running");
        }
        
        // stop server
        if(this.server != null) {
            this.server.stop();
            this.server = null;
        }
        this.serverStarted = false;
    }
    
    @Override
    public synchronized boolean isServerStarted() {
        return this.serverStarted;
    }

    @Override
    public URI getServiceURI() throws IOException {
        try {
            Collection<String> hostAddress = IPUtils.getHostNames();
            List<String> acceptedHostAddr = new ArrayList<String>();
            
            for(String addr : hostAddress) {
                Pattern pattern = Pattern.compile(this.config.getServiceHostNamePattern());
                Matcher matcher = pattern.matcher(addr);
                if(matcher.matches()) {
                    acceptedHostAddr.add(addr);
                }
            }
            
            if(acceptedHostAddr.isEmpty()) {
                return new URI("http://localhost:" + this.config.getServicePort());
            } else {
                for(String addr : acceptedHostAddr) {
                    // preferred - domainname
                    if(IPUtils.isDomainName(addr)) {
                        return new URI("http://" + addr + ":" + this.config.getServicePort());
                    }
                }
                
                for(String addr : acceptedHostAddr) {
                    // preferred - public address
                    if(IPUtils.isPublicIPAddress(addr)) {
                        return new URI("http://" + addr + ":" + this.config.getServicePort());
                    }
                }
                
                return new URI("http://" + acceptedHostAddr.get(0) + ":" + this.config.getServicePort());
            }
            
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    private TransportManager getTransportManager() {
        if(this.manager == null) {
            throw new IllegalStateException("manager is not initialized");
        }
        
        return (TransportManager) this.manager;
    }
    
    @Override
    public AbstractTransportClient getClient(Node node) throws IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        TransportServiceInfo transportServiceInfo = node.getTransportServiceInfo();
        
        HTTPTransportClient existingClient = (HTTPTransportClient) this.clients.get(node.getName());
        if(existingClient != null) {
            boolean isLive;
            if(DateTimeUtils.timeElapsedSec(existingClient.getLastActiveTime(), DateTimeUtils.getTimestamp(), DEFAULT_LIVECHECK_SECONDS)) {
                isLive = existingClient.isLive();
            } else {
                isLive = true;
            }
            
            if(isLive) {
                // use existing
                LOG.debug(String.format("Reuse an existing transport client for %s", transportServiceInfo.getServiceURI().toASCIIString()));
                return existingClient;
            } else {
                if(existingClient.isConnected()) {
                    existingClient.disconnect();
                }
                this.clients.remove(node.getName());
            }
        }
        
        TransportManager transportManager = getTransportManager();
        try {
            LOG.debug(String.format("Get a transport client for %s", transportServiceInfo.getServiceURI().toASCIIString()));
            if(transportServiceInfo.getDriverClass().equals(HTTPTransportDriver.class)) {
                HTTPTransportClient client = new HTTPTransportClient(transportServiceInfo, null, null);
                client.connect();
                if(client.isLive()) {
                    this.clients.put(node.getName(), client);
                    return client;
                } else {
                    // report
                    transportManager.reportNodeUnreachable(node);
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        
        throw new IOException(String.format("unable to connect to a node (%s)", node.getName()));
    }
}
