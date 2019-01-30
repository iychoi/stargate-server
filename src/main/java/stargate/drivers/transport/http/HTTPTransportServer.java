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

import stargate.utils.logging.JettyLog;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 *
 * @author iychoi
 */
public class HTTPTransportServer {
    
    private static final Log LOG = LogFactory.getLog(HTTPTransportServer.class);
    
    private HTTPTransportDriver driver;
    private int servicePort;
    private Server jettyWebServer;
    private boolean started = false;
    
    public HTTPTransportServer(HTTPTransportDriver driver, int port) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is invalid");
        }
        
        if(port <= 0) {
            throw new IllegalArgumentException("port is invalid");
        }
        
        this.driver = driver;
        this.servicePort = port;
        this.started = false;
    }
    
    public HTTPTransportDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        if(this.started) {
            throw new IllegalStateException("Server is already started");
        }
        
        try {
            JettyLog logger = new JettyLog();
            org.eclipse.jetty.util.log.Log.__logClass = JettyLog.class.getCanonicalName();
            org.eclipse.jetty.util.log.Log.setLog(logger);

            // configure servlets
            ServletHolder sh = new ServletHolder(ServletContainer.class);  

            sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig"); 
            sh.setInitParameter("com.sun.jersey.config.property.packages", HTTPTransportServlet.class.getPackage().getName());
            sh.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true"); 

            // start web service
            this.jettyWebServer = new Server(this.servicePort);

            // setting servlets
            ServletContextHandler context = new ServletContextHandler(this.jettyWebServer, "/", ServletContextHandler.SESSIONS);
            context.addServlet(sh, "/*");
            
            HTTPTransportServlet.setDriver(this.driver);
        
            LOG.debug("Starting HTTP Transport Server");
            this.jettyWebServer.start();
            // block until break
            //LOG.debug("Joining HTTP Transport Server");
            //this.jettyWebServer.join();
            this.started = true;
            LOG.debug("HTTP Transport Server is started");
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized void stop() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Server is already stopped");
        }
        
        // stop web server
        try {
            if(this.jettyWebServer != null) {
                LOG.debug("Stopping HTTP Transport Server");
                this.jettyWebServer.stop();
            }
            this.started = false;
            LOG.debug("HTTP Transport Server is stopped");
        } catch(Exception ex) {
            LOG.error("Exception occurred while stopping web server", ex);
        }
    }
    
    public synchronized boolean isStarted() {
        return this.started;
    }
}
