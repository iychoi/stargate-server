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
package stargate.service;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.xml.DOMConfigurator;
import stargate.commons.utils.ResourceUtils;

/**
 *
 * @author iychoi
 */
public class ServiceMain {
    private static final Log LOG = LogFactory.getLog(ServiceMain.class);
    
    public static String LOG4J_PROPERTY_PATH = "config/java.util.logging.properties";
    
    public static void main(String[] args) {
        try {
            setLogger();
            disableIPv6();
            
            StargateServiceConfig serviceConfig = null;
            if(args.length != 0) {
                File serviceConfigFile = new File(args[0]).getAbsoluteFile();
                if (!serviceConfigFile.exists()) {
                    throw new IOException(String.format("Config file %s does not exist", args[0]));
                }
                
                serviceConfig = StargateServiceConfig.createInstance(serviceConfigFile);
            }
            
            if(serviceConfig == null) {
                serviceConfig = new StargateServiceConfig();
            }
            
            StargateService instance = StargateService.getInstance(serviceConfig);
            instance.start();
            System.out.println("press ctrl + c for stopping the service...");
            
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        Thread.sleep(200);
                        instance.stop();
                    } catch (IOException e) {
                        LOG.error(e);
                    } catch (InterruptedException e) {
                        LOG.error(e);
                    }
                }
            });
            
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    // service loop
                    Thread.sleep(1000);
                } catch(InterruptedException ex) {
                    //Thread.currentThread().interrupt();
                    break;
                }
            }
            
            // when pressing ctrl+c, execution line does not reach to here.
            // so use shutdown hook to release resources instead.
        } catch (Exception ex) {
            LOG.error("Unknown Exception", ex);
            ex.printStackTrace();
        }
    }
    
    private static void setLogger() {
        File log_property_file = new File(ResourceUtils.getStargateRoot(), LOG4J_PROPERTY_PATH);
        LogManager.resetConfiguration();
        DOMConfigurator.configure(log_property_file.getAbsolutePath());
    }

    private static void disableIPv6() {
        System.setProperty("java.net.preferIPv4Stack", "true");
    }
}
