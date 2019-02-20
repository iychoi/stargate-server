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
package stargate.drivers.datasource.localfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.AbstractDataSourceDriverConfig;
import stargate.commons.datasource.SourceFileMetadata;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.utils.IPUtils;
import stargate.commons.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class LocalFSDataSourceDriver extends AbstractDataSourceDriver {

    private static final Log LOG = LogFactory.getLog(LocalFSDataSourceDriver.class);
    
    private LocalFSDataSourceDriverConfig config;
    private String scheme;
    private File rootPath;
    
    private String cachedLocalNodeName;
    private final Object cachedLocalNodeNameSyncObj = new Object();
    
    public LocalFSDataSourceDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof LocalFSDataSourceDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of HDFSDataSourceDriverConfig");
        }
        
        this.config = (LocalFSDataSourceDriverConfig) config;
        this.scheme = this.config.getScheme();
    }
    
    public LocalFSDataSourceDriver(AbstractDataSourceDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof LocalFSDataSourceDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of HDFSDataSourceDriverConfig");
        }
        
        this.config = (LocalFSDataSourceDriverConfig) config;
        this.scheme = this.config.getScheme();
    }
    
    public LocalFSDataSourceDriver(LocalFSDataSourceDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        this.scheme = this.config.getScheme();
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.debug("Initializing LocalFS Data Source Driver");
        
        this.rootPath = this.config.getRootPath();
        LOG.debug("localfs root - " + this.rootPath);
    }

    @Override
    public synchronized void uninit() throws IOException {
        this.cachedLocalNodeName = null;
        
        super.uninit();
    }
    
    @Override
    public String getScheme() {
        return this.scheme;
    }
    
    // convert driver uri to sourcefs uri
    private File getSourcePath(URI uri) {
        if(!uri.getScheme().equalsIgnoreCase(this.scheme)) {
            throw new IllegalArgumentException(String.format("Scheme does not match - %s vs. %s", uri.getScheme(), this.scheme));
        }
        
        String path = uri.getPath();
        File localfsPath = new File(this.rootPath, path);
        return localfsPath;
    }
    
    // convert sourcefs uri to driver uri
    private URI getDriverURI(File path) throws URISyntaxException, IOException {
        String root = this.rootPath.getAbsolutePath();
        String source = path.getAbsolutePath();
        
        if(source.startsWith(root)) {
            String relativePath = source.substring(root.length());
            String driverPath = PathUtils.makeAbsolutePath(relativePath);
            return new URI(String.format("%s://%s", this.scheme, driverPath));
        } else {
            throw new IOException(String.format("cannot convert sourcefs URI to driver URI - %s (root: %s)", source, root));
        }
    }
    
    @Override
    public SourceFileMetadata getMetadata(URI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("file (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        return new SourceFileMetadata(uri, true, localPath.isDirectory(), localPath.length(), localPath.lastModified());
    }
    
    @Override
    public boolean exist(URI uri) throws IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        return localPath.exists();
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("file (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        return localPath.isDirectory();
    }

    @Override
    public boolean isFile(URI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("file (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        return localPath.isFile();
    }

    @Override
    public Collection<URI> listDirectory(URI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("directory (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        if(!localPath.isDirectory()) {
            throw new FileNotFoundException(localPath.getAbsolutePath() + " is not a directory");
        }
        
        File[] listFiles = localPath.listFiles();
        List<URI> entries = new ArrayList<URI>();
        if(listFiles != null && listFiles.length > 0) {
            for(File file : listFiles) {
                try {
                    URI entryDriverUri = getDriverURI(file);
                    entries.add(entryDriverUri);
                } catch (URISyntaxException ex) {
                    LOG.error("URISyntaxException", ex);
                    throw new IOException(ex);
                }
            }
        }
        
        return Collections.unmodifiableCollection(entries);
    }

    @Override
    public Collection<SourceFileMetadata> listDirectoryWithMetadata(URI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("directory (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        if(!localPath.isDirectory()) {
            throw new FileNotFoundException(localPath.getAbsolutePath() + " is not a directory");
        }
        
        File[] listFiles = localPath.listFiles();
        List<SourceFileMetadata> entries = new ArrayList<SourceFileMetadata>();
        if(listFiles != null && listFiles.length > 0) {
            for(File file : listFiles) {
                try {
                    URI entryDriverUri = getDriverURI(file);
                    SourceFileMetadata metadata = new SourceFileMetadata(entryDriverUri, true, file.isDirectory(), file.length(), file.lastModified());
                    entries.add(metadata);
                } catch (URISyntaxException ex) {
                    LOG.error("URISyntaxException", ex);
                    throw new IOException(ex);
                }
            }
        }
        
        return entries;
    }

    @Override
    public InputStream openFile(URI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("file (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        if(!localPath.isFile()) {
            throw new FileNotFoundException(localPath.getAbsolutePath() + " is not a file");
        }
        
        return new FileInputStream(localPath);
    }

    @Override
    public InputStream openFile(URI uri, long offset, int size) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(size <= 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("file (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        if(!localPath.isFile()) {
            throw new FileNotFoundException(localPath.getAbsolutePath() + " is not a file");
        }
        
        return new LocalFSChunkReader(localPath, offset, size);
    }

    @Override
    public Collection<String> listBlockLocations(Cluster cluster, URI uri, long offset, int size) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(size <= 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        File localPath = getSourcePath(uri);
        if(!localPath.exists()) {
            throw new FileNotFoundException("file (" + localPath.getAbsolutePath() + ") does not exist");
        }
        
        if(!localPath.isFile()) {
            throw new FileNotFoundException(localPath.getAbsolutePath() + " is not a file");
        }
        
        List<String> locations = new ArrayList<String>();
        String location = getLocalNode(cluster);
        locations.add(location);
        
        return locations;
    }
    
    private String getLocalNode(Cluster cluster) throws IOException {
        synchronized(this.cachedLocalNodeNameSyncObj) {
            if(this.cachedLocalNodeName == null || this.cachedLocalNodeName.isEmpty()) {
                String nodeName = null;
                Collection<String> hostnames = IPUtils.getHostNames();
                for(String hostname : hostnames) {
                    // use one of hostnames that can be converted to node name
                    String newNodeName = convNodeName(cluster, hostname);
                    if(newNodeName != null) {
                        nodeName = newNodeName;
                        break;
                    }
                }

                if(nodeName == null) {
                    throw new IOException("cannot get local node");
                }
                this.cachedLocalNodeName = nodeName;
            }
            return this.cachedLocalNodeName;
        }
    }
    
    private String convNodeName(Cluster cluster, String hostname) {
        Collection<Node> clusterNodes = cluster.getNodes();
        for(Node node : clusterNodes) {
            if(node.getName().equals(hostname)) {
                return node.getName();
            }
        }

        for(Node node : clusterNodes) {
            Collection<String> hostnames = node.getHostnames();
            for(String hname : hostnames) {
                if(hname.equals(hostname)) {
                    return node.getName();
                }
                
                // compare IP part
                String location_hostname = extractHostname(hname);
                if(location_hostname.equals(hostname)) {
                    return node.getName();
                }
            }
        }
        
        return null;
    }
    
    private String extractHostname(String location) {
        String hostname = location;
        int idxScheme = hostname.indexOf("://");
        if(idxScheme >= 0) {
            hostname = hostname.substring(idxScheme + 3);
        }
        
        int idxPort = hostname.indexOf(":");
        if(idxPort >= 0) {
            hostname = hostname.substring(0, idxPort);
        }
        
        int idxSlash = hostname.indexOf("/");
        if(idxSlash >= 0) {
            hostname = hostname.substring(0, idxSlash);
        }
        
        return hostname;
    }
}
