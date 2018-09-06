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
package stargate.drivers.datasource.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.AbstractDataSourceDriverConfig;
import stargate.commons.datasource.SourceFileMetadata;
import stargate.commons.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class HDFSDataSourceDriver extends AbstractDataSourceDriver {

    private static final Log LOG = LogFactory.getLog(HDFSDataSourceDriver.class);
    
    private HDFSDataSourceDriverConfig config;
    private String scheme;
    private Configuration hadoopConfig;
    private Path rootPath;
    private FileSystem filesystem;
    
    private Map<String, String> cachedNodeNameConvTable = new HashMap<String, String>();
    
    public HDFSDataSourceDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HDFSDataSourceDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of HDFSDataSourceDriverConfig");
        }
        
        this.config = (HDFSDataSourceDriverConfig) config;
        this.scheme = this.config.getScheme();
    }
    
    public HDFSDataSourceDriver(AbstractDataSourceDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HDFSDataSourceDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of HDFSDataSourceDriverConfig");
        }
        
        this.config = (HDFSDataSourceDriverConfig) config;
        this.scheme = this.config.getScheme();
    }
    
    public HDFSDataSourceDriver(HDFSDataSourceDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        this.scheme = this.config.getScheme();
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.info("Initializing HDFS Data Source Driver");
        
        this.hadoopConfig = new Configuration();
        Path rootPath = this.config.getRootPath();
        String hdfsRoot = this.hadoopConfig.get("fs.default.name");
        LOG.info("hdfs root : " + hdfsRoot);
        
        // combine hdfs root + user defined root
        this.rootPath = new Path(hdfsRoot, rootPath);
        this.filesystem = this.rootPath.getFileSystem(this.hadoopConfig);
    }

    @Override
    public synchronized void uninit() throws IOException {
        this.filesystem.close();
        this.cachedNodeNameConvTable.clear();
        
        super.uninit();
    }
    
    @Override
    public String getScheme() {
        return this.scheme;
    }
    
    // convert driver uri to sourcefs uri
    private Path getSourcePath(URI uri) {
        if(!uri.getScheme().equalsIgnoreCase(this.scheme)) {
            throw new IllegalArgumentException(String.format("Scheme does not match - %s vs. %s", uri.getScheme(), this.scheme));
        }
        
        String path = uri.getPath();
        Path hdfsPath = new Path(this.rootPath, path);
        return hdfsPath;
    }
    
    // convert sourcefs uri to driver uri
    private URI getDriverURI(Path path) throws URISyntaxException, IOException {
        String root = this.rootPath.toUri().getPath();
        String source = path.toUri().getPath();
        
        if(source.startsWith(root)) {
            String relativePath = source.substring(root.length());
            String driverPath = PathUtils.concatPath("/", relativePath);
            return new URI(String.format("%s://%s", this.scheme, driverPath));
        } else {
            throw new IOException(String.format("cannot convert sourcefs URI to driver URI - %s (root: %s)", source, root));
        }
    }
    
    @Override
    public SourceFileMetadata getMetadata(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") does not exist");
        }
        
        return new SourceFileMetadata(uri, true, status.isDirectory(), status.getLen(), status.getModificationTime());
    }
    
    @Override
    public boolean exist(URI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        return this.filesystem.exists(hdfsPath);
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("directory (" + hdfsPath.toString() + ") does not exist");
        }
        return status.isDirectory();
    }

    @Override
    public boolean isFile(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") does not exist");
        }
        return status.isFile();
    }

    @Override
    public Collection<URI> listDirectory(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("directory (" + hdfsPath.toString() + ") does not exist");
        }
        
        if(!status.isDirectory()) {
            throw new FileNotFoundException(hdfsPath.toString() + " is not a directory");
        }
        
        FileStatus[] listStatus = this.filesystem.listStatus(hdfsPath);
        List<URI> entries = new ArrayList<URI>();
        if(listStatus != null && listStatus.length > 0) {
            for(FileStatus stat : listStatus) {
                try {
                    URI entryDriverUri = getDriverURI(stat.getPath());
                    entries.add(entryDriverUri);
                } catch (URISyntaxException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
        
        return Collections.unmodifiableCollection(entries);
    }

    @Override
    public Collection<SourceFileMetadata> listDirectoryWithMetadata(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("directory (" + hdfsPath.toString() + ") does not exist");
        }
        
        if(!status.isDirectory()) {
            throw new FileNotFoundException(hdfsPath.toString() + " is not a directory");
        }
        
        FileStatus[] listStatus = this.filesystem.listStatus(hdfsPath);
        List<SourceFileMetadata> entries = new ArrayList<SourceFileMetadata>();
        if(listStatus != null && listStatus.length > 0) {
            for(FileStatus stat : listStatus) {
                try {
                    URI entryDriverUri = getDriverURI(stat.getPath());
                    SourceFileMetadata metadata = new SourceFileMetadata(entryDriverUri, true, stat.isDirectory(), stat.getLen(), stat.getModificationTime());
                    entries.add(metadata);
                    
                } catch (URISyntaxException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
        
        return entries;
    }

    @Override
    public InputStream openFile(URI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") does not exist");
        }
        
        if(!status.isFile()) {
            throw new FileNotFoundException(hdfsPath.toString() + " is not a file");
        }
        
        return this.filesystem.open(hdfsPath);
    }

    @Override
    public InputStream openFile(URI uri, long offset, int size) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(size <= 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") does not exist");
        }
        
        if(!status.isFile()) {
            throw new FileNotFoundException(hdfsPath.toString() + " is not a file");
        }
        
        return new HDFSChunkReader(this.filesystem, hdfsPath, offset, size);
    }

    @Override
    public Collection<String> listBlockLocations(Cluster cluster, URI uri, long offset, int size) throws IOException, FileNotFoundException {
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
        
        Path hdfsPath = getSourcePath(uri);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(status == null) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") does not exist");
        }
        
        if(!status.isFile()) {
            throw new FileNotFoundException(hdfsPath.toString() + " is not a file");
        }
        
        List<String> locations = new ArrayList<String>();
        BlockLocation[] fileBlockLocations = this.filesystem.getFileBlockLocations(status, offset, size);
        if(fileBlockLocations != null) {
            Set<String> location_set = new HashSet<String>();
            for(BlockLocation location : fileBlockLocations) {
                for(String host : location.getHosts()) {
                    location_set.add(host);
                }

                for(String host : location.getCachedHosts()) {
                    location_set.add(host);
                }
            }
            
            // convert HDFS node name to Stargate node name
            for(String location : location_set) {
                String nodeName = convNodeName(cluster, location);
                if(nodeName != null) {
                    locations.add(nodeName);
                } else {
                    LOG.info(String.format("Cannot convert block location to a node name - %s", location));
                    locations.clear();
                    locations.add("*");
                    break;
                }
            }
        }
        
        if(locations.size() == 0) {
            throw new IOException("The block cannot be accessed");
        }
        
        return locations;
    }
    
    private String convNodeName(Cluster cluster, String location) {
        String nodeName = this.cachedNodeNameConvTable.get(location);
        if(nodeName != null) {
            return nodeName;
        }
        
        Collection<Node> clusterNodes = cluster.getNodes();
        for(Node node : clusterNodes) {
            if(node.getName().equals(location)) {
                nodeName = node.getName();
                this.cachedNodeNameConvTable.put(location, nodeName);
                return nodeName;
            }
        }

        for(Node node : clusterNodes) {
            Collection<String> hostnames = node.getHostnames();
            for(String hostname : hostnames) {
                if(hostname.equals(location)) {
                    nodeName = node.getName();
                    this.cachedNodeNameConvTable.put(location, nodeName);
                    return nodeName;
                }
                
                // compare IP part
                String location_hostname = extractHostname(location);
                if(hostname.equals(location_hostname)) {
                    nodeName = node.getName();
                    this.cachedNodeNameConvTable.put(location, nodeName);
                    return nodeName;
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
