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

/**
 *
 * @author iychoi
 */
public abstract class HTTPTransportRestfulConstants {
    public static final String BASE_PATH = "/";
    public static final String API_PATH = "/api";
    public static final String GET_METADATA_PATH = "/metadata";
    public static final String LIST_METADATA_PATH = "/lmetadata";
    public static final String GET_DIRECTORY_PATH = "/directory";
    public static final String GET_RECIPE_PATH = "/recipe";
    public static final String GET_DATA_CHUNK_PATH = "/data";
    
    public static final String API_CHECK_LIVE_PATH = "live";
    public static final String API_GET_CLUSTER_PATH = "cluster";
}
