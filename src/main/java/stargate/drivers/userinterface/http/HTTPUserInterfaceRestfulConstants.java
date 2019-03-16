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
package stargate.drivers.userinterface.http;

/**
 *
 * @author iychoi
 */
public abstract class HTTPUserInterfaceRestfulConstants {
    public static final String BASE_PATH = "/";
    public static final String API_PATH = "/api";
    
    public static final String API_GET_METADATA_PATH = "metadata";
    public static final String API_LIST_METADATA_PATH = "lmetadata";
    public static final String API_GET_RECIPE_PATH = "recipe";
    public static final String API_GET_DATA_CHUNK_PATH = "data";
    public static final String API_CHECK_LIVE_PATH = "live";
    public static final String API_GET_SERVICE_CONFIG_PATH = "svcconfig";
    public static final String API_GET_FS_SERVICE_INFO_PATH = "fssvcinfo";
    public static final String API_GET_CLUSTER_PATH = "cluster";
    public static final String API_GET_LOCAL_CLUSTER_PATH = "lcluster";
    public static final String API_ACTIVATE_CLUSTER_PATH = "activate";
    public static final String API_CHECK_ACTIVE_CLUSTER_PATH = "active";
    public static final String API_GET_REMOTE_CLUSTER_PATH = "rcluster";
    public static final String API_LIST_REMOTE_CLUSTERS_PATH = "lrcluster";
    public static final String API_GET_REMOTE_CLUSTERS_PATH = "rclusters";
    public static final String API_ADD_REMOTE_CLUSTER_PATH = "rcluster";
    public static final String API_REMOVE_REMOTE_CLUSTER_PATH = "rcluster";
    public static final String API_SYNC_REMOTE_CLUSTERS_PATH = "srcluster";
    public static final String API_GET_LOCAL_NODE_PATH = "lnode";
    public static final String API_GET_LEADER_NODE_PATH = "leadernode";
    public static final String API_GET_DATA_EXPORT_ENTRY_PATH = "export";
    public static final String API_LIST_DATA_EXPORT_ENTRIES_PATH = "lexport";
    public static final String API_GET_DATA_EXPORT_ENTRIES_PATH = "exports";
    public static final String API_ADD_DATA_EXPORT_ENTRY_PATH = "export";
    public static final String API_REMOVE_DATA_EXPORT_ENTRY_PATH = "export";
    public static final String API_LIST_RECIPES_PATH = "lrecipe";
    public static final String API_REMOVE_RECIPE_PATH = "recipe";
    public static final String API_SYNC_RECIPES_PATH = "srecipe";
    public static final String API_SCHEDULE_PREFETCH_PATH = "prefetch";
    public static final String API_GET_REMOTE_RECIPE_WITH_TRANSFER_SCHEDULE_PATH = "rrecipewts";
    public static final String API_LIST_DATA_SOURCES_PATH = "lsources";
    
    public static final String API_GET_STATISTICS_PATH = "stat";
    public static final String API_CLEAR_STATISTICS_PATH = "stat";
    public static final String API_CLEAR_ALL_STATISTICS_PATH = "allstat";
}
