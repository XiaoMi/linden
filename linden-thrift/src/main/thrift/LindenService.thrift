namespace java com.xiaomi.linden.thrift.service

include "LindenCommon.thrift"
include "LindenRequest.thrift"
include "LindenResponse.thrift"

service LindenService {
    LindenCommon.LindenResult search(1: LindenRequest.LindenSearchRequest request);
    LindenResponse.Response delete(1: LindenRequest.LindenDeleteRequest request);
    LindenResponse.Response index(1: string content);
    LindenCommon.LindenResult handleBqlRequest(1: string bql);
    LindenResponse.Response executeCommand(1: string command);

    LindenResponse.Response handleClusterIndexRequest(1: string content);
    LindenCommon.LindenResult handleClusterSearchRequest(1: string bql);
    LindenResponse.Response handleClusterDeleteRequest(1: string bql);
    LindenCommon.LindenResult handleClusterBqlRequest(1: string bql);
    LindenResponse.Response handleClusterCommand(1: string command);

    #deprecated
    LindenCommon.LindenResult searchByBqlCluster(1: string bql);

    LindenCommon.LindenServiceInfo getServiceInfo();
}