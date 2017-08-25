Linden provides a java client. You can use this client for index, search and delete operation to linden cluster.  Linden client will send the request to a random linden instance of the cluster, the linden instance will broadcast the request if necessary.

### Constructor

	public LindenClient(String clusterUrl)
	public LindenClient(String clusterUrl, int timeout)

clusterUrl is linden zookeeper cluster path, this is specified in linden.properties, e.g. 127.0.0.1:2181/linden_demo
You can specify timeout in linden client constructor, else timeout is 3000ms by default.

### Index

	public Response index(String content) throws Exception

Content is an index request in JSON string format. It contains a "type" field to indicate the operation type, whose value may be index, delete, replace, delete_index or swap_index. Please see [Linden Work Mode Document](LindenWorkMode.md) for detail.

### Search

	public LindenResult search(String bql) throws Exception

Search request is BQL, see more in [BQL Document](BQL.md).
Search result is LindenResult type, see more in ***com.xiaomi.linden.thrift.common.LindenResult***.

### Delete

	public Response delete(String bql) throws Exception
	
This interface is for BQL delete request, e.g.

	DELETE FROM LINDEN WHERE title = 'beijing'