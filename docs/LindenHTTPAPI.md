To enable linden Http API, you need set admin.port in linden properties, see [Linden Properties Document](LindenProperties.md).

### Index

Index is a POST API.

URL:  http://$IP:$Port/index

where $IP is one linden instance IP and $Port is admin.port configured.

Only one parameter is *content*, Content is an index request in JSON string format. It contains a “type" field to indicate the operation type, whose value may be index, delete, replace, delete_index or swap_index. Please see [Linden Work Mode Document](LindenWorkMode.md) for detail.

### Search
Search is a GET API.

URL:  http://$IP:$Port/search?bql=$BQL

where $BQL is search request, see more in [BQL Document](BQL.md). Remember to do URL encoding.

### Delete
Delete is a POST API.

URL:  http://$IP:$Port/delete

Only one parameter is bql, which value is a BQL indicating a delete operation.

### Demo
A simple Http API demo is in ***com.xiaomi.linden.demo.cars.HttpAPIDemo*** in demo module.