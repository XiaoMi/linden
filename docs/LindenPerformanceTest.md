### Instance schema
* 11 fields in total
* 11 fields stored
* 5 tokenized string fields
	* Average string length of ‘subject’ field is about 10
	* Average string length of ‘message’ field is about 200
	* Average string length of ‘author’ field is about 10
	* Average string length of ‘authorid’ field is about 10
	* Average string length of ‘useip’ field is about 10

* 3 string term (non-tokenized) fields
	* Average string length of ‘thread_id’ field is about 7
	* Average string length of ‘forum_id’ field is about 4
	* Average string length of ‘port’ field is about 4

* 3 numeric fields
	* Long type field ‘dateline’
	* Integer type field ‘view’
	* Integer type field ‘replies’

### Index size
* 11,015,908 docs
* 3.2GB disk usage

### Machine info
* 2 * Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz
[http://ark.intel.com/products/75790/Intel-Xeon-Processor-E5-2630-v2-15M-Cache-2_60-GHz](http://ark.intel.com/products/75790/Intel-Xeon-Processor-E5-2630-v2-15M-Cache-2_60-GHz)
* DISK: SSD

### Linden configuration
* No cache
* Heap size 4G

### Test query scale
* 200,000 queries in total
* 40000 unique query
 
### Test Result
 
	Select * from linden by query is 'message:($query)' source 
 

 |            | QPS  | Average latency (MS) | Average number of hits |
 |------------|------|----------------------|------------------------|
 | 1 Instance | 2060 | 14                   | 146484                 |
 | 3 Instances| 4620 | 14                   | 146484                 |

 <br>
 
	Select * from linden by query is 'subject:($query) message:($query)' source
 
 |            | QPS  | Average latency (MS) | Average number of hits |
 |------------|------|----------------------|------------------------|
 | 1 Instance | 670  | 40                   | 181940                 |
 | 3 Instances| 1650 | 40                   | 181940                 |
