Linden has 3 work modes for different scenarios: simple mode, hot-swap mode and multi-core mode. Different work mode corresponds to different linden core. One linden core corresponds to one physical index directory. 

**Simple mode**: the default mode used in linden, only one active linden core and it will never be replaced

**Hot-swap mode**: there is always only one active linden core and another linden core is in preparation status. After preparation, prepared linden core can be swapped to active status to replace the previous one

**Multi-core mode**: one shard index is split to several sub-indexes according some rules. Each sub-index corresponds to one linden core
 
In each work mode, linden consumes index data in JSON format from linden client (see detail in [Linden Client Document](LindenClient.md#index)) or LindenGateWay (see detail in [Linden Plugin Document](LindenPlugin.md#gateway)).
There is an operation type field in JSON format data, whose value may be index, delete, replace, swap\_index or delete\_index.

**index**: add a new document or overwriting the old document if it is existed in the same physical index directory.

**delete**: delete an existed document.

**replace**: overwriting the old document no matter the old document lives in which physical index directory.  Replace operation has the same effect with index operation in simple mode and hot-swap mode, since there is only one active index in these 2 modes.

**swap\_index** is only valid in hot-swap mode.
delete\_index is only valid in multi-core mode. 

### Simple Mode
This is linden default mode.
Index example:

	{
	    "type":"index",
	    "content":{
	        "id":3,
	        "title":"lucene"
	    }
	}
 
Content filed value is index document in JSONObject format. The document JSONObject must contain an ID field. The field name is defined in linden schema. This field is string type and it stores a unique string to indicate the identification of this document.  Same ID value document will be overwritten.
 
Delete example:

	{
	    "type":"delete",
	    "id":"3"
	}
	
This is a delete request example, which will delete “3” document.
 
### Hot Swap Mode
If you want to replace the old index with a completely new index and keep linden working for online requests, you can enable linden hot-swap mode in linden.properties.

	linden.core.mode=HOTSWAP
 
There is only 1 active index in hot-swap mode. In hot-swap mode, linden checks the index field in JSON data to decide which index to process.
If the JSON data has no index field, the data will be sent to the current working index. 
 
	{
	    "content":{
	        "id":"1",
	        "title":"active index data"
	    },
	    "type":"index"
	}
 
If the JSON data has index field, the data will be sent to the specified index. If the specified index is not existed, linden will create an empty index with the specified index name. The specified name index is a standby index in preparation status. Standby index name must start with “next\_” prefix.  Prefix + timestamp is a good naming convention.  Preparation data example:

	{
	    "content":{
	        "id":"1",
	        "title":"preparation data"
	    },
	    "index":"next_14375446",
	    "type":"index"
	}
 
 
After preparation is done, you can replace the active index with standby index by issuing a swap command in JSON format, and the previous active index will be closed.
Swap_index example:

	{
	    "index":"next_14375446",
	    "type":"swap_index"
	}
 
### Multiple Core Mode
Sometimes you may want split one shard index to several sub-indexes based on some strategies. You can use linden multiple core mode.
Linden has 3 splitting strategies:

* Time
* Doc number
* Index name
 
Time strategy: linden will create a new index for the following index data every time unit, i.e. hourly/daily/monthly/yearly. It is easily to control whole index life in time strategy. ***multi.index.max.live.index.num*** in linden.properties specified how many sub-indexes are active at most. If number exceeded, the oldest sub-index is retired automatically.
 
Doc number strategy: linden will create a new index when the latest index doc number exceeded ***multi.index.doc.num.limit*** in linden.properties. Same with time strategy the oldest sub-index is retired automatically if sub-index number exceeds the live index number limit, so it is easily to control whole index doc number.
 
Index name strategy: each indexing JSON data must has an index name in index field. Linden will send the data to specified sub-index. If the specified index doesn’t exist, linden will create an empty index with the index name. So it is all your decision that how many sub-index and which sub-index each doc belongs to.
 
**Index**

For time and doc number strategies, linden will send the document to latest sub-index, so there should not have index field.

	{
	    "content":{
	        "id":"1",
	        "title":"adding a new doc"
	    },
	    "type":"index"
	}
 
For index name strategy, indexing JSON data must have index field.

	{
	    "content":{
	        "id":"1",
	        "title":"preparation data"
	    },
	    "index":"demo-0",
	    "type":"index"
	}
 
**Delete**

Deleting operation will be applied to all live sub-indexes, since it is hard to know which sub-index the doc belongs to.

	{
	    "type":"delete",
	    "id":"1"
	}
 
**Replace**

The replacing has two steps: first try to delete the old doc from all active sub-indexes, and then send the new doc to the latest or specified sub-index.

For time and doc number strategies:

	{
	    "content":{
	        "id":"1",
	        "title":"replacing a new doc"
	    },
	    "type":"replace"
	}
 
For index name strategy:

	{
	    "content":{
	        "id":"1",
	        "title":"replacing a new doc"
	    },
	    "index":"demo-0",
	    "type":"replace"
	}
 
**Delete_index**

Sub-Index deletion only supports index name strategy, since in time and doc number strategy you may not know index name and sub-indexes number is limitted by multi.index.max.live.index.num.
 
	{
	    "index":"demo-0",
	    "type":"delete_index"
	}
 
**Configuration**

You can enable linden hot-swap mode in linden.properties.
	
	linden.core.mode=MULTI
 
Strategy configure in linden.properties:
	
	multi.index.division.type=TIME_HOUR/TIME_DAY/TIME_MONTH/TIME_YEAR
	multi.index.division.type=DOC_NUM
	multi.index.division.type=INDEX_NAME
 
