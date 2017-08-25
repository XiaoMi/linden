### STATIC SCHEMA

There is a linden schema file in linden configuration directory, which is named as schema.xml.
Schema file defines static linden field properties. This is a linden schema file example. It defines 3 fields in Linden: id, title and keywords.

	<?xml version="1.0" encoding="UTF-8" standalone="no"?>
	<schema>
	    <table id="id">
	      <column docvalues="yes" index="yes" multi="no" name="title" omitfreqs="no" omitnorms="no" snippet="yes" store="yes" tokenize="yes" type="string"/>
	      <column docvalues="no" index="yes" multi="no" name="keywords" omitfreqs="no" omitnorms="no" snippet="no" store="yes" tokenize="yes" type="string"/>
	    </table>
	</schema>
 
##### id
Linden has a special field: id. Id field is an un-tokenized string type field, we can only define its field name, by \<table id="xxx"\> in the schema file. In linden, id field uniquely identifies each document. Documents have the same id value will be overwritten.
 
##### name
Define the name of field in a document, uniquely identity for index and search.
 
##### type
Define field type, including String, Integer, Long, Double, Float, Facet. Linden field type is case insensitive.
 
##### index
Yes if this field should be indexed.
 
##### store
Yes if the field's value should be stored.
 
##### tokenize
Yes if this field's value should be analyzed by the analyzer
 
##### docvalues
Yes if the field's value should be stored as BinaryDocValuesField or NumericDocValuesField, which provides a much higher performance way to get field value from document than stored field.
 
##### multi
Yes if this field may contain multiple values for one field per document. One indexable JSON document example:

	{
	    "type":"index",
	    "content":{
	        "id":"6",
	        "tagstr":[
	            "MI4",
	            "MI Note",
	            "Note3"
	        ]
	    }
	}
	
tagstr is a multiple values field, it has 3 values in this example. The values are in JSONArray format.
 
##### omitfreqs
Yes if only documents are indexed, but term frequencies and positions are omitted. It is implemented by lucene index option “DOCS\_ONLY”.
 
##### omitnorms
Yes if normalization values should be omitted for the field. No norm values means that index time field boost and field length normalization value are disabled. By default length normalization value would allow you to boost shorter fields. Also index time boost will allow you boost some fields.
 
##### snippet
Yes if a text fragment containing your search terms in bold can be returned for your searched field.

<br>

### DYNAMIC SCHEMA
Linden also support dynamic schema, you can specify field properties at index time. The dynamic fields are put in a JSONArray named as \_dynamic. You can specify any properties of type, index, tokenize, docvalues, multi, omitfreqs, omitnorms and snippet by adding an underscore to the property name. Store property is not configurable and it is always true for dynamic field. Index property is true by default and the others are false. Here is an example.

	{
	    "type": "index",
	    "content": {
	        "_dynamic": [
	            {
	                "_type": "string",
	                "group": "search"
	            },
	            {
	                "_type": "long",
	                "cost": 30
	            },
	            {
	                "_type": "double",
	                "val": "10.0"
	            }
	        ],
	        "id": "1",
	        "level": "info"
	    }
	}
	
This indexable JSON document has 2 static fields “id” and “level”, 3 dynamic fields “group”, “cost” and “val”. 
Dynamic field type should also be specified in search time. For example:

	SELECT * FROM LINDEN WHERE group.string = "search" and cost.long < 100 source
 
Dynamic field provides convenience that you can add new field without change schema and restart linden, but it is not encouraged. Dynamic field properties are required at both index and search time, so the properties may be conflicted. Another reason is that dynamic field can’t be accessed in linden score model.