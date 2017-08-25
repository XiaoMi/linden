# BQL
BQL (Browsing Query Language) is the only query language between user and Linden.
BQL is a SQL-like Language. We use BQL to represent a query so that it is easy to understand.

# BASIC KEYWORD
    SELECT * FROM LINDEN BY QUERY IS "title:McDonald^0.8 and address:beijing" WHERE price < 100 LIMIT 0,10 EXPLAIN SOURCE

Keyword	| Description  |
--------------------|--------------------|
SELECT  | Select one or more fields in result documents if SOURCE enabled
LINDEN  | Default index name. LINDEN means searching all indexes. Specified index names only work in Linden multi core mode
BY | Indicates the start of query
WHERE | Indicates the start of filter
LIMIT | Specifies result offset and size, by default is 0, 10
EXPLAIN | Indicates fetching lucene explain information
SOURCE | Works with SELECT to fetch fields in hit documents

# ADVANCED KEYWORD
### AND
	SELECT * FROM LINDEN BY QUERY IS "title:phone" AND QUERY IS "address:xiaomi"
###  OR
	SELECT * FROM LINDEN BY QUERY IS "title:phone" OR QUERY IS "address:xiaomi"

### QUERY 
Filter:

		SELECT * FROM LINDEN WHERE QUERY is 'contents:(sports leather)' source

Query:

		SELECT * FROM LINDEN BY QUERY IS "title:McDonald^0.8 AND address:beijing"
		SELECT * FROM LINDEN BY QUERY IS 'title:"Beijing McDonald"~2'
		SELECT * FROM LINDEN BY QUERY IS 'title:"Bei*"' SOURCE
		
The string quoted is passed to lucene query parser. So all kinds of queries supported by lucene query parser are supported in linden natively. But remember to do lucene QueryParser escape before set the query string to BQL.

### IN, EXCEPT
Filter:

	SELECT * FROM LINDEN WHERE id IN (1,2,3) EXCEPT (4,5)
	SELECT * FROM LINDEN WHERE id NOT IN (1,2,3) EXCEPT (4,5)

Query:
	
	SELECT * FROM LINDEN BY id IN (1,2,3) EXCEPT (4,5)
	SELECT * FROM LINDEN BY id NOT IN (1,2,3) EXCEPT (4,5)

### NULL
Filter:

	SELECT * FROM LINDEN WHERE title IS NULL
	SELECT * FROM LINDEN WHERE title IS NOT NULL

Query:

	SELECT * FROM LINDEN BY title IS NULL
	SELECT * FROM LINDEN BY title IS NOT NULL

### COMPARISON
=, <>, >=, <=, >, < are supported in BQL

Filter:

	SELECT * FROM LINDEN WHERE id <> 1000
	SELECT * FROM LINDEN WHERE id <= 1000
	SELECT * FROM LINDEN WHERE title = 'beijing' SOURCE

Query:

	SELECT * FROM LINDEN BY id <> 1000
	SELECT * FROM LINDEN BY id <= 1000
	SELECT * FROM LINDEN BY title = 'beijing' SOURCE

### BETWEEN AND
Filter:

	SELECT * FROM LINDEN WHERE id BETWEEN 2000 AND 2004

Query:

	SELECT * FROM LINDEN BY id BETWEEN 2000 AND 2004

### LIKE
Wildcard matching, “*” match any string, “?” match any single character.

Filter:

	SELECT * FROM LINDEN WHERE title LIKE 'bl*'
	SELECT * FROM LINDEN WHERE title NOT LIKE 'bl*'

Query:

	SELECT * FROM LINDEN BY title LIKE 'bl*'
	SELECT * FROM LINDEN BY title NOT LIKE 'bl*'

### PIPING SYMBOL
BQL use “|” to connect sub-queries of disjunction max query
[https://lucene.apache.org/core/3_0_3/api/core/org/apache/lucene/search/DisjunctionMaxQuery.html](https://lucene.apache.org/core/3_0_3/api/core/org/apache/lucene/search/DisjunctionMaxQuery.html)

	SELECT * FROM LINDEN BY QUERY IS "name:XiaoMi" | SELECT * FROM LINDEN BY QUERY IS "name:MiPhone" BOOST BY 2

### FLEXIBLE QUERY
Flexible query is a customized query in linden, which allows user to search and score documents flexibly. Please refer to [flexible query doc](LindenFlexibleQuery.md) for detail.

	SELECT * FROM LINDEN BY flexible_query is "holiday hotel" in (title, address) USING MODEL simplest BEGIN
	float sum = 0f;
    for (int i = 0; i < getFieldLength(); ++i) {
        for (int j = 0; j < getTermLength(); ++j) {
            if (isMatched(i, j)) {
                sum += getScore(i, j);
            }
        }
    }
    return sum;
    END 
    Limit 0, 10

### DINSTANCE IN
Filter search result within radius of N kilometer. This only works if the documents contain 2 specific fields “latitude” and “longitude”, and both fields are double type.

	SELECT * FROM LINDEN WHERE DISTANCE(116.3,45) IN 10 and QUERY IS "title:McDonald AND address:beijing"

### SNIPPET
Get highlighted field value snippet for display

	SELECT * FROM linden QUERY query is 'title:test AND body:best' snippet title, body

The query above will return result containing highlighted title and body, e.g.

	title = This is a <b>test</b>
	body = This is body text containing <b>best</b>

### OP
Specified operator of BooleanQuery clauses, the default value is OR

	SELECT * FROM LINDEN by query is 'title:(Beijing McDonald)' OP(AND)
	SELECT * FROM LINDEN by query is 'title:(Beijing McDonald)' OP(OR)

### ROUTE
Indicates which shards are searched, by default all shards are searched
	
	SELECT * FROM LINDEN BY QUERY IS "title:McDonald AND address:beijing" ROUTE BY 0,1

### REPLICA_KEY
Works with ROUTE, it determines which replica is searched for one shard
	
	SELECT * FROM LINDEN BY QUERY IS "title:McDonald AND address:beijing" ROUTE BY REPLICA_KEY  "12345"

### IN TOP
Specifies how many documents at most to collect for scoring

	SELECT * FROM LINDEN WHERE title = 'beijing' IN TOP 500
	SELECT * FROM LINDEN WHERE title = 'beijing' ROUTE BY 0 IN TOP 500, 1
The query above means collecting at most 500 in shard 0 and unlimited in shard 1

### ORDER BY
Sort result by fields, the default order is descending. If there are multiple sort fields, the first field dominates, then second…

	SELECT * FROM LINDEN BY QUERY IS "title:McDonald AND address:beijing" ORDER BY rank, time
	SELECT * FROM LINDEN BY QUERY IS "title:McDonald AND address:beijing" ORDER BY rank DESC
	SELECT * FROM LINDEN BY QUERY IS "title:McDonald AND address:beijing" ORDER BY rank ASC

### GROUP BY
Group searching results by some field

	SELECT * FROM LINDEN BY QUERY IS "title:McDonald AND address:beijing" GROUP BY title

### BROWSE BY
This key word is used in facet search, https://en.wikipedia.org/wiki/Faceted_search
Get the distribution of documents under a specified path. It will aggregate all documents into some groups, and you get top N (10 as default) groups.

	SELECT * FROM LINDEN WHERE price > 15000 BROWSE BY makemodel(5, 'asian') SOURCE

Get top 5 group results under “asian” makemodel path with a price filter, result example

	{
    	"dim": "makemodel",
    	"path": "asian",
    	"value": 417,
    	"childCount": 3,
    	"labelValues": [
      		{
      	 	    "label": "acura",
      	     	 "value": 241
     	   },
        	{
          	"label": "lexus",
            	"value": 163
        	},
        	{
          	 "label": "infiniti",
           	 "value": 13
        	}
    	]
	}

### DRILL DOWN
The facet path works as a filter

	SELECT * FROM LINDEN DRILL DOWN makemodel('european/audi')
The facet makemodel field of any document returned is under “european/audi”

### DRILL SIDEWAYS
What is drill sideways?
[http://blog.mikemccandless.com/2013/02/drill-sideways-faceting-with-lucene.html](http://blog.mikemccandless.com/2013/02/drill-sideways-faceting-with-lucene.html)
	
	SELECT * FROM LINDEN BROWSE BY color(3), makemodel(5) DRILL SIDEWAYS makemodel('asian')

Get top 3 color groups under “asian” makemodel path and top 5 makemodel groups regardless makemodel path

### AGGREGATE
Aggregate result to different range buckets. Brackets [] means inclusive; braces {} means exclusive.

	SELECT * FROM LINDEN WHERE category='luxury' AGGREGATE BY price ({*, 10000}, [10000, 15000}, [15000, *}) SOURCE

Result example:

	{
	    "field": "price",
	    "labelValues": [
	        {
	            "label": "{*,10000}",
	            "value": 260
	        },
	        {
	            "label": "[10000,15000}",
	            "value": 1728
	        },
	        {
	            "label": "[15000,*}",
	            "value": 747
	        }
	    ]
	}


### BOOST

	SELECT * FROM LINDEN BY QUERY IS "name:XiaoMi" | SELECT * FROM LINDEN BY QUERY IS "name:MiPhone" BOOST BY 2
	
The second sub-query of this Disjunction Max Query weight is doubled.

### SUBBOOST

	SELECT * FROM LINDEN by title = 'test' subBoost BY 0.8 and id = 211 subBoost BY 2 boost BY 0.5
SUBBOOST means a sub-query boost. The 1st sub-query “title = 'test'” is boosted by 0.8 and the 2nd sub-query “id = 211” is boost by 2. The whole query is boosted by 0.5, however whole query boost is meaningless.

### ANDBOOST

	SELECT * FROM LINDEN BY title = 'test' subBoost BY 0.8 and id = 211 subBoost BY 2 andBoost BY 3 or query is 'content:test' subBoost BY 5 boost BY 0.5"
AndBoost means an “and” connected Boolean query boost. The Boolean query “by title = 'test' subBoost by 0.8 and id = 211 subBoost by 2” is boosted by 3.

### ORBOOST
	SELECT * FROM LINDEN BY title = 'test' subBoost by 0.8 and (id = 211 subBoost by 2 or query is 'content:test' orBoost by 3)
OrBoost means an “OR” connected Boolean query boost. The Boolean query “id = 211 subBoost by 2 or query is 'content:test' subBoost by 5” is boosted by 3.

### DISABLECOORD
DisableCoord of Boolean Query generated from string query by linden query parser is true by default.
	
	SELECT * FROM LINDEN BY query IS 'abc def' disableCoord FALSE

### ANDDISABLECOORD
AndDisableCoord means an “AND” connected Boolean query disableCoord.
	
	SELECT * FROM LINDEN BY query IS 'abc def' and query is 'ghi' andDisableCoord

disableCoord of Boolean Query “query IS 'abc def' and query is 'ghi'” is set to true

### ORDISABLECOORD
OrDisableCoord means an “OR” connected Boolean query disableCoord.

	SELECT * FROM LINDEN BY id = 123 or query is 'abc def' disableCoord orDisableCoord TRUE
disableCoord of String Query “query is 'abc def'” is set to true
disableCoord of Boolean Query “id = 123 or query is 'abc def'” is set to true

### DELETE
Delete documents from index

	DELETE FROM LINDEN WHERE title = 'beijing'


### SCORE MODEL
Linden allows user to customized score logic by a piece of code, which is called score model.

#### Field Access
In the code, you can access any field value by same field name function (i.e. add a pair of parentheses to the field name), except one type string field: indexed, tokenized, not stored and not BinaryDocValuesField.
For performance purpose, it is highly recommended to enable DocValues for the fields accessed in score model.

#### Parameter Types
Linden score model support parameter input, you can set different parameter values in each BQL.

Type | Parameter Sample |
--------------------|--------------------|
Integer | Integer a = 1
Long | Long a = 1
Double | Double a = 1.0
String | String a = “1234”
LongList | Long a = [1,2,3,4,5]
DoubleList | Double a = [1.0, 2.1, 3.2]
StringList | String a = [“a”, “b”, “c”]
Map | Map<String, Double> dict = {'a': 1.0, 'b': 2.0})

#### Score Model BQL

	SELECT id FROM LINDEN BY query is 'name:雷军'
	USING SCORE MODEL fullMatchBoosted(String q="雷军")
	BEGIN
	  float ret = score()/(name().length()+1);
	  if(q.equalsIgnoreCase(name())) {
	    ret += 1;
	  }
	  return ret;
	END
	LIMIT 0,40
The purpose of the above BQL is to get id field value of at most 40 documents, which are hit by query “name:雷军” and ranked by a customized score model. score() in the model returns lucene query score. The final score is the number of lucene score divided by name field length and boosted by name exactly match.

#### Score Model Plugin

It is hard to use score model in BQL when the java code is too long and too complicate. Linden provides plugin way to customized score logic. The plugin class should extend from *com.xiaomi.linden.core.search.query.model.LindenScoreModelStrategy*
The plugin can implement *public abstract double computeScore()* to any logic you want.
The BQL example

	SELECT id FROM LINDEN BY query is 'name:雷军' USING SCORE MODEL PLUGIN com.xiaomi.linden.example.FullMatchBoosted(String q="雷军")

One plugin example code:

	package com.xiaomi.linden.example;
	import com.xiaomi.linden.lucene.query.flexiblequery.FlexibleScoreModelStrategy;
	import java.io.IOException;
	public class FullMatchBoosted extends FlexibleScoreModelStrategy {
	    private FieldValues<String> name;
	    private String q;
	    @Override
	    public void init() throws IOException {
	        // get 'name' field value
	        name = getFieldValues("name");
	        // get plugin input parameter e.g. (String q="雷军")
	        q = getParams().get(0).getValue().getStringValue();
	    }
	    @Override
	    public double computeScore() throws IOException {
	        float ret = score()/(name.get().length()+1);
	        if(q.equalsIgnoreCase(name.get())) {
	            ret += 1;
	        }
	        return ret;
	    }
	}
 The plugin JAR should be placed under linden plugin path, which is defined as Plugin.Path in linden.properties.


### TIPS
String Value
In BQL we use single quote or double quote to indicate a string value

	SELECT * FROM LINDEN BY QUERY IS "title:phone"
	SELECT * FROM LINDEN BY QUERY IS 'title:phone'

Escape single quote in string

*	Quote the string with double quote

		SELECT * FROM LINDEN WHERE title = "She's gone"

*	Escape single quote with 2 single quotes

		SELECT * FROM LINDEN WHERE title = 'She''s gone'

Escape double quote in string

*	Quote the string with single quote

		SELECT * FROM LINDEN by query is 'title:"Beijing McDonald"~2'

*	Escape double quote with 2 double quote

		SELECT * FROM LINDEN by query is "title: ""Beijing McDonald""~2"

### Key Word Case Insensitive
All BQL key words are case insensitive. You can use any case format as you like, e.g. “SELECT”, “Select”, “select”

### Field Name Escape
Use reverse single quote to escape the field name if the field name is conflict with some BQL key word.
		
	SELECT * FROM LINDEN WHERE `FROM`="MiPhone"
While no deed to escape if the conflicted field name is in string value, quoted by single or double quotes, like this

	SELECT * FROM LINDEN BY QUERY IS "FROM:MiPhone"

For more BQL examples, please refer to ***com.xiaomi.linden.bql.TestBql***