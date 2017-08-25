Flexible Query is a linden-customized query. It gives user flexibility to customize scoring logic from low index level, so we call it Flexible Query. It exposes some basic feature APIs and a 2-dimensions match matrix to you. Flexible Query must work with score model together.

One simplest example:

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

In this example, score model function name is “simplest”, the Java code between “BEGIN” and “END” is the exact scoring procedure. Flexible Query will generate a match matrix according to the tokenized query (“holiday hotel”) and search fields (title, address). In this case, the following matrix is generated:

 <sub>j</sub> &nbsp;&nbsp;&nbsp;&nbsp; <sup>i</sup>| 0, holiday | 1, hotel 
------------- | ------------- | -------------
0, field=title | Match info of **holiday** in **title** | Match info of **hotel** in **title**
1, field=address | Match info of **holiday** in **address** | Match info of **hotel** in **address**

Flexible Query supports some match info APIs you can call in score model.

API | Description |
--------------------|------------------|
getFieldLength() | Search fields count that your specify in BQL
getTermLength() | Query term length
getFieldBoost(i) | The i-th field boost, default 1
getTermBoost(j) | The j-th query term boost, default 1
isMatched(i, j) | Whether the j-th query term is matched in the i-th field
getRawScore(i,j) | Term score of the j-th query term in the i-th field.
getScore(i,j) | getRawScore(i,j) * getFieldBoost(i)
field(i, j) | The i-th field name. It is equivalent to field(i, 0)
text(i, j)  | The j-th query term text. It is equivalent to text(0, j)
freq(i, j) | Frequency of j-th query term in the i-th field.
positions(i,j) | Positions list of j-th query term in the i-th field.
writeExplanation(String format, Object... args) | Write root explanation with string format and arguments
setRootExpl(expl) | Set the root explanation
addTermExpl(i,j,ep) | Add explanation of the j-th term in the i-th field
addTermExpl(int i, int j, float score, String expl) | Add explanation of the j-th term in the i-th field with score information
addFieldExpl(i,ep) | Add explanation of the i-th field
addFieldExpl(int i, float score, String expl) | Add explanation of the i-th field with score information

To understand Flexible Query easily, let’s see the example in ***com.xiaomi.linden.lucene.query.flexiblequery.TestFlexibleQuery***

#### Index
4 Documents in the index:

 <sub>Doc Id</sub> &nbsp;&nbsp;&nbsp;&nbsp; <sup>Field Name</sup>| Text Field | Title Field 
------------- | ------------- | ------------- |
Doc 0 | hello world | hello lucene 
Doc 1 | hello lucene hello world | hello world
Doc 2 | world hello | lucene
Doc 3 | hello world lucene hello |world

#### Simplest Flexible Query
	SELECT * FROM LINDEN BY flexible_query is "hello world" in (text) USING MODEL simplest BEGIN
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

getFieldLength() returns 1;

getTermLength() returns 2;

field(0, j) returns “text”;

This flexible query has exactly same effect with lucene string query  “text:(hello world)”.

	SELECT * FROM LINDEN BY query is 'text:(hello world)'

Relevance order |
------------- |
Doc0, score: 0.6866505742073059 |
Doc2, score: 0.6866505742073059 |
Doc1, score: 0.6630884408950806 |
Doc3, score: 0.6630884408950806 |


Doc0 score is same with Doc2; Doc1 score is same with Doc3 score, since lucene string query doesn’t take position into consideration.

#### Field Boost Flexible Query
	SELECT * FROM LINDEN BY flexible_query is "hello world" in (text^2) USING MODEL fieldBoost1 BEGIN
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

This is a single field boost flexible query.
getFieldBoost(0) returns 2;
Relevance order is unchanged and  the score is double compared to result in example above.

Let’s see a two-field boost flexible query

	SELECT * FROM LINDEN BY flexible_query is "hello lucene" in (text, title^2) USING fieldBoost2 BEGIN
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

This flexible query works on 2 fields (text, title^2), text and title.

getFieldLength() returns 2;

getFieldBoost(0) returns 1;

getFieldBoost(1) returns 2.

The relevance order is Doc0, Doc2, Doc1 and Doc3.

If we don’t boost title field, the relevance order is Doc0, Doc1, Doc2 and Doc3.

Doc2 has higher rank than Doc1 under the title boost query, because Doc2 title has a term “lucene” while Doc1 doesn’t.

#### Term Boost Flexible Query

	SELECT * FROM LINDEN BY flexible_query is "hello world^3" in (text) USING MODEL termBoost BEGIN
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

getTermBoost(0) returns 1

getTermBoost(1) returns 3

This flexible query has exactly same effect with lucene string query  “text:(hello world^3)”.

	SELECT * FROM LINDEN BY query is 'text:(hello world^3)'


#### Continuous Match Boost Flexible Query
Lucene string query can’t boost score with position information, while flexible query can.  Here is a continuous match boost example.

	SELECT * FROM LINDEN BY flexible_query is "hello world lucene" in (text) USING MODEL continuousMatchBoost BEGIN
	   float sum = 0f;
	   int continuousMatches = 0;
	     for (int i = 0; i < getFieldLength(); ++i) {
	      int lastMatechedTermIdx = Integer.MIN_VALUE;
	      List<Integer> lastPositions = null;
	      List<Integer> curPositions;
	      for (int j = 0; j < getTermLength(); ++j) {
	        if (isMatched(i, j)) {
	          curPositions = positions(i, j);
	          if (lastMatechedTermIdx + 1 == j) {
	            for (int ii = 0; ii < lastPositions.size(); ++ii)
	              for (int jj = 0; jj < curPositions.size(); ++jj) {
	                if (lastPositions.get(ii) + 1 == curPositions.get(jj)) {
	                  ++continuousMatches;
	                }
	              }
	          }
	          lastMatechedTermIdx = j;
	          lastPositions = curPositions;
	          sum += getScore(i, j);
	        }
	      }
	    }
	    sum  += continuousMatches * 0.5;
	    return sum;
	END
	Limit 0, 10

If we use simple lucene string query  “text:(hello world lucene)”

	SELECT * FROM LINDEN BY query is 'text:(hello world lucene)'

Relevance order |
------------- |
Doc1, score: 0. 9201777577400208 | 
Doc3, score: 0. 9201777577400208 |
Doc0, score: 0. 4456756114959717 |
Doc2, score: 0. 4456756114959717 |

In continuous match boost query:

getFieldLength() returns 1

getTermLength() returns 3

Query terms [“hello”, “world”, “lucene”]

Position Matrix:

<sub>Text Field</sub> &nbsp;&nbsp;&nbsp;&nbsp; <sup>Query Terms</sup>| text:hello | text:world | text:lucene 
------------- | ------------- | ------------- | ------------- |
Doc 0: hello world | [0] | [1] | N/A
Doc 1: hello lucene hello word | [0,2] | [3] | [1]
Doc 2: world hello | [1] | [0] | N/A
Doc 3: hello world lucene hello | [0,3] | [1] | [2]

The relevance order continuous match boost flexible query is:

Relevance order |
------------- |
Doc3, score: 1.920177698135376 |
Doc1, score: 1.420177698135376 |
Doc0, score: 0.9456756114959717 |
Doc2, score: 0. 4456756114959717 |

In Doc3, there are 2 continuous matches  (hello@0, world@1) (world@1, lucene@2)

In Doc1, there is 1 continuous match  (hello@2, world@3)

In Doc0, there is 1 continuous match  (hello@0, world@1)

In Doc2, there is no continuous match

So Doc3 score is 2*0.5 higher than the score of simple lucene string query; while Doc1 score and Doc0 score has 0.5 higher than the scores of simple lucene string query.


#### Match Ratio Controlled Flexible Query

Sometimes you may want to control how many query terms to be matched. Flexible query can easily handle this.

	SELECT * FROM LINDEN BY flexible_query is "hello lucene" full_match in (title) USING MODEL simplest BEGIN
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
Key word “full_match” indicates all query terms should be matched，so only doc 0 is matched，doc 0 title field is “hello lucene”

	SELECT * FROM LINDEN BY flexible_query is "hello world lucene" match 0.5 in (title) USING MODEL simplest BEGIN
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

This query requires match ratio is greater than 0.5. There are 3 query terms, so the threshold is ceil(0.5*3) = 2.

Doc 0 with title field “hello lucene” and Doc 1 with title field “hello world” are matched. And of course “match 1” is same with “full_match”.

#### Flexible Query Explanation
Flexible Query provides a default explanation, and it allows users to customize explanation, so that we can easily debug our score model. Remember explanation should only be enabled in debug mode, it may drop performance.

	SELECT * FROM LINDEN BY flexible_query is "hello world lucene" in (text) USING MODEL continuousMatchBoostExplained BEGIN
	   setRootExpl("Explanation of continuousMatchBoostExplained model:");
	   float sum = 0f;
	   for (int i = 0; i < getFieldLength(); ++i) {
	      int continuousMatches = 0;
	      float fieldScore = 0f;
	      int lastMatechedTermIdx = Integer.MIN_VALUE;
	      List<Integer> lastPositions = null;
	      List<Integer> curPositions;
	      for (int j = 0; j < getTermLength(); ++j) {
	        if (isMatched(i, j)) {
	          curPositions = positions(i, j);
	          if (lastMatechedTermIdx + 1 == j) {
	            for (int ii = 0; ii < lastPositions.size(); ++ii)
	              for (int jj = 0; jj < curPositions.size(); ++jj) {
	                if (lastPositions.get(ii) + 1 == curPositions.get(jj)) {
	                  ++continuousMatches;
	                }
	              }
	          }
	          lastMatechedTermIdx = j;
	          lastPositions = curPositions;
	          float termScore = getScore(i, j);
	          fieldScore += termScore;
	          addTermExpl(i, j, termScore, getExpl("%s is matched in %s field, positions are %s", text(i, j), field(i, j), curPositions));
	        }
	      }
	      fieldScore += continuousMatches * 0.5;
	      addFieldExpl(i, fieldScore, getExpl("%d continuous matches in %s field", continuousMatches, field(i, 0)));
	      sum += fieldScore;
	    }
	    return sum;
	END
	EXPLAIN Limit 0, 10


The explanations result:

	Explanation of Doc 3
	* Explanation of continuousMatchBoostExplained model:
	** 2 continuous matches in text field        [FIELD:text MATCHED:3]
	*** hello is matched in text field, positions are [0, 3]
	*** world is matched in text field, positions are [1]
	*** lucene is matched in text field, positions are [2]
	
	Explanation of Doc 1
	* Explanation of continuousMatchBoostExplained model:
	** 1 continuous matches in text field        [FIELD:text MATCHED:3]
	*** hello is matched in text field, positions are [0, 2]
	*** world is matched in text field, positions are [3]
	*** lucene is matched in text field, positions are [1]
	
	Explanation of Doc 0
	* Explanation of continuousMatchBoostExplained model:
	** 1 continuous matches in text field        [FIELD:text MATCHED:2]
	*** hello is matched in text field, positions are [0]
	*** world is matched in text field, positions are [1]
	
	Explanation of Doc 2
	* Explanation of continuousMatchBoostExplained model:
	** 0 continuous matches in text field        [FIELD:text MATCHED:2]
	*** hello is matched in text field, positions are [1]
	*** world is matched in text field, positions are [0]

#### Conclusion
Flexible Query makes user customize scoring logic flexibly.  It provides field boost, term boost, and positions information APIs in score model.  And of course,  Flexible Query score model inherits all features of normal linden score model such as field value access and score model plugin support. See more in [BQL Document](BQL.md#score-model).