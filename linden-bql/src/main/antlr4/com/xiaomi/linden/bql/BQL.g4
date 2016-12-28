grammar BQL;

@parser::members {

    private static enum KeyType {
      STRING_LITERAL,
      IDENT,
      STRING_LITERAL_AND_IDENT
    }
}

// ***************** parser rules:

statement
    :   (select_stmt | delete_stmt)   SEMI? EOF |
        multi_select EOF
    ;

delete_stmt
    :   DELETE FROM indexes=table_stmt (dw=where)?
        (route_param = route_by_clause)* ;

multi_select
    : select_stmt SEMI? ('|' select_stmt SEMI?)*
    ;

select_stmt
    :   SELECT ('*' | cols=selection_list)
        (FROM tables=table_stmt)?
        q=query_where?
        w=where?
        (   order_by = order_by_clause
        |   limit = limit_clause
        |   group_by = group_by_clause
        |   browse_by = browse_by_clause
        |   aggregate_by = aggregate_by_clause
        |   drill = drill_clause
        |   explain = explain_clause
        |   source = source_clause
        |   snippet = snippet_clause
        |   route_param = route_by_clause
        |   scoring_model = score_model_clause
        |   boost_by = boost_by_clause
        |   in_top = in_top_clause
        )*
    ;

disable_coord_clause
    :  DISABLE_COORD (TRUE | FALSE)?
    ;

and_disable_coord_clause
    :  AND_DISABLE_COORD (TRUE | FALSE)?
    ;

or_disable_coord_clause
    :  OR_DISABLE_COORD (TRUE | FALSE)?
    ;

table_stmt
    :   (def='default' | IDENT)?
        (COMMA IDENT)*
    ;

selection_list
    :   (   col=column_name
        )
        (   COMMA
            (   col=column_name
            )
        )*
    ;

column_name
    :   (IDENT | ESCAPED_IDENT)
    ;

where
    :   WHERE search_expr
    ;

query_where
    :   (QUERY|BY) search_expr
    ;

order_by_clause
    :   ORDER BY sort_specs
    ;

sort_specs
    :   sort=sort_spec
        (COMMA sort=sort_spec   // It's OK to use variable sort again here
        )*
    ;

sort_spec
    :
    (       column_name
        |   { "distance".equals(_input.LT(1).getText()) }? DISTANCE
        |   { "score".equals(_input.LT(1).getText()) }? SCORE
    )
        ordering=(ASC | DESC)?
    ;

limit_clause
    :   LIMIT (n1=numeric_value COMMA)? n2=numeric_value
    ;

group_by_clause
    :   GROUP BY column_name (TOP top = (INTEGER | PLACEHOLDER))?
    ;

browse_by_clause
    :   BROWSE BY f=facet_spec
        (COMMA f=facet_spec
        )*
    ;

aggregate_by_clause
    :   AGGREGATE BY aggregation=aggregation_spec
        (COMMA aggregation=aggregation_spec
        )*
    ;

facet_spec
    :   column_name
        (
            LPAR
            n1=INTEGER
            (COMMA path=STRING_LITERAL)?
            RPAR
        )?
    ;

aggregation_spec
    :   column_name
        (
             LPAR
             bucket = bucket_spec
             (COMMA bucket=bucket_spec
             )*
             RPAR
        )?
    ;

bucket_spec
    :  (LBRACE | LBRACK)
       (n1 = numeric_value | s1 = STAR)
       COMMA
       (n2 = numeric_value | s2 = STAR)
       (RBRACE | RBRACK)
    ;


drill_clause
    :   (DRILL DOWN | DRILL SIDEWAYS) f=drill_spec
        (COMMA f=drill_spec
        )*
    ;

drill_spec
    :   column_name
        (
            LPAR
            path=STRING_LITERAL
            RPAR
        )?
    ;

explain_clause
    :   EXPLAIN
        e1 = (   TRUE
        |   FALSE
        |   PLACEHOLDER
        )?
    ;

source_clause
    :   SOURCE
        s1 = (   TRUE
        |   FALSE
        |   PLACEHOLDER
        )?
    ;

snippet_clause
    :   SNIPPET
        selection_list
    ;

route_by_clause
    :   ROUTE BY (route_shard_clause
    |   route_replica_clause
    |   route_shard_clause COMMA route_replica_clause)
    ;

route_replica_clause
    :   REPLICA_KEY STRING_LITERAL
    ;

route_shard_clause
    :   route_shard_value
        (COMMA route_shard_value)*
    ;

route_shard_value
    :   route_single_shard_value
    |   route_multi_shard_values
    ;

route_single_shard_value
    :   numeric_value (in_top_clause)?
    ;

route_multi_shard_values
    :   LPAR mn=numeric_value (COMMA mn=numeric_value)* RPAR in_top_clause
    ;

search_expr
    :   t=term_expr |
        (term_op=MUST)? t=term_expr (OR t=term_expr)+ (disable_coord = or_disable_coord_clause)? (boost_by = or_boost_by_clause)?
    ;

term_expr
    :   f=factor_expr |
        f=factor_expr (AND f=factor_expr)+ (disable_coord = and_disable_coord_clause)? (boost_by = and_boost_by_clause)?
    ;

factor_expr
    :   predicate
    |   LPAR search_expr RPAR
    ;

predicate
    :   (in_predicate
    |   equal_predicate
    |   not_equal_predicate
    |   query_predicate
    |   between_predicate
    |   range_predicate
    |   like_predicate
    |   null_predicate
    |   flexible_query_predicate
    |   distance_predicate)
        (boost_by = sub_boost_by_clause)?
    ;

in_predicate
    :   column_name not=NOT? IN value_list except=except_clause?
    ;

equal_predicate
    :   column_name EQUAL value
    ;

not_equal_predicate
    :   column_name NOT_EQUAL value
    ;

query_predicate
    :   QUERY IS STRING_LITERAL (disable_coord = disable_coord_clause)? (OP LPAR (AND|OR) RPAR)?
    ;

between_predicate
    :   column_name not=NOT? BETWEEN val1=value AND val2=value
    ;

range_predicate
    :   column_name op=(GT | GTE | LT | LTE) val=value
    ;

flexible_query_predicate
    :   FLEXIBLE_QUERY IS STRING_LITERAL ((fm=FULL_MATCH)|(MATCH mrt=numeric_value))? (gi=GLOBAL_IDF (OF gfd=global_fields)?)? IN flexible_fields
        USING MODEL (PLUGIN)? (OVERRIDE)? IDENT (flexible_params=formal_parameters)?
        model=score_model?
    ;

global_fields
    :   LPAR field=flexible_field
        (COMMA field=flexible_field)*
        RPAR
    ;

flexible_fields
    :   LPAR field=flexible_field
        (COMMA field=flexible_field)*
        RPAR
    ;

flexible_field
    :   column_name (CARET boost=numeric_value)?
    ;

distance_predicate
    :   DISTANCE LPAR lat=numeric_value ',' lon=numeric_value RPAR IN range=numeric_value
    ;

like_predicate
    :   column_name (NOT)? LIKE (STRING_LITERAL | PLACEHOLDER)
    ;

null_predicate
    :   column_name IS (NOT)? NULL
    ;


value_list
    :   LPAR v=value
        (   COMMA v=value
        )*
        RPAR
    |   LPAR RPAR
    ;

python_style_list
    :   '[' v=python_style_value?
        (   COMMA v=python_style_value
        )*
        ']'
    ;

python_style_dict
    :   '{' '}'
    |   '{' p=key_value_pair[KeyType.STRING_LITERAL]
        (   COMMA p=key_value_pair[KeyType.STRING_LITERAL]
        )*
        '}'
    ;

python_style_value
    :   value
    |   python_style_list
    |   python_style_dict
    ;

value
    :   numeric
    |   STRING_LITERAL
    |   TRUE
    |   FALSE
    |   PLACEHOLDER
    ;

numeric
    :   INTEGER
    |   REAL
    ;

numeric_value
    :   numeric
    |   PLACEHOLDER
    ;

except_clause
    :   EXCEPT value_list
    ;

key_value_pair[KeyType keyType]
    :   ( { $keyType == KeyType.STRING_LITERAL ||
            $keyType == KeyType.STRING_LITERAL_AND_IDENT}? STRING_LITERAL
        | { $keyType == KeyType.IDENT ||
            $keyType == KeyType.STRING_LITERAL_AND_IDENT}? IDENT
        )
        COLON (v=value | vs=python_style_list | vd=python_style_dict)
    ;

// =====================================================================
// Relevance model related
// =====================================================================

variable_declarators
    :   var1=variable_declarator
        (COMMA var2=variable_declarator
        )*
    ;

variable_declarator
    :   variable_declarator_id ('=' variable_initializer)?
    ;

variable_declarator_id
    :   IDENT ('[' ']')*
    ;

variable_initializer
    :   array_initializer
    |   expression
    ;

array_initializer
    :   '{' (variable_initializer (',' variable_initializer)* (',')?)? '}'
    ;

type
    :   primitive_type ('[' ']')*
    |   boxed_type ('[' ']')*
    |   limited_type ('[' ']')*
    |   map_type
    ;

map_type
    :   'Map' '<'left=type',' rigth=type'>'
    ;

formal_parameters
    :   LPAR formal_parameter_decls RPAR
    ;

formal_parameter_decls
    :   decl=formal_parameter_decl
        (COMMA decl=formal_parameter_decl
        )*
    ;

formal_parameter_decl
    :   variable_modifiers type variable_declarator_id EQUAL python_style_value
    ;

primitive_type
    :   { "boolean".equals(_input.LT(1).getText()) }? BOOLEAN
    |   'char'
    |   { "byte".equals(_input.LT(1).getText()) }? BYTE
    |   'short'
    |   { "int".equals(_input.LT(1).getText()) }? INT
    |   'int'
    |   { "long".equals(_input.LT(1).getText()) }? LONG
    |   'float'
    |   { "double".equals(_input.LT(1).getText()) }? DOUBLE
    ;

boxed_type
    :   { "Boolean".equals(_input.LT(1).getText()) }? BOOLEAN
    |   'Character'
    |   { "Byte".equals(_input.LT(1).getText()) }? BYTE
    |   'Short'
    |   'Integer'
    |   { "Long".equals(_input.LT(1).getText()) }? LONG
    |   'Float'
    |   { "Double".equals(_input.LT(1).getText()) }? DOUBLE
    ;

limited_type
    :   'String'
    |   'System'
    |   'Math'
    ;

variable_modifier
    :   'final'
    ;

score_model
    :    BEGIN model_block END
    ;

model_block
    :   block_statement+
    ;

block
    :   '{'
        block_statement*
        '}'
    ;

block_statement
    :   local_variable_declaration_stmt
    |   java_statement
    ;

local_variable_declaration_stmt
    :   local_variable_declaration SEMI
    ;

local_variable_declaration
    :   variable_modifiers type variable_declarators
    ;

variable_modifiers
    :   variable_modifier*
    ;

java_statement
    :   block
    |   'if' par_expression java_statement (else_statement)?
    |   FOR LPAR for_control RPAR java_statement
    |   'while' par_expression java_statement
    |   'do' java_statement 'while' par_expression SEMI
    |   'switch' par_expression '{' switch_block_statement_groups '}'
    |   'return' expression SEMI
    |   'break' IDENT? SEMI
    |   'continue' IDENT? SEMI
    |   SEMI
    |   statement_expression SEMI
    ;

else_statement
    :   { "else".equals(_input.LT(1).getText()) }? ELSE java_statement
    ;

switch_block_statement_groups
    :   (switch_block_statement_group)*
    ;

switch_block_statement_group
    :   switch_label+ block_statement*
    ;

switch_label
    :   'case' constant_expression COLON
    |   'case' enum_constant_name COLON
    |   'default' COLON
    ;

for_control
    :   enhanced_for_control
    |   for_init? SEMI expression? SEMI for_update?
    ;

for_init
    :   local_variable_declaration
    |   expression_list
    ;

enhanced_for_control
    :   variable_modifiers type IDENT COLON expression
    ;

for_update
    :   expression_list
    ;

par_expression
    :   LPAR expression RPAR
    ;

expression_list
    :   expression (',' expression)*
    ;

statement_expression
    :   expression
    ;

constant_expression
    :   expression
    ;

enum_constant_name
    :   IDENT
    ;

expression
    :   conditional_expression (assignment_operator expression)?
    |   'new' creator
    |   primary
    ;

creator
    : createdName (arrayCreatorRest) ;

createdName
    :   primitive_type ;

arrayCreatorRest
    :   '[' expression ']' ('[' expression ']')* ('[' ']')*
    ;

assignment_operator
    :   '='
    |   '+='
    |   '-='
    |   '*='
    |   '/='
    |   '&='
    |   '|='
    |   '^='
    |   '%='
    |   '<' '<' '='
    |   '>' '>' '>' '='
    |   '>' '>' '='
    ;

conditional_expression
    :   conditional_or_expression ( '?' expression ':' expression )?
    ;

conditional_or_expression
    :   conditional_and_expression ( '||' conditional_and_expression )*
    ;

conditional_and_expression
    :   inclusive_or_expression ('&&' inclusive_or_expression )*
    ;

inclusive_or_expression
    :   exclusive_or_expression ('|' exclusive_or_expression )*
    ;

exclusive_or_expression
    :   and_expression ('^' and_expression )*
    ;

and_expression
    :   equality_expression ( '&' equality_expression )*
    ;

equality_expression
    :   instanceof_expression ( ('==' | '!=') instanceof_expression )*
    ;

instanceof_expression
    :   relational_expression ('instanceof' type)?
    ;

relational_expression
    :   shift_expression ( relational_op shift_expression )*
    ;

relational_op
    :   '<='
    |   '>='
    |   '<'
    |   '>'
    ;

shift_expression
    :   additive_expression ( shift_op additive_expression )*
    ;

shift_op
    :   '<' '<'
    |   '>' '>' '>'
    |   '>' '>'
    ;

additive_expression
    :   multiplicative_expression ( ('+' | '-') multiplicative_expression )*
    ;

multiplicative_expression
    :   unary_expression ( ( '*' | '/' | '%' ) unary_expression )*
    ;

unary_expression
    :   '+' unary_expression
    |   '-' unary_expression
    |   '++' unary_expression
    |   '--' unary_expression
    |   unary_expression_not_plus_minus
    ;

unary_expression_not_plus_minus
    :   '~' unary_expression
    |   '!' unary_expression
    |   cast_expression
    |   primary selector* ('++'|'--')?
    ;

cast_expression
    :  '(' primitive_type ')' unary_expression
    |  '(' (type | expression) ')' unary_expression_not_plus_minus
    ;

primary
    :   par_expression
    |   { "score()".equals(_input.LT(1).getText()) }? 'score()'
    |   { "score".equals(_input.LT(1).getText()) }? SCORE
    |   { "distance()".equals(_input.LT(1).getText()) }? 'distance()'
    |   { "distance".equals(_input.LT(1).getText()) }? DISTANCE
    |   literal
    |   java_method identifier_suffix
    |   java_ident ('.' java_method)* identifier_suffix?
    ;

java_ident
    :   boxed_type
    |   limited_type
    |   IDENT
    ;

// Need to handle the conflicts of BQL keywords and common Java method
// names supported by BQL.
java_method
    :   { "contains".equals(_input.LT(1).getText()) }? CONTAINS
    |   IDENT
    ;

identifier_suffix
    :   ('[' ']')+ '.' 'class'
    |   arguments
    |   '.' 'class'
    |   '.' 'this'
    |   '.' 'super' arguments
    ;

literal
    :   integer_literal
    |   REAL
    |   FLOATING_POINT_LITERAL
    |   CHARACTER_LITERAL
    |   STRING_LITERAL
    |   boolean_literal
    |   { "null".equals(_input.LT(1).getText()) }? NULL
    ;

integer_literal
    :   HEX_LITERAL
    |   OCTAL_LITERAL
    |   INTEGER
    ;

boolean_literal
    :   { "true".equals(_input.LT(1).getText()) }? TRUE
    |   { "false".equals(_input.LT(1).getText()) }? FALSE
    ;

selector
    :   '.' IDENT arguments?
    |   '.' 'this'
    |   '[' expression ']'
    ;

arguments
    :   '(' expression_list? ')'
    ;

score_model_clause
    :   USING SCORE MODEL (PLUGIN)? (OVERRIDE)? IDENT (params=formal_parameters)? model=score_model?
    ;

boost_by_clause
    :   BOOST BY boost=numeric_value
    ;

sub_boost_by_clause
    :   SUB_BOOST BY boost=numeric_value
    ;

and_boost_by_clause
    :   AND_BOOST BY boost=numeric_value
    ;

or_boost_by_clause
    :   OR_BOOST BY boost=numeric_value
    ;

in_top_clause
    :   IN TOP max_num=numeric_value
    ;

fragment DIGIT : '0'..'9' ;
fragment ALPHA : 'a'..'z' | 'A'..'Z' ;

INTEGER : ('-')? ('0' | '1'..'9' '0'..'9'*) INTEGER_TYPE_SUFFIX? ;
REAL : ('-')? DIGIT+ '.' DIGIT*('E'|'e')?('+'|'-')?(DIGIT*)? ;
LPAR : '(' ;
RPAR : ')' ;
COMMA : ',' ;
COLON : ':' ;
SEMI : ';' ;
EQUAL : '=' ;
GT : '>' ;
GTE : '>=' ;
LT : '<' ;
LTE : '<=';
NOT_EQUAL : '<>' ;
LBRACE : '{';
RBRACE : '}';
LBRACK : '[';
RBRACK : ']';
STAR : '*';
CARET : '^';

STRING_LITERAL
    :   '"'
        (   '"' '"'
        |   ~('"'|'\r'|'\n')
        )*
        '"'
    |   '\''
        (   '\'' '\''
        |   ~('\''|'\r'|'\n')
        )*
        '\''
    ;

//
// BQL Relevance model related
//

fragment HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;
fragment INTEGER_TYPE_SUFFIX: ('l' | 'L') ;
fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;
fragment FLOAT_TYPE_SUFFIX : ('f'|'F'|'d'|'D') ;

fragment
ESCAPE_SEQUENCE
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESCAPE
    |   OCTAL_ESCAPE
    ;

fragment
UNICODE_ESCAPE
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

fragment
OCTAL_ESCAPE
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

HEX_LITERAL : '0' ('x'|'X') HEX_DIGIT+ INTEGER_TYPE_SUFFIX? ;
OCTAL_LITERAL : '0' ('0'..'7')+ INTEGER_TYPE_SUFFIX? ;

FLOATING_POINT_LITERAL
    :   REAL EXPONENT? FLOAT_TYPE_SUFFIX?
    |   '.' DIGIT+ EXPONENT? FLOAT_TYPE_SUFFIX?
    |   DIGIT+ EXPONENT FLOAT_TYPE_SUFFIX?
    |   DIGIT+ FLOAT_TYPE_SUFFIX
    ;

CHARACTER_LITERAL
    :   '\'' ( ESCAPE_SEQUENCE | ~('\''|'\\') ) '\''
    ;

//
// Java-only Keywords
//

BREAK : 'break';
CASE : 'case';
CHAR : 'char';
CHARACTER : 'Character';
CLASS : 'class';
CONTINUE : 'continue';
DO : 'do';
EXTENDS : 'extends';
FINAL : 'final';
FLOAT : 'float';
FLOAT2 : 'Float';
FOR : 'for';
IF : 'if';
INTEGER2 : 'Integer';
INSTANCEOF : 'instanceof';
MATH : 'Math';
MIN : 'min';
RETURN : 'return';
SHORT : 'short';
SHORT2 : 'Short';
STRING2 : 'String';
SUPER : 'super';
SWITCH : 'switch';
SYSTEM : 'System';
THIS : 'this';
WHILE : 'while';

//
// BQL Keywords
//

AGGREGATE : [Aa][Gg][Gg][Rr][Ee][Gg][Aa][Tt][Ee] ;
ALL : [Aa][Ll][Ll] ;
AND : [Aa][Nn][Dd] ;
ASC : [Aa][Ss][Cc] ;
BEGIN : [Bb][Ee][Gg][Ii][Nn] ;
BETWEEN : [Bb][Ee][Tt][Ww][Ee][Ee][Nn] ;
BOOLEAN : [Bb][Oo][Oo][Ll][Ee][Aa][Nn] ;
BOOST : [Bb][Oo][Oo][Ss][Tt] ;
AND_BOOST : [Aa][Nn][Dd][Bb][Oo][Oo][Ss][Tt] ;
OR_BOOST : [Oo][Rr][Bb][Oo][Oo][Ss][Tt] ;
SUB_BOOST : [Ss][Uu][Bb][Bb][Oo][Oo][Ss][Tt] ;
BROWSE : [Bb][Rr][Oo][Ww][Ss][Ee] ;
BY : [Bb][Yy] ;
CONTAINS : [Cc][Oo][Nn][Tt][Aa][Ii][Nn][Ss] ;
BYTE : [Bb][Yy][Tt][Ee] ;
DELETE: [Dd][Ee][Ll][Ee][Tt][Ee] ;
DISABLE_COORD : [Dd][Ii][Ss][Aa][Bb][Ll][Ee][Cc][Oo][Oo][Rr][Dd] ;
AND_DISABLE_COORD : [Aa][Nn][Dd][Dd][Ii][Ss][Aa][Bb][Ll][Ee][Cc][Oo][Oo][Rr][Dd] ;
OR_DISABLE_COORD : [Oo][Rr][Dd][Ii][Ss][Aa][Bb][Ll][Ee][Cc][Oo][Oo][Rr][Dd] ;
DOWN : [Dd][Oo][Ww][Nn] ;
DRILL : [Dd][Rr][Ii][Ll][Ll] ;
DEFINED : [Dd][Ee][Ff][Ii][Nn][Ee][Dd] ;
DESC : [Dd][Ee][Ss][Cc] ;
DISTANCE : [Dd][Ii][Ss][Tt][Aa][Nn][Cc][Ee]? ;
DOUBLE : [Dd][Oo][Uu][Bb][Ll][Ee] ;
ELSE : [Ee][Ll][Ss][Ee] ;
END : [Ee][Nn][Dd] ;
EXCEPT : [Ee][Xx][Cc][Ee][Pp][Tt] ;
EXPLAIN : [Ee][Xx][Pp][Ll][Aa][Ii][Nn] ;
FALSE : [Ff][Aa][Ll][Ss][Ee] ;
FETCHING : [Ff][Ee][Tt][Cc][Hh][Ii][Nn][Gg] ;
FLEXIBLE_QUERY : [Ff][Ll][Ee][Xx][Ii][Bb][Ll][Ee][_][Qq][Uu][Ee][Rr][Yy]? ;
FULL_MATCH : [Ff][Uu][Ll][Ll][_][Mm][Aa][Tt][Cc][Hh]? ;
FROM : [Ff][Rr][Oo][Mm] ;
GLOBAL_IDF: [Gg][Ll][Oo][Bb][Aa][Ll][_][Ii][Dd][Ff]? ;
GROUP : [Gg][Rr][Oo][Uu][Pp] ;
IN : [Ii][Nn] ;
INT : [Ii][Nn][Tt] ;
IS : [Ii][Ss] ;
LIKE : [Ll][Ii][Kk][Ee] ;
LIMIT : [Ll][Ii][Mm][Ii][Tt] ;
LONG : [Ll][Oo][Nn][Gg] ;
MATCH : [Mm][Aa][Tt][Cc][Hh] ;
MODEL : [Mm][Oo][Dd][Ee][Ll] ;
MUST: [Mm][Uu][Ss][Tt] ;
NOT : [Nn][Oo][Tt] ;
NULL : [Nn][Uu][Ll][Ll] ;
OF : [Oo][Ff] ;
OP : [Oo][Pp]? ;
OR : [Oo][Rr] ;
OVERALL_IDF: [Oo][Vv][Ee][Rr][Aa][Ll][Ll][_][Ii][Dd][Ff] ;
OVERRIDE: [Oo][Vv][Ee][Rr][Rr][Ii][Dd][Ee] ;
ORDER : [Oo][Rr][Dd][Ee][Rr] ;
PLUGIN: [Pp][Ll][Uu][Gg][Ii][Nn] ;
QUERY : [Qq][Uu][Ee][Rr][Yy] ;
ROUTE : [Rr][Oo][Uu][Tt][Ee] ;
REPLICA_KEY : [Rr][Ee][Pp][Ll][Ii][Cc][Aa][_][Kk][Ee][Yy] ;
SCORE: [Ss][Cc][Oo][Rr][Ee] ;
SELECT : [Ss][Ee][Ll][Ee][Cc][Tt] ;
SIDEWAYS : [Ss][Ii][Dd][Ee][Ww][Aa][Yy][Ss] ;
SNIPPET: [Ss][Nn][Ii][Pp][Pp][Ee][Tt] ;
SOURCE: [Ss][Oo][Uu][Rr][Cc][Ee] ;
STORED : [Ss][Tt][Oo][Rr][Ee][Dd] ;
STRING : [Ss][Tt][Rr][Ii][Nn][Gg] ;
TOP : [Tt][Oo][Pp] ;
TRUE : [Tt][Rr][Uu][Ee] ;
USING : [Uu][Ss][Ii][Nn][Gg] ;
WHERE : [Ww][Hh][Ee][Rr][Ee] ;
WITH : [Ww][Ii][Tt][Hh] ;


// Have to define this after the keywords?
IDENT : (ALPHA | '_') (ALPHA | DIGIT | '-' | '_' | '.')* ;
ESCAPED_IDENT : '`' IDENT '`';

PLACEHOLDER : '$' (ALPHA | '_') (ALPHA | DIGIT | '_')* ;

WS : ( ' ' | '\t' | '\r' | '\n' )+ -> channel(HIDDEN);

COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

LINE_COMMENT
    : '--' ~('\n'|'\r')* -> channel(HIDDEN)
    ;

