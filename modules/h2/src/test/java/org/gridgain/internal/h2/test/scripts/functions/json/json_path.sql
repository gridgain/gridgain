-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- IS_JSON Tests for a valid JSON value. This can be a JSON object, array, number, string or one of the three literal values (false, true, null)

SELECT IS_JSON('{"a":[3]}');
>> TRUE

SELECT IS_JSON('[{"a":1}]');
>> TRUE

SELECT IS_JSON(1);
>> TRUE

SELECT IS_JSON('scalar string');
>> TRUE

SELECT IS_JSON(true);
>> TRUE

SELECT IS_JSON('{"a"');
>> FALSE

SELECT IS_JSON('["a"');
>> FALSE

SELECT IS_JSON(NULL);
>> null

CREATE TABLE TEST (ID INT, J VARCHAR);
> ok

INSERT INTO TEST VALUES (1, '{"info":{"type":1,"address":{"town":"Bristol","country":"England"},"tags":["Sport","Water polo"]}}');
> update count: 1

-- JSON_VALUE extracts a scalar value

SELECT JSON_VALUE(NULL, NULL);
>> null

SELECT JSON_VALUE('{"town":"Bristol"}', '$.town');
>> Bristol

SELECT JSON_VALUE(J, '$') FROM TEST;
>> null

SELECT JSON_VALUE(J, '$.info.type') FROM TEST;
>> 1

SELECT JSON_VALUE(J, '$.info.address.town') FROM TEST;
>> Bristol

SELECT JSON_VALUE(J, '$.info.address') FROM TEST;
>> null

SELECT JSON_VALUE(J, '$.info.tags') FROM TEST;
>> null

SELECT JSON_VALUE(J, '$.info.type[0]') FROM TEST;
>> null

SELECT JSON_VALUE(J, '$.info.none') FROM TEST;
>> null

-- JSON_QUERY extracts an object or an array

SELECT JSON_QUERY(NULL, NULL);
>> null

SELECT JSON_QUERY('{"town":"Bristol"}', '$');
>> {"town":"Bristol"}

SELECT JSON_QUERY(J, '$') FROM TEST;
>> {"info":{"type":1,"address":{"town":"Bristol","country":"England"},"tags":["Sport","Water polo"]}}

SELECT JSON_QUERY(J, '$.info.type') FROM TEST;
>> null

SELECT JSON_QUERY(J, '$.info.address.town') FROM TEST;
>> null

SELECT JSON_QUERY(J, '$.info.address') FROM TEST;
>> {"town":"Bristol","country":"England"}

SELECT JSON_QUERY(J, '$.info.tags') FROM TEST;
>> ["Sport","Water polo"]

SELECT JSON_QUERY(J, '$.info.type[0]') FROM TEST;
>> null

SELECT JSON_QUERY(J, '$.info.none') FROM TEST;
>> null

-- JSON_MODIFY changes a value in a JSON string

SELECT JSON_MODIFY(NULL, NULL, NULL);
>> null

SELECT JSON_MODIFY('{"town":"Bristol"}', '$.town', 'London');
>> {"town":"London"}

SELECT JSON_MODIFY(J, '$.info.address.town', 'London') FROM TEST;
>> {"info":{"type":1,"address":{"town":"London","country":"England"},"tags":["Sport","Water polo"]}}

SELECT JSON_MODIFY(J, '$.info.type', 2) FROM TEST;
>> {"info":{"type":2,"address":{"town":"Bristol","country":"England"},"tags":["Sport","Water polo"]}}

SELECT JSON_MODIFY(J, '$.info.tags', '["Sport"]') FROM TEST;
>> {"info":{"type":1,"address":{"town":"Bristol","country":"England"},"tags":["Sport"]}}

SELECT JSON_MODIFY(J, '$.info', '{"name":"John"}') FROM TEST;
>> {"info":{"name":"John"}}

SELECT JSON_MODIFY(J, '$.none', '{"name":"John"}') FROM TEST;
>> {"info":{"type":1,"address":{"town":"Bristol","country":"England"},"tags":["Sport","Water polo"]},"none":{"name":"John"}}

SELECT JSON_MODIFY(J, '$.info.address', NULL) FROM TEST;
>> {"info":{"type":1,"tags":["Sport","Water polo"]}}

DROP TABLE TEST;
> ok
