@set schema=STATISTICS
open schema :schema;


create or replace lua script Z_INTERNAL_LIBRARY() as
script_schema = exa.meta.script_schema
newest_version = "0.02"
table_res = {}

--
-- return_result
-- 
function return_result()
    for i=1,#table_res do
        output("output line: "..table.concat(table_res[i],", "))
    end
    return table_res, '"ACTION" VARCHAR(1024),"MESSAGE" VARCHAR(2048)'
end    

--
-- handle_error
-- 
-- section:       string to identify the caller
-- fail:          boolean - should we fail on error
-- res:           result - also fails if empty
-- save_to_result:  if fail is false - should error be saved to into result table
--
function handle_error(section, res, fail, save_to_result)
    local error_msg = ""
    if res.error_message ~= nil then
        error_msg = res.error_message
    elseif #res < 1 then
        error_msg = "No Result found"
    end


    if fail then
        pquery("ROLLBACK")
        error(section .. quote(error_msg))
    elseif save_to_result then
        table.insert(table_res,{section, quote(error_msg)})
    return false, nil
    end
end


function replace_global_parms(sql)
    local start = 1
    local parms = {}
    local max_repeat = 1000
    local repeat_cnt = 0
    repeat
        repeat_cnt = repeat_cnt + 1
        local s,e
        s, e = string.find(sql, "!.-!", start)
        -- none empty match
        if s ~= nil and (s+1) ~= e then
            output("replace_global_parms: Found parm at "..s.." to "..e)
            start = e+1
            local token = string.sub(sql,(s+1),(e-1))
            local rep_str = string.sub(sql,s,e)
            output("replace_global_parms: found token:"..token)

            -- new parameter
            if parms[rep_str] == nil then
                output("replace_global_parms: no value found for token '"..token.. "'")
                local parm  = get_global_param(token, false)
                -- we found the parameter
                if parm ~= nil then
                    output("replace_global_parms: token: '"..token.. "' has value of '"..parm.."'")
                    parms[rep_str] = parm
                end
            end
        end
        if repeat_cnt > max_repeat then
            output("replace_global_parms: we hit max 1000 repeats. This should not happen")
            break
        end
    until s == nil or e == nil
    for k, v in pairs(parms) do
        output("replace_global_parms: replace '"..k.."' with '"..v.."'")
        sql = string.gsub(sql, k, v)
    end
    output("replace_global_parms: SQL: '"..sql.."'")
    return sql
end

--
-- sql_execute
-- 
-- Parameters will be passed as table
-- section:         string to identify the caller
-- sql:             success handler of pquery
-- parameters:      success handler of pquery
-- fail:            fail execution on error
-- save_to_result:  if fail is false - should error be saved to into result table
-- no_parms:        don't look for global parms in sql - boolean
--
-- returns:
--     Query - success, result
--     Other - success, row_count
--
-- Description:
--        central function for execution of sqls
function sql_execute(parms)
    local parameters = parms.parameters or {}
    parameters["schema"] = script_schema
    local sql = parms["sql"]
    local no_parms = parms["no_parms"]
    
    -- don't replace global parms in query for global parms
    if not no_parms then
        output("sql_execute: don't look into paramters")
        sql = replace_global_parms(parms["sql"])
    end
    

    local section = parms.section
    local fail
    if parms.fail ~= nil then 
        fail = parms.fail
    else
        fail = true
    end
    local save_to_result 
    if parms.save_to_result ~= nil then 
        save_to_result = parms.save_to_result
    else
        save_to_result = true
    end

    output("sql_execute: called with section='"..section.."',sql='"..sql.."',fail='"..(fail and 'true' or 'false').."',save_to_result='"..(save_to_result and 'true' or 'false').."',no_parms='"..(no_parms and 'true' or 'false').."',sql_parms='"..table.concat(parameters,",").."'")
    local success, res = pquery(
        sql,
        parameters
    )

    if success then
        local value 
        if res.rows_affected ~= nil then
            value=res.rows_affected
        elseif res.etl_rows_written ~= nil then
            value=res.etl_rows_written
        elseif res.etl_rows_read ~= nil then
            value=res.etl_rows_read
        else 
            value=res
        end
        if save_to_result then
            if type(value) == "table" then
                table.insert(table_res,{section, #value})
            else
                table.insert(table_res,{section, value})
            end
        end
        return success,value
    else
        handle_error(section, res, fail, save_to_result)
    end


end

--
-- retrieve global parameter by name
-- 
function get_global_param(param_name, fail)
    local section = "Get Global Parameter "..param_name
    output("get_global_param: called with parameter='"..param_name.."',fail='"..(fail and 'true' or 'false').."'")
    local success, res = sql_execute({
            section=section , 
            sql=[[SELECT PARAM_VALUE FROM ::schema.GLOBAL_PARMS WHERE PARAM_NAME = :param_name]],
            parameters={param_name=param_name},
            fail=fail,
            save_to_result=false
        }
    )
    if res ~= nil and  #res > 0 then
        output("get_global_param: returning '"..res[1].PARAM_VALUE.."'")
        return res[1].PARAM_VALUE
    else 
        return nil
    end
end

--
-- get_version
-- 
function get_version()
    res = get_global_param("VERSION", false)
    if res ~= nil then
        output("get_version returned: "..res)
        return res
    else
        output("get_version returned: 0.00")
        return "0.00"
    end
end
       

--
-- newest_version
-- 
-- Do we have the newest version active?
--
function got_newest_version()
    local version = get_version()

    output("newest_version: installed_version='"..version..",newest_version='"..newest_version.."'")

    if version == newest_version then
        return true
    else
        return false
    end
end

function verify_version()
    if not got_newest_version() then
        error("You don't have the newest version active. Please run EXECUTE SCRIPT INSTALL_UPGRADE()")
    end
end

;

create or replace lua script INSTALL_UPGRADE() returns table as
import(exa.meta.script_schema..".Z_INTERNAL_LIBRARY", "z_lib")

install = {
    -- version, sql, fail on error
    {"0.01","Create Parameters Table","CREATE TABLE ::schema.GLOBAL_PARMS (PARAM_NAME VARCHAR(100), PARAM_VALUE VARCHAR(100000))", true},
    {"0.01","Insert Version Info","INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE) VALUES ('VERSION','0.01')", true},
    {"0.01","Set Scan Row threshold","INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE) VALUES ('SCANS_ROW_MIN','20000000')", true},
    {"0.01","Set Scan Exclude Schemas","INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE) VALUES ('SCANS_EXCLUDE_SCHEMAS','(''SYS'',''EXA_STATISTICS'')')", true},
    {"0.01","Set SQL for Scan Detection", [[INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME , PARAM_VALUE)
                 VALUES ('SCANS_SQL',
                'INSERT INTO ::schema.EXA_AUDIT_SCANS (SESSION_ID, STMT_ID, part_id, PART_NAME, OBJECT_SCHEMA, OBJECT_NAME, OBJECT_ROWS, OUT_ROWS, ANZAHL)
                    SELECT
                        session_id,
                        stmt_id,
                        part_id,
                        part_name,
                        OBJECT_schema,
                        object_name,
                        OBJECT_ROWS,
                        out_rows,
                        1 as anzahl
                    FROM
                        "$EXA_PROFILE_LAST_DAY" a
                    WHERE
                        OBJECT_ROWS <= OUT_ROWS
                        AND object_name NOT LIKE ''$%'' and object_schema not in !SCANS_EXCLUDE_SCHEMAS!
                        AND PART_NAME NOT IN (''INDEX CREATE'')
                        AND part_info NOT IN (''on TEMPORARY table'')
                        AND object_name not like ''tmp_%''
                        AND OBJECT_ROWS > !SCANS_ROW_MIN!
                        AND not exists (SELECT true from ::schema.EXA_AUDIT_SCANS b where a.session_id = b.session_id and a.stmt_id = b.stmt_id and a.part_id = b.part_id)
                '
                )
            ]], true},
            {"0.01","Set SQL for Scan Cleanup", [[INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE)
            VALUES ('SCANS_CLEANUP',
           'DELETE FROM ::schema.EXA_AUDIT_SCANS A
           WHERE NOT EXISTS    (              
                                   SELECT 1 FROM "$EXA_STATS_AUDIT_SQL" B
                                   WHERE A.SESSION_ID = B.SESSION_ID
                                     AND A.STMT_ID    = B.STMT_ID
                               )'
           )
       ]], true},
    {"0.01","Set SQL for Index Usage",[[INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE )
                VALUES ('INDEX_USAGE_SQL',
                'INSERT INTO ::schema.EXA_AUDIT_INDEX_USAGE
                (SESSION_ID,STMT_ID,OBJECT_SCHEMA,OBJECT_NAME,INDEX_TYPE,REMARKS)
                    SELECT session_id
                        , stmt_id
                        , object_schema AS index_schema
                        , object_name AS index_table
                        , REGEXP_REPLACE(remarks, ''^.* => ((LOCAL|GLOBAL) INDEX.*)$'', ''\2'') AS index_type
                        , REGEXP_REPLACE(remarks, ''^.* => ((LOCAL|GLOBAL) INDEX.*)$'', ''\1'') AS remarks
                    FROM "$EXA_PROFILE_LAST_DAY" a
                    WHERE object_schema IS NOT NULL
                        AND object_name IS NOT NULL
                        AND remarks REGEXP_LIKE ''^.* => ((LOCAL|GLOBAL) INDEX.*)$''
                        AND NOT EXISTS (
                                            SELECT 1 
                                            FROM ::schema.EXA_AUDIT_INDEX_USAGE b
                                            WHERE A.SESSION_ID = B.SESSION_ID 
                                            AND A.STMT_ID    = B.STMT_ID
                                        )
                    GROUP BY 1,2,3,4,5,6'
            )]], true},
    {"0.01","set SQL for Index Usage Cleanup",[[INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE )
            VALUES ('INDEX_USAGE_CLEANUP',
            'DELETE FROM ::schema.EXA_AUDIT_INDEX_USAGE A
            WHERE NOT EXISTS    (              
                                    SELECT 1 FROM "$EXA_STATS_AUDIT_SQL" B
                                    WHERE A.SESSION_ID = B.SESSION_ID
                                        AND A.STMT_ID    = B.STMT_ID
                                )'
        )]], true},
    {"0.01","Create Table for Index Usage", [[CREATE TABLE ::schema."EXA_AUDIT_INDEX_USAGE" (
                "SESSION_ID" DECIMAL(20,0) NOT NULL ,
                "STMT_ID" DECIMAL(12,0) NOT NULL ,
                "OBJECT_SCHEMA" VARCHAR(128) UTF8 NOT NULL ,
                "OBJECT_NAME" VARCHAR(128) UTF8 NOT NULL ,
                "INDEX_TYPE" VARCHAR(20) UTF8 NOT NULL ,
                "REMARKS" VARCHAR(2000000) UTF8 NOT NULL 
            )]], true},
    {"0.01","Set SQL for statement statistics",[[
        INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE) 
        VALUES ('STMT_STATISTICS_SQL', 
        '
        MERGE INTO ::schema.EXA_DBA_STATEMENT_STATISTICS a
                using 
                (SELECT
                	session_id,
                	stmt_id,
                	query_start,
                	query_stop,
                	SUM(CASE WHEN part_name = ''WAIT FOR COMMIT'' THEN duration ELSE 0 END) AS LOCK_TIME,
                	SUM(CASE WHEN part_name <> ''WAIT FOR COMMIT'' THEN duration ELSE 0 END) AS EXEC_TIME,
                	SUM(CASE WHEN part_name = ''CONSTRAINT CHECK'' THEN duration ELSE 0 END) AS CONSTRAINT_CHECK_TIME,
                	SUM(CASE WHEN part_name LIKE ''INDEX%'' THEN duration ELSE 0 END) AS INDEX_TIME
                FROM
                	"$EXA_PROFILE_LAST_DAY"
                WHERE
                	query_stop < ADD_MINUTES(CURRENT_TIMESTAMP,-10)
                GROUP BY
                	session_id,
                	stmt_id ,
                	query_start,
                	query_stop
                ) b
                on a.SESSION_ID = b.session_id and a.STMT_ID = b.stmt_id
                when not matched then 
                  insert(SESSION_ID, STMT_ID, QUERY_START, QUERY_STOP, LOCK_TIME, EXEC_TIME, CONSTRAINT_CHECK_TIME, INDEX_TIME) values (SESSION_ID, STMT_ID, QUERY_START, QUERY_STOP, LOCK_TIME, EXEC_TIME, CONSTRAINT_CHECK_TIME, INDEX_TIME)
        '
        )
    ]], true},
    {"0.01","Set SQL for statement statistics cleanup",[[
        INSERT INTO ::schema.GLOBAL_PARMS (PARAM_NAME, PARAM_VALUE) 
        VALUES ('STMT_STATISTICS_CLEANUP', 
        'DELETE
        FROM
            ::schema.EXA_DBA_STATEMENT_STATISTICS a
        WHERE
            NOT EXISTS (
            SELECT
                1
            FROM
                EXA_STATISTICS.EXA_DBA_AUDIT_SESSIONS b
            WHERE
                a.session_id = b.session_id)'
        )
        ]], true},
    {"0.01","Create table for index drop info",[[CREATE TABLE ::schema."EXA_AUDIT_INDEX_DROPS" (
                    "TABLE_SCHEMA" VARCHAR(128) UTF8 ,
                    "TABLE_NAME" VARCHAR(128) UTF8 ,
                    "INDEX_TYPE" VARCHAR(20) UTF8 ,
                    "INDEX_DEFINITION" VARCHAR(100000) UTF8 ,
                    "SIZE_IN_GB" DOUBLE ,
                    "LAST_ACCESS" TIMESTAMP ,
                    "DROP_STATEMENT" VARCHAR(400297) UTF8 ,
                    "DROP_DATE" DATE 
            )]], true},
    {"0.01","create table for scan info",[[CREATE TABLE ::schema."EXA_AUDIT_SCANS" (
                    "SESSION_ID" DECIMAL(20,0) ,
                    "STMT_ID" DECIMAL(12,0) ,
                    "PART_ID" DECIMAL(9,0) ,
                    "PART_NAME" VARCHAR(40) UTF8 ,
                    "OBJECT_SCHEMA" VARCHAR(128) UTF8 ,
                    "OBJECT_NAME" VARCHAR(128) UTF8 ,
                    "OBJECT_ROWS" DECIMAL(18,0) ,
                    "OUT_ROWS" DECIMAL(18,0) ,
                    "ANZAHL" DECIMAL(1,0) 
            )]], true},
    {"0.01","create table for statement statistics",[[CREATE TABLE ::schema."EXA_DBA_STATEMENT_STATISTICS" (
                    "SESSION_ID" DECIMAL(20,0) ,
                    "STMT_ID" DECIMAL(12,0) ,
                    "QUERY_START" TIMESTAMP ,
                    "QUERY_STOP" TIMESTAMP ,
                    "LOCK_TIME" DECIMAL(29,6) ,
                    "EXEC_TIME" DECIMAL(29,6) ,
                    "CONSTRAINT_CHECK_TIME" DECIMAL(29,6) ,
                    "INDEX_TIME" DECIMAL(29,6) ,
                    DISTRIBUTE BY SESSION_ID,STMT_ID
            )]], true},
    {"0.02","Create Session Stats View",[[CREATE OR REPLACE
                    VIEW ::schema.AUDIT_SESSION AS
                    SELECT
                        s.SESSION_ID,
                        s.user_name,
                        s.client,
                        s.host,
                        s.os_user,
                        s.LOGIN_TIME,
                        s.LOGOUT_TIME,
                        TO_DATE( TRUNC( s.LOGIN_TIME, 'DD' )) AS session_date,
                        SUM(q.ROW_COUNT) AS ROWS_PROCESSED,
                        SUM(SECONDS_BETWEEN(s.logout_time, s.login_time)) AS DURATION_S,
                        SUM(SECONDS_BETWEEN(q.stop_time, q.start_time)) AS SQL_DURATION_S,
                        local.DURATION_S - local.SQL_DURATION_S AS APP_DURATION_S,
                        ZEROIFNULL(MAX(BLOCK_DURATION)) AS BLOCK_TIME_S,
                        ZEROIFNULL(MAX(c.LOCK_DURATION)) AS LOCK_WAIT_TIME_S,
                        LOCAL.SQL_DURATION_S - LOCAL.LOCK_WAIT_TIME_S AS EXECUTION_TIME_S,
                        SUM( CASE WHEN i.COMMAND_CLASS = 'DQL' THEN 1 ELSE 0 END ) AS COUNT_DQL,
                        SUM( CASE WHEN i.COMMAND_CLASS = 'DDL' THEN 1 ELSE 0 END ) AS COUNT_DDL,
                        SUM( CASE WHEN i.COMMAND_CLASS = 'DML' THEN 1 ELSE 0 END ) AS COUNT_DML,
                        SUM( CASE WHEN i.COMMAND_CLASS = 'TRANSACTION' THEN 1 ELSE 0 END ) AS "COUNT_TRANS",
                        SUM( CASE WHEN q.ERROR_CODE IS NOT NULL THEN 1 ELSE 0 END ) AS "COUNT_ERRORS"
                    FROM
                        EXA_STATISTICS.EXA_DBA_AUDIT_SESSIONS s
                    INNER JOIN "$EXA_STATS_AUDIT_SQL" q ON
                        s.session_id = q.session_id
                    LEFT OUTER JOIN EXA_COMMAND_IDS i ON
                        i.command_id = q.command_id
                    LEFT OUTER JOIN (
                        SELECT
                            SESSION_ID,
                            SUM(SECONDS_BETWEEN(STOP_TIME, START_TIME)) AS LOCK_DURATION
                        FROM
                            EXA_STATISTICS.EXA_DBA_TRANSACTION_CONFLICTS
                        GROUP BY
                            SESSION_ID ) c ON
                        s.SESSION_ID = c.session_id
                    LEFT OUTER JOIN (
                        SELECT
                            CONFLICT_SESSION_ID,
                            SUM(SECONDS_BETWEEN(STOP_TIME, START_TIME)) AS BLOCK_DURATION
                        FROM
                            EXA_STATISTICS.EXA_DBA_TRANSACTION_CONFLICTS x
                        GROUP BY
                            CONFLICT_SESSION_ID ) b ON
                        s.SESSION_ID = b.CONFLICT_SESSION_ID
                    GROUP BY
                        s.SESSION_ID,
                        s.user_name,
                        s.client,
                        s.host,
                        s.os_user,
                        s.LOGIN_TIME,
                        s.LOGOUT_TIME,
                        TO_DATE( TRUNC( s.LOGIN_TIME, 'DD' ))
            ]],true},
    {"0.02","Create Active Sessions Views",[[CREATE OR REPLACE
                VIEW ::schema.ACTIVE_SESSIONS AS WITH sess AS (
                SELECT
                    SESSION_ID,
                    USER_NAME,
                    status,
                    command_name,
                    stmt_id,
                    POSITION(':' IN DURATION ) AS pos_hours,
                    POSITION(':' IN SUBSTRING(duration, LOCAL.pos_hours + 1)) AS pos_minutes,
                    POSITION(':' IN SUBSTRING(duration, LOCAL.pos_minutes + 1)) AS pos_seconds,
                    SUBSTRING(duration, 0, LOCAL.pos_hours-1) AS "hours",
                    SUBSTRING(duration, LOCAL.pos_hours + 1, LOCAL.pos_minutes-1) AS "minutes",
                    SUBSTRING(duration, LOCAL.pos_hours + LOCAL.pos_minutes + 1, LOCAL.pos_seconds) AS "seconds",
                    duration AS duration_time,
                    LOCAL."hours" * 3600 + LOCAL."minutes" * 60 + LOCAL."seconds" AS duration,
                    ACTIVITY,
                    CASE
                        WHEN ACTIVITY LIKE 'Waiting for %' THEN CAST( REPLACE( ACTIVITY,
                        'Waiting for session ',
                        '' ) AS DECIMAL( 20,
                        0 ) )
                        ELSE NULL
                    END AS WAIT_SESSION,
                    TEMP_DB_RAM,
                    client,
                    os_user,
                    RESOURCES,
                    SQL_TEXT
                FROM
                    SYS.EXA_DBA_SESSIONS
                WHERE
                    STATUS <> 'IDLE'
                    AND SESSION_ID <> CURRENT_SESSION )
                SELECT
                    CASE
                        WHEN 'Blocks: ' || GROUP_CONCAT(bl.session_id) = 'Blocks: ' THEN NULL
                        ELSE 'Blocks: ' || GROUP_CONCAT(bl.session_id)
                    END AS session_blocks,
                    s.SESSION_ID,
                    s.USER_NAME,
                    s.status,
                    s.command_name,
                    s.stmt_id,
                    s.duration_time,
                    s.duration,
                    SUM(DISTINCT pr.DURATION) AS lock_Wait_time,
                    s.ACTIVITY,
                    s.wait_session,
                    s.TEMP_DB_RAM,
                    s.client,
                    s.os_user,
                    s.RESOURCES,
                    s.SQL_TEXT
                FROM
                    SESS AS S
                LEFT OUTER JOIN SESS AS bl ON
                    s.session_id = bl.wait_session
                LEFT OUTER JOIN EXA_STATISTICS.EXA_DBA_PROFILE_RUNNING pr ON
                    pr.SESSION_ID = s.session_id
                    AND pr.PART_NAME = 'WAIT FOR COMMIT'
                    AND s.stmt_id = pr.stmt_id
                GROUP BY
                    s.SESSION_ID,
                    s.USER_NAME,
                    s.status,
                    s.command_name,
                    s.stmt_id,
                    s.duration_time,
                    s.duration,
                    s.ACTIVITY,
                    s.wait_session,
                    s.TEMP_DB_RAM,
                    s.client,
                    s.os_user,
                    s.RESOURCES,
                    s.SQL_TEXT
                ORDER BY
                    s.duration_Time DESC
        ]],true},
    {"0.02","Create Audit SQL View",[[CREATE OR REPLACE
                VIEW ::schema.AUDIT_SQL AS
                SELECT
                    q.SESSION_ID,
                    s.user_name,
                    s.client,
                    s.host,
                    s.os_user,
                    q.COMMAND_NAME,
                    q.COMMAND_CLASS,
                    q.CPU,
                    q.TEMP_DB_RAM_PEAK,
                    q.HDD_READ,
                    q.HDD_WRITE,
                    q.NET,
                    q.SUCCESS,
                    q.ERROR_CODE,
                    q.ERROR_TEXT,
                    q.SCOPE_SCHEMA,
                    q.ROW_COUNT,
                    q.SQL_TEXT,
                    q.STMT_ID,
                    q.START_TIME,
                    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE( UPPER(SQL_TEXT), '.+?', '?' ), '(?:,|=\s*)\d+', '?' ), '(?m)\r\n', ' ' ), '(?m)\n', ' '  ), '\t', ' '), '((?<=)||[a-zA-Z])\d+[_,\s;]', '\1'), '\d+(\s+AS\s+\w+\s*)', '?\1') AS PARSED_SQL_TEXT,
                    TO_DATE( TRUNC( q.START_TIME, 'DD' )) AS STMT_DATE,
                    q.DURATION AS duration_s,
                    HASH_SHA1(LOCAL.PARSED_SQL_TEXT) AS SQL_HASH,
                    l.lock_time AS LOCK_WAIT_TIME_S,
                    l.EXEC_TIME AS EXECUTION_TIME_S,
                    l.CONSTRAINT_CHECK_TIME AS CONSTRAINT_CHECK_TIME,
                    l.index_time,
                    CASE
                        WHEN COMMAND_NAME IN( 'MERGE',
                        'INSERT',
                        'UPDATE' ) THEN UPPER( REPLACE( REGEXP_REPLACE( LOCAL.PARSED_SQL_TEXT, '(?i)(?s).(?:INSERT\s+INTO|UPDATE)(?:\t|\s*)(.+?\..+?)(?:\s+|\().+', '\1' ), ' ', '' ))
                        WHEN COMMAND_NAME IN('DELETE') THEN UPPER( REPLACE( REGEXP_REPLACE( LOCAL.PARSED_SQL_TEXT, '(?i)(?s).DELETE(?:\t|\s*)FROM(?:\t*|\s*)(.+?\..+?)(?:\s+|\().+', '\1' ), ' ', '' ))
                        ELSE 'Not a DML Command: ' || command_name
                    END AS MOD_AFFECTED_TABLE
                FROM
                    EXA_STATISTICS.EXA_DBA_AUDIT_SQL q
                INNER JOIN EXA_STATISTICS.EXA_DBA_AUDIT_SESSIONS s ON
                    s.session_id = q.session_id
                INNER JOIN STATISTICS.EXA_DBA_STATEMENT_STATISTICS l ON
                    q.SESSION_ID = l.SESSION_ID
                    AND q.STMT_ID = l.STMT_ID
        ]],true},
    {"0.02","Create Index Daily Statistics",[[CREATE OR REPLACE
                VIEW STATISTICS.INDEX_DAILY_STATISTICS AS
                SELECT
                    edi.INDEX_SCHEMA AS TABLE_SCHEMA,
                    edi.INDEX_TABLE AS TABLE_NAME,
                    edi.INDEX_TYPE,
                    edi.REMARKS AS INDEX_DEFINITION,
                    TO_DATE(TRUNC(edas.START_TIME, 'DD')) AS "ACCESS_DATE",
                    ROUND(MAX(edi.MEM_OBJECT_SIZE) /(1024 * 1024 * 1024.0), 2) AS SIZE_IN_GB,
                    COALESCE(MAX(edas.start_time),
                    TO_TIMESTAMP('1900-01-01')) AS LAST_ACCESS,
                    SUM(CASE WHEN edas.start_time IS null THEN 0 ELSE 1 end) AS ACCESS_COUNT,
                    'drop ' || edi.index_type || ' index on "' || edi.INDEX_SCHEMA || '"."' || edi.INDEX_TABLE || '"' || REPLACE(REGEXP_REPLACE(REPLACE(edi.REMARKS, edi.index_type), '([^,\)\(]+)', '"\1"'),
                    '" INDEX "',
                    '')|| ';' AS drop_statement
                FROM
                    sys.EXA_DBA_INDICES edi
                LEFT OUTER JOIN STATISTICS.EXA_AUDIT_INDEX_USAGE aiu ON
                    edi.INDEX_SCHEMA = aiu.OBJECT_SCHEMA
                    AND edi.INDEX_TABLE = AIU.OBJECT_NAME
                    AND edi.INDEX_TYPE = aiu.INDEX_TYPE
                    AND edi.REMARKS = aiu.REMARKS
                LEFT OUTER JOIN "$EXA_STATS_AUDIT_SQL" edas ON
                    aiu.SESSION_ID = edas.SESSION_ID
                    AND aiu.STMT_ID = edas.STMT_ID
                GROUP BY
                    edi.INDEX_SCHEMA,
                    edi.INDEX_TABLE,
                    edi.INDEX_TYPE,
                    local.access_date,
                    edi.REMARKS
        ]],true},
    {"0.02","Create Index Statistics",[[CREATE OR REPLACE
                    VIEW STATISTICS.INDEX_STATISTICS AS
                    SELECT
                        edi.INDEX_SCHEMA AS TABLE_SCHEMA,
                        edi.INDEX_TABLE AS TABLE_NAME,
                        edi.INDEX_TYPE,
                        edi.REMARKS AS INDEX_DEFINITION,
                        ROUND(MAX(edi.MEM_OBJECT_SIZE) /(1024 * 1024 * 1024.0), 2) AS SIZE_IN_GB,
                        COALESCE(MAX(edas.start_time),
                        TO_TIMESTAMP('1900-01-01')) AS LAST_ACCESS,
                        SUM(CASE WHEN edas.start_time IS null THEN 0 ELSE 1 end) AS ACCESS_COUNT,
                        'drop ' || edi.index_type || ' index on "' || edi.INDEX_SCHEMA || '"."' || edi.INDEX_TABLE || '"' || REPLACE(REGEXP_REPLACE(REPLACE(edi.REMARKS, edi.index_type), '([^,\)\(]+)', '"\1"'),
                        '" INDEX "',
                        '')|| ';' AS drop_statement
                    FROM
                        sys.EXA_DBA_INDICES edi
                    LEFT OUTER JOIN STATISTICS.EXA_AUDIT_INDEX_USAGE aiu ON
                        edi.INDEX_SCHEMA = aiu.OBJECT_SCHEMA
                        AND edi.INDEX_TABLE = AIU.OBJECT_NAME
                        AND edi.INDEX_TYPE = aiu.INDEX_TYPE
                        AND edi.REMARKS = aiu.REMARKS
                    LEFT OUTER JOIN "$EXA_STATS_AUDIT_SQL" edas ON
                        aiu.SESSION_ID = edas.SESSION_ID
                        AND aiu.STMT_ID = edas.STMT_ID
                    GROUP BY
                        edi.INDEX_SCHEMA,
                        edi.INDEX_TABLE,
                        edi.INDEX_TYPE,
                        edi.REMARKS
        ]],true},
    {"0.02","Create Transaction Conflict View",[[CREATE OR REPLACE
                VIEW ::schema.TRANSACTION_CONFLICT AS
                SELECT
                    c.SESSION_ID,
                    v.CLIENT AS victim_client,
                    v.USER_NAME AS victim_user_name,
                    v.OS_USER AS victim_os_user,
                    c.CONFLICT_SESSION_ID,
                    h.client AS holder_client,
                    h.USER_NAME AS holder_user_name,
                    h.OS_USER AS holder_os_user,
                    c.START_TIME,
                    c.STOP_TIME,
                    SECONDS_BETWEEN(STOP_TIME, START_TIME) AS DURATION,
                    CONFLICT_INFO,
                    CONFLICT_OBJECTS,
                    CONFLICT_TYPE
                FROM
                    EXA_STATISTICS.EXA_DBA_TRANSACTION_CONFLICTS c
                LEFT OUTER JOIN EXA_STATISTICS.EXA_DBA_AUDIT_SESSIONS v ON
                    c.SESSION_ID = v.SESSION_ID
                LEFT OUTER JOIN EXA_STATISTICS.EXA_DBA_AUDIT_SESSIONS h ON
                    c.CONFLICT_SESSION_ID = h.SESSION_ID]],true},
    {"0.02","Update Version Info","UPDATE ::schema.GLOBAL_PARMS SET PARAM_VALUE = '0.02' WHERE PARAM_NAME = 'VERSION'", true},
    {"0.03", "Add Primary key to parms","ALTER TABLE ::schema.GLOBAL_PARMS ADD CONSTRAINT PK_GLOBAL_PARMS PRIMARY KEY (PARAM_NAME) ENABLE", true},
    {"0.03","Update Version Info","UPDATE ::schema.GLOBAL_PARMS SET PARAM_VALUE = '0.02' WHERE PARAM_NAME = 'VERSION'", true}

}            
local installed_version = z_lib.get_version()
output("Installed version:"..installed_version)
for i=1, #install do
    -- execute if version is lower
    if install[i][1] > installed_version then
        z_lib.sql_execute({
            section="Update to version ".. install[i][1] .. ": " .. install[i][2], 
            sql=install[i][3], 
            fail=install[i][4],
            no_parms=true
        })
    end
end
return z_lib.return_result()
;


create or replace lua script gather_statistics() returns table as
import(exa.meta.script_schema..".Z_INTERNAL_LIBRARY", "z_lib")
-- check version
z_lib.verify_version()

-- gather statistics
stats = {"STMT_STATISTICS","SCANS", "INDEX_USAGE" }

for i=1,#stats do
    local sql = z_lib.get_global_param(stats[i]..'_SQL', true)
    local cleanup = z_lib.get_global_param(stats[i]..'_CLEANUP', true)
    z_lib.sql_execute({
        section="Gather statistics: "..stats[i],
        fail=true,
        save_to_result=true,
        sql=sql
    })
    z_lib.sql_execute({
        section="Cleanup statistics: "..stats[i],
        fail=true,
        save_to_result=true,
        sql=cleanup
    })
end
return z_lib.return_result()
;
