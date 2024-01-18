from datetime import datetime
from datetime import timedelta


def query_create_tbl_collect_customer_trn(table_name: str) -> str:
    """create table for collects customer transaction for target

    Args:
        table_name (str): table name

    Returns:
        str: sql script
    """
    sql = f"""
    set role cvm_sbx;
    CREATE TABLE {table_name} (contact_id numeric, cheque_pk bytea, article_id int
    , summ int , date date , frmt varchar ,art_grp_lvl_0_name varchar(10485760)
    , art_grp_lvl_1_name varchar(10485760) ,art_grp_lvl_2_name varchar(10485760)
    -- , art_grp_lvl_3_name varchar(10485760) ,art_grp_lvl_4_name varchar(10485760)
    -- , art_grp_lvl_5_name varchar(10485760) ,art_grp_lvl_6_name varchar(10485760)
    --, art_grp_lvl_7_name varchar(10485760) 
    , art_grp_name varchar(10485760)
    , art_grp_2_lvl_0_name varchar(10485760) 
    , art_grp_2_lvl_1_name varchar(10485760)
    , art_grp_2_lvl_2_name varchar(10485760) 
    -- , art_grp_2_lvl_3_name varchar(10485760)
    , direction int) WITH (
        appendonly=true,
        blocksize=32768,
        compresstype=zstd,
        compresslevel=4,
        orientation=column)
    DISTRIBUTED BY (contact_id)

    --partition by range(date)
    ;
reset role;

    """
    return sql


def query_collect_customer_trn(
    table_name, begin_cus_dt, end_cus_dt, site_format, cat_level, cat_name,cus
):
    """
    collect_customer_trn collects customer transaction for target
    Args:
        parameters (Dict): dictionary with parameters

    Returns:

    """
    sql = f"""
    INSERT INTO {table_name}
    SELECT
            trn.contact_id
            , trn.cheque_pk
            , trn.article_id
            , trn.summ
            , date(trn.datetime) date
            , st.frmt
            , t2.art_grp_lvl_0_name
            , t2.art_grp_lvl_1_name
            , t2.art_grp_lvl_2_name
            --, t2.art_grp_lvl_3_name
            --, t2.art_grp_lvl_4_name
            --, t2.art_grp_lvl_5_name
            --, t2.art_grp_lvl_6_name
            --, t2.art_grp_lvl_7_name
            , t2.art_grp_name
            , t2.art_grp_2_lvl_0_name
            , t2.art_grp_2_lvl_1_name
            , t2.art_grp_2_lvl_2_name
            --, t2.art_grp_2_lvl_3_name
            , t2.direction
    FROM dm.fct_trnsrcproducts trn
    JOIN (SELECT contact_id FROM CVM_SBX.CVM_FILTERED_CUSTOMER GROUP BY contact_id) lst2
        ON lst2.contact_id = trn.contact_id
        AND trn.orgunit_id > 0
        AND trn.contact_id > 0
        AND trn.operation_type_id = 1
    JOIN dm.whs st
    ON st.orgunit_id=trn.orgunit_id
    --AND trn.datetime >= '{begin_cus_dt}'
    --AND trn.datetime < '{end_cus_dt}'
    AND trn.contact_id > 0
    JOIN dm.art_ext t2
    ON trn.article_id=t2.article_id
    AND t2.art_grp_lvl_{cat_level}_name IN {cat_name}
    WHERE 
    trn.datetime >= '{begin_cus_dt}'
    AND trn.datetime < '{end_cus_dt}'
    AND  MOD(trn.contact_id, 10) = {cus}
    AND  MOD(lst2.contact_id, 10) = {cus} 
    ;"""
    return sql


def query_collect_customer_trn1(
    table_name, begin_cus_dt, end_cus_dt, site_format,cus
):
    sql = f"""
    INSERT INTO {table_name}
    SELECT
            trn.contact_id
            , trn.cheque_pk
            , trn.article_id
            , trn.summ
            , date(trn.datetime) date
            , st.frmt
            , t2.art_grp_lvl_0_name
            , t2.art_grp_lvl_1_name
            , t2.art_grp_lvl_2_name
            --, t2.art_grp_lvl_3_name
            --, t2.art_grp_lvl_4_name
            --, t2.art_grp_lvl_5_name
            --, t2.art_grp_lvl_6_name
            --, t2.art_grp_lvl_7_name
            , t2.art_grp_name
            , t2.art_grp_2_lvl_0_name
            , t2.art_grp_2_lvl_1_name
            , t2.art_grp_2_lvl_2_name
            --, t2.art_grp_2_lvl_3_name
            , t2.direction
    FROM dm.fct_trnsrcproducts trn
    (SELECT contact_id FROM CVM_SBX.CVM_FILTERED_CUSTOMER GROUP BY contact_id) lst2
        ON lst2.contact_id = trn.contact_id
        AND trn.orgunit_id > 0
        AND trn.contact_id > 0
        AND trn.operation_type_id = 1
    JOIN dm.whs st
    ON st.orgunit_id=trn.orgunit_id
    --AND trn.datetime >= '{begin_cus_dt}'
    --AND trn.datetime < '{end_cus_dt}'
    AND trn.contact_id >= 1
    {site_format}
    JOIN dm.art_ext t2
    ON trn.article_id=t2.article_id
    WHERE trn.datetime >= '{begin_cus_dt}'
    AND trn.datetime < '{end_cus_dt}'
    AND  MOD(trn.contact_id, 100) = {cus}
    AND  MOD(lst2.contact_id, 100) = {cus} 
    
    """
    return sql


def query_create_tbl_collect_customers(table_name: str) -> str:
    """create table for collects customers

    Args:
        table_name (str): table name

    Returns:
        str: sql script
    """
    sql = f"""
    set role cvm_sbx;
    CREATE TABLE {table_name} (contact_id numeric, rto_cat int, 
    prd_cnt_cat int, trn_cnt_cat int, rto_brand int, prd_cnt_brand int, 
    trn_cnt_brand int)
    WITH (
        appendonly=true,
        blocksize=32768,
        compresstype=zstd,
        compresslevel=4,
        orientation=column)
        DISTRIBUTED BY (contact_id);
    reset role;
    """
    return sql


def query_collect_customers(
    table_name, brand_col, brand_name, custom_query, cat_level, cat_name
):
    sql = f"""INSERT INTO  {table_name}
    SELECT
            contact_id AS contact_id
            ,SUM(summ) AS rto_cat
            ,COUNT(article_id) AS prd_cnt_cat
            ,COUNT(DISTINCT cheque_pk) AS trn_cnt_cat
            ,SUM(CASE WHEN {brand_col} IN {brand_name} THEN summ ELSE 0 END) AS rto_brand
            ,COUNT(CASE WHEN {brand_col} IN {brand_name} THEN article_id ELSE NULL END) AS prd_cnt_brand
            ,COUNT(DISTINCT CASE WHEN {brand_col} IN {brand_name} THEN cheque_pk ELSE NULL END) AS trn_cnt_brand
            {custom_query}
    FROM CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET
    WHERE art_grp_lvl_{cat_level}_name IN {cat_name}
    GROUP BY contact_id
    --HAVING rto_cat>0
    """
    return sql


def query_collect_customers1(table_name, cat_level, cat_name):
    sql = f"""INSERT INTO  {table_name}
    SELECT
            contact_id AS contact_id
            ,SUM(summ) AS rto
            ,COUNT(article_id) AS prd_cnt
            ,COUNT(DISTINCT cheque_pk) AS trn_cnt
            ,SUM(CASE WHEN art_grp_lvl_{cat_level}_name='{cat_name}' THEN summ ELSE 0 END) AS rto_cat
            ,COUNT(CASE WHEN art_grp_lvl_{cat_level}_name='{cat_name}' THEN article_id ELSE NULL END) AS prd_cnt_cat
            ,COUNT(DISTINCT CASE WHEN art_grp_lvl_{cat_level}_name='{cat_name}' THEN cheque_pk ELSE NULL END) AS trn_cnt_cat
    FROM CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET
    GROUP BY contact_id
    --HAVING rto>0
    """
    return sql


def query_cust_data(calc_dt, cust_table):
    sql = f"""
        WITH REGION AS
        (select 
            contact_id
            ,region from
                (
                select *
                ,row_number() over (partition by contact_id order by cnt desc) as r_n
                    from (
                    select cust.contact_id
                    ,w.region
                    ,count(cheque_pk) as cnt
                    from dm.cheque c 
                    join {cust_table} cust
                    ON c.contact_id=cust.contact_id
                    join dm.whs w 
                    on c.orgunit_id=w.orgunit_id
                    and c.orgunit_id > 0
                    and c.operation_type_id = 1
                    and c.datetime >='{(datetime.strptime(str(calc_dt), '%Y-%m-%d') 
                           + timedelta(days=-60)).strftime('%Y-%m-%d')}'
                    group by cust.contact_id
                    ,region) tbl
                )tbl1
            where r_n=1
            )
        SELECT
            t2.contact_id
            ,gender_code
            ,r.region
            ,CAST((date('{calc_dt}') - date(birth_date))/365.5 AS INTEGER) AS cus_age
            ,date('{calc_dt}') - date(registration_date) AS enrollment_days
    FROM {cust_table} t1
    JOIN dm.contact t2
        ON t1.contact_id=t2.contact_id
    LEFT JOIN REGION r
        ON r.contact_id=t1.contact_id
    GROUP BY t2.contact_id
        ,gender_code
        ,r.region
        ,CAST((date('{calc_dt}') - date(birth_date))/365.5 AS INTEGER) 
        ,date('{calc_dt}') - date(registration_date)
        """
    return sql


def query_create_tbl_collect_transprod(table_name: str) -> str:
    """create table for transprod data

    Args:
        table_name (str): table name

    Returns:
        str: sql script
    """
    sql = f"""
    set role cvm_sbx;
    CREATE TABLE {table_name} (contact_id numeric, cheque_pk bytea
    , article_id int, summ int, date date
    , art_grp_lvl_0_name varchar(10485760) , art_grp_lvl_1_name varchar(10485760) 
    , art_grp_lvl_2_name varchar(10485760) 
    --, art_grp_lvl_3_name varchar(10485760) 
    -- , art_grp_lvl_4_name varchar(10485760) , art_grp_lvl_5_name varchar(10485760) 
    -- , art_grp_lvl_6_name varchar(10485760) , art_grp_lvl_7_name varchar(10485760) 
    , art_grp_name varchar(10485760) , art_grp_2_lvl_0_name varchar(10485760) 
    , art_grp_2_lvl_1_name varchar(10485760) , art_grp_2_lvl_2_name varchar(10485760) 
    --, art_grp_2_lvl_3_name varchar(10485760) 
    , direction int)
    WITH (
        appendonly=true,
        blocksize=32768,
        compresstype=zstd,
        compresslevel=4,
        orientation=column)
    DISTRIBUTED BY (contact_id)
    --partition by range(date)
    ;
    reset role;
    """
    return sql


def query_collect_transprod(
    table_name, cust_tbl, begin_dt, end_dt, cat20_name, cus
):
    sql = f"""
    INSERT INTO  {table_name} 
    SELECT
            trn.contact_id
            , trn.cheque_pk
            , trn.article_id
            , trn.summ
            , date(trn.datetime) date
            , t2.art_grp_lvl_0_name
            , t2.art_grp_lvl_1_name
            , t2.art_grp_lvl_2_name
            --, t2.art_grp_lvl_3_name
            --, t2.art_grp_lvl_4_name
            --, t2.art_grp_lvl_5_name
            --, t2.art_grp_lvl_6_name
            --, t2.art_grp_lvl_7_name
            , t2.art_grp_name
            , t2.art_grp_2_lvl_0_name
            , t2.art_grp_2_lvl_1_name
            , t2.art_grp_2_lvl_2_name
            --, t2.art_grp_2_lvl_3_name
            , t2.direction
    FROM dm.fct_trnsrcproducts trn
    JOIN {cust_tbl} cust
    ON cust.contact_id=trn.contact_id
    --AND trn.datetime >= '{begin_dt}'
    --AND trn.datetime < '{end_dt}'
    AND trn.contact_id > 0
    AND trn.operation_type_id = 1
    AND trn.orgunit_id > 0
    JOIN dm.art_ext t2
    ON trn.article_id=t2.article_id
    AND  t2.art_grp_lvl_0_name in {cat20_name}
    WHERE trn.datetime >= '{begin_dt}'
    AND trn.datetime < '{end_dt}'
    AND  MOD(trn.contact_id, 50) = {cus}
    AND  MOD(cust.contact_id, 50) = {cus} 
    ;"""
    return sql


def query_get_cat(level, cat_level, cat_name):
    sql = f"""
        SELECT DISTINCT art_grp_lvl_{level}_name
        FROM dm.art_ext dp
        WHERE
        art_grp_lvl_{cat_level}_name IN {cat_name}
    """
    return sql


def query_create_tbl_collect_trn(table_name: str) -> str:
    """create table for collect_trn

    Args:
        table_name (str): table name

    Returns:
        str: sql script
    """
    sql = f"""
    set role cvm_sbx;
    CREATE TABLE {table_name} (contact_id numeric, cheque_pk bytea, 
    summ int, date date, frmt varchar)
    WITH (
        appendonly=true,
        blocksize=32768,
        compresstype=zstd,
        compresslevel=4,
        orientation=column)
        DISTRIBUTED BY (contact_id)
    
    --partition by range(date)
    ;
    reset role;
    """
    return sql


def query_collect_trn(table_name, cust_tbl, begin_dt, end_dt, cus):
    sql = f"""
    INSERT INTO  {table_name} 
    SELECT
            fl.contact_id
            , fl.cheque_pk
            , fl.summ
            , date(fl.datetime) date
            , st.frmt
    FROM dm.cheque fl
    JOIN {cust_tbl} lst2
        ON lst2.contact_id = fl.contact_id
        --AND fl.datetime >= '{begin_dt}'
        --AND fl.datetime < '{end_dt}'
        AND fl.contact_id > 0
        AND fl.operation_type_id = 1
        AND fl.orgunit_id > 0
        --AND fl.TRN_TTPCLEANORGID = 'ER'
        --AND fl.TRN_TRSCLEANORGID = 'B'
    JOIN dm.whs st
    ON st.orgunit_id=fl.orgunit_id
    AND st.frmt IN ('МК','МД','БФ')
WHERE fl.datetime >= '{begin_dt}'
        AND fl.datetime < '{end_dt}'
        AND  MOD(fl.contact_id, 50) = {cus}
        AND  MOD(lst2.contact_id, 50) = {cus} 
    """
    return sql


def query_calc_feat(input_tbl, calc_dt, begin_dt, custom_query):
    sql = f"""
         SELECT
                contact_id
                ,(MAX(date)- MIN(date)) AS trn_period
                ,('{calc_dt}' - MAX(date)) AS days_since_last_trn
                ,MAX(DAYS_BTW) AS max_days_btw
                ,AVG(DAYS_BTW) AS avg_days_btw
                ,COUNT(date) AS trn_days
                ,SUM(DAILY_RTO) AS rto
                ,AVG(DAILY_RTO) AS avg_daily_bill
                ,MAX(DAILY_RTO) AS max_daily_bill
                ,MIN(DAILY_RTO) AS min_daily_bill
                ,SUM(TRN_CNT) AS trn_cnt
                ,SUM(PRD_CNT) AS prd_cnt
                ,AVG(PRD_CNT) AS avg_daily_prd_cnt
                ,MAX(PRD_CNT) AS max_daily_prd_cnt
                ,MIN(PRD_CNT) AS min_daily_prd_cnt
        FROM 
        (
            SELECT
                *
                ,date - prev_date AS DAYS_BTW
                FROM 
                    (
                        SELECT
                        contact_id
                        ,date
                        ,LAG(date,1) OVER (PARTITION BY contact_id ORDER BY date) AS prev_date
                        ,DAILY_RTO
                        ,PRD_CNT
                        ,TRN_CNT
                        FROM 
                                (
                                SELECT
                                    contact_id
                                    ,date
                                    ,SUM(summ) AS DAILY_RTO
                                    ,COUNT(article_id) AS PRD_CNT
                                    ,COUNT(DISTINCT cheque_pk) AS TRN_CNT
                                        FROM
                                        {input_tbl}
                                        WHERE date>='{begin_dt}'
                                        {custom_query}
                                        GROUP BY
                                        contact_id
                                        ,date
                                 ) trn
                    ) trn1
        ) trn2
         GROUP BY contact_id
    """
    return sql


def query_calc_feat_trn(input_tbl, calc_dt, begin_dt, custom_query):
    sql = f"""
        SELECT
                contact_id
                ,MAX(date) - MIN(date) AS trn_period
                ,date('{calc_dt}') - MAX(date) AS days_since_last_trn
                ,MAX(DAYS_BTW) AS max_days_btw
                ,AVG(DAYS_BTW) AS avg_days_btw
                ,COUNT(date) AS trn_days
                ,SUM(DAILY_RTO) AS rto
                ,AVG(DAILY_RTO) AS avg_daily_bill
                ,MAX(DAILY_RTO) AS max_daily_bill
                ,MIN(DAILY_RTO) AS min_daily_bill
                ,SUM(TRN_CNT) AS trn_cnt
        FROM 
        (
            SELECT
                    *
                    ,date - prev_date AS DAYS_BTW
                    FROM 
                    (
                            SELECT
                            contact_id
                            ,date
                            ,LAG(date,1) OVER (PARTITION BY contact_id ORDER BY date) AS prev_date
                            ,DAILY_RTO
                            ,TRN_CNT
                            FROM 
                            (
                                SELECT
                                contact_id
                                ,date
                                ,SUM(summ) AS DAILY_RTO
                                ,COUNT(DISTINCT cheque_pk) AS TRN_CNT
                                FROM
                                {input_tbl}
                                WHERE date>='{begin_dt}'
                                {custom_query}
                                GROUP BY
                                    contact_id
                                    ,date
                            ) trn
                    ) trn1
        ) trn2
        GROUP BY contact_id
    """
    return sql


def query_calc_feat_price(input_tbl, begin_dt, custom_query):
    sql = f"""
        SELECT
                contact_id
                ,COUNT(date) AS trn_days
                ,SUM(DAILY_RTO) AS rto
                ,SUM(PRD_CNT) AS prd_cnt
                FROM 
                                (
                                SELECT
                                    contact_id
                                    ,date
                                    ,SUM(summ) AS DAILY_RTO
                                    ,COUNT(article_id) AS PRD_CNT
                                        FROM
                                        {input_tbl}
                                         WHERE date>='{begin_dt}'
                                        {custom_query}
                                        GROUP BY
                                        contact_id
                                        ,date
                                 ) trn
         GROUP BY contact_id
    """
    return sql


def query_collect_cust_dna(cust_tbl):
    sql = f"""
    WITH TBL1 AS
    (SELECT
            cct.contact_id
            ,FORMAT
            ,DNA_CLUSTER
    FROM CVM_SBX.MT_CVM_BASETBL_CUSTDNA_GROUPED mcbc
    JOIN dm.contact cct
    ON cct.external_id=mcbc.ext_id
    JOIN {cust_tbl} cust
    ON cust.contact_id=cct.contact_id)
    SELECT
            contact_id
            ,MAX(CASE WHEN FORMAT='МД' THEN DNA_CLUSTER ELSE NULL END) AS DNA_CLUSTER_MD
            ,MAX(CASE WHEN FORMAT='МК' THEN DNA_CLUSTER ELSE NULL END) AS DNA_CLUSTER_MK
            ,MAX(CASE WHEN FORMAT='БФ' THEN DNA_CLUSTER ELSE NULL END) AS DNA_CLUSTER_BF
    FROM TBL1
    GROUP BY contact_id
    """
    return sql
