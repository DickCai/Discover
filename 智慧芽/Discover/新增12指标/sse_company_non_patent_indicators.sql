-- fundings - round average value fill NA
CREATE TABLE finance_data.np_metric_fundings_modified
    WITH (external_location = %(loc)s)
AS

with np_metric_fundings_round_avg_value as (
    select * from
    (
        select round, avg(money_formatted) as round_avg_value
        from
        (
            select clean_name as company_name,
            replace(replace(replace(replace(replace(
                replace(replace(replace(round, '++', '+'), '天使+轮', '天使轮'), '交叉轮', 'Pre-IPO'), 'Pre-A+轮', 'Pre-A轮'), '借壳上市', 'IPO'), 
                '债权融资', ''), '官方披露', ''), '私有化', '') as round,
            cast(money_formatted as BIGINT) as money_formatted from npd_st.fundings_modified2
            where ((strpos(round, 'IPO') = 0) OR (strpos(round, 'Pre-IPO') != 0)) AND (strpos(round, '定向增发') = 0)
        ) a
        where a.money_formatted != -1
        group by round
    ) a
    where round != ''
)

select company_name, a.ans_id, a.round, if(money_formatted = -1, round_avg_value, money_formatted) as money_formatted, pubtime from
(
    select b.company_name, a.ans_id, b.pubtime, b.round, b.money_formatted from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (
        select clean_name as company_name, pubtime,
        replace(replace(replace(replace(replace(
            replace(replace(replace(round, '++', '+'), '天使+轮', '天使轮'), '交叉轮', 'Pre-IPO'), 'Pre-A+轮', 'Pre-A轮'), '借壳上市', 'IPO'), 
            '债权融资', ''), '官方披露', ''), '私有化', '') as round,
        cast(money_formatted as BIGINT) as money_formatted from npd_st.fundings_modified2
        where ((strpos(round, 'IPO') = 0) OR (strpos(round, 'Pre-IPO') != 0)) AND (strpos(round, '定向增发') = 0)
    ) b
    on a.clean_name = b.company_name
) a left join np_metric_fundings_round_avg_value b on a.round = b.round;



-- fundings - money_sum, money_max, rounds
CREATE TABLE finance_data.np_metric_fundings_stats
    WITH (external_location = %(loc)s)
AS 
select ans_id, 
if(max(money_formatted) = -1, 0, max(money_formatted)) as money_max,
if(sum(money_formatted) < 0, 0, sum(money_formatted)) as money_sum,
count(round) as rounds
from
(
    select b.company_name, a.ans_id, b.round, b.money_formatted from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (select company_name, round, cast(money_formatted as BIGINT) as money_formatted from finance_data.np_metric_fundings_modified) b
    on a.clean_name = b.company_name
) a
group by ans_id;



-- fundings - most_recent_time, most_recent_money_formatted
CREATE TABLE finance_data.np_metric_fundings_mostRecent
    WITH (external_location = %(loc)s)
AS 
select a.ans_id, max(money_formatted) as most_recent_money_formatted,
date_diff('day', from_unixtime(max(pubtime) / 1000), now()) / 365.0 as most_recent_time from
(
    select b.company_name, a.ans_id, b.pubtime, b.round, b.money_formatted from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (
        select company_name, cast(pubtime as BIGINT) as pubtime, round, cast(money_formatted as BIGINT) as money_formatted
        from finance_data.np_metric_fundings_modified
    ) b
    on a.clean_name = b.company_name
) a
inner join
(
    select ans_id, max(pubtime) as most_recent_time from
    (
        select b.company_name, a.ans_id, b.pubtime, b.round, b.money_formatted from
        (select distinct clean_name, ans_id from npd_st.names_ansid) a
        inner join
        (
            select company_name, cast(pubtime as BIGINT) as pubtime, round, cast(money_formatted as BIGINT) as money_formatted
            from finance_data.np_metric_fundings_modified
        ) b
        on a.clean_name = b.company_name
    ) a
    group by ans_id
) b
on a.ans_id = b.ans_id and a.pubtime = b.most_recent_time
group by a.ans_id;



-- fundings - avg_funding_interval
CREATE TABLE finance_data.np_metric_fundings_avgFundingInterval
    WITH (external_location = %(loc)s)
AS 
select ans_id, avg(funding_interval) as avg_funding_interval from
(
    select *, prev_interv - interv as funding_interval from
    (
        select a.ans_id, from_unixtime(pubtime / 1000) as pubtime, interv, lag(interv, 1) over (partition by company_name order by interv desc) as prev_interv from
        (
            select b.company_name, a.ans_id, b.interv, b.pubtime from
            (select distinct clean_name, ans_id from npd_st.names_ansid) a
            inner join
            (
                select company_name, date_diff('day', from_unixtime(cast(pubtime AS BIGINT) / 1000), now()) / 365.0 as interv, cast(pubtime AS BIGINT) as pubtime 
                from finance_data.np_metric_fundings_modified
            ) b
            on a.clean_name = b.company_name
        ) a
        order by ans_id, pubtime
    )
    where prev_interv is not null
)
group by ans_id;



-- copy_reg: reg_num
CREATE TABLE finance_data.np_metric_copy_reg_stats
    WITH (external_location = %(loc)s)
AS 
select ans_id, max(total) as reg_num from
(
    select b.company_name, a.ans_id, b.total from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (select distinct company_name, case when ((total is null) OR (total = '')) then 0.0 else cast(total as DOUBLE) end as total from npd_st.copy_reg) b
    on a.clean_name = b.company_name
) a
group by a.ans_id;



-- copy_reg: reg_growth_ratio
CREATE TABLE finance_data.np_metric_copy_reg_growthRatio
    WITH (external_location = %(loc)s)
AS 
select distinct m.ans_id, (reg_num_recent3y - (reg_num_recent6y - reg_num_recent3y)) / (reg_num_recent3y + 1.0) as reg_growth_ratio from
(
    select ans_id, count(reg_num) as reg_num_recent6y from
    (
        select b.company_name, a.ans_id, b.reg_num, b.reg_year from
        (select distinct clean_name, ans_id from npd_st.names_ansid) a
        inner join
        (
            select distinct company_name, reg_num, 
            cast(date_format(from_unixtime(cast(nullif(reg_time, '') as DOUBLE) / 1000), '%%Y') as BIGINT) as reg_year
            from npd_st.copy_reg
        ) b
        on a.clean_name = b.company_name
    ) a
    where reg_year >= year(current_date) - 6
    group by ans_id
) m
left join
(
    select ans_id, count(reg_num) as reg_num_recent3y from
    (
        select b.company_name, a.ans_id, b.reg_num, b.reg_year from
        (select distinct clean_name, ans_id from npd_st.names_ansid) a
        inner join
        (
            select distinct company_name, reg_num, 
            cast(date_format(from_unixtime(cast(nullif(reg_time, '') as DOUBLE) / 1000), '%%Y') as BIGINT) as reg_year
            from npd_st.copy_reg
        ) b
        on a.clean_name = b.company_name
    ) a
    where reg_year >= year(current_date) - 3
    group by ans_id
) n
on m.ans_id = n.ans_id;



-- copy_reg: reg_3y_avg
CREATE TABLE finance_data.np_metric_copy_reg_recent3y_avg_num
    WITH (external_location = %(loc)s)
AS 
select distinct ans_id, reg_num_recent3y / (reg_unique_years + 0.0) as reg_3y_avg from
(
    select ans_id, count(distinct reg_num) as reg_num_recent3y, count(distinct reg_year) as reg_unique_years from
    (
        select b.company_name, a.ans_id, b.reg_num, b.reg_year from
        (select distinct clean_name, ans_id from npd_st.names_ansid) a
        inner join
        (
            select distinct company_name, reg_num, 
            cast(date_format(from_unixtime(cast(nullif(reg_time, '') as DOUBLE) / 1000), '%%Y') as BIGINT) as reg_year
            from npd_st.copy_reg
        ) b
        on a.clean_name = b.company_name
    ) a
    where reg_year >= 2018
    group by ans_id
) n;



-- trademark: num_trademarks_registered, num_trademarks_total
CREATE TABLE finance_data.np_metric_trademark_total
    WITH (external_location = %(loc)s)
AS
select ans_id, count(distinct trademark_id) as num_trademarks_total from
(
    select b.company_name, a.ans_id, b.trademark_id from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (
        select distinct company_name, trademark_id from npd_st.trademark
        where status in ('初审公告', '商标申请中', '等待实质审查', '驳回复审中', '商标已注册')
    ) b
    on a.clean_name = b.company_name
) a
group by a.ans_id;

CREATE TABLE finance_data.np_metric_trademark_registered
    WITH (external_location = %(loc)s)
AS
select ans_id, count(distinct trademark_id) as num_trademarks_registered from
(
    select b.company_name, a.ans_id, b.trademark_id from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (
        select distinct company_name, trademark_id from npd_st.trademark
        where status in ('商标已注册')
    ) b
    on a.clean_name = b.company_name
) a
group by a.ans_id;



-- corporate quality certificate: qualify_certificate
CREATE TABLE finance_data.np_metric_corporate_qualification
    WITH (external_location = %(loc)s)
AS 
select b.company_name, a.ans_id, b.qualify_certificate, b.public_type from
(select distinct clean_name, ans_id from npd_st.names_ansid) a
inner join
(select distinct company_name, 0 as qualify_certificate, 1 as public_type from npd_st.certificate) b
on a.clean_name = b.company_name;
-- select distinct company_name, qualify_certificate, public_type from npd_st.corporate_qualify_certificate;



-- certificate: num_certificates
CREATE TABLE finance_data.np_metric_certificate
    WITH (external_location = %(loc)s)
AS
select ans_id, count(distinct certificate_number) as num_certificates from
(
    select b.company_name, a.ans_id, b.certificate_number from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (select distinct company_name, certificate_number from npd_st.certificate) b
    on a.clean_name = b.company_name
) a
group by a.ans_id;



-- team_member: num_members
CREATE TABLE finance_data.np_metric_team_member
    WITH (external_location = %(loc)s)
AS
select ans_id, count(distinct member_name) as num_members from
(
    select b.company_name, a.ans_id, b.member_name from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (
        select distinct company_name, member_name from npd_st.team_member
        where cast(nullif(is_dimission, '') as DOUBLE) = 0
    ) b
    on a.clean_name = b.company_name
) a
group by a.ans_id;



-- capital: reg_capital_formatted, actual_capital_formatted, duration
CREATE TABLE finance_data.np_metric_capital_duration
    WITH (external_location = %(loc)s)
AS 
select ans_id, max(actual_capital_formatted) as actual_capital_formatted, max(reg_capital_formatted) as reg_capital_formatted, max(duration) as duration from
(
    select b.company_name, a.ans_id, b.actual_capital_formatted, b.reg_capital_formatted, b.duration from
    (select distinct clean_name, ans_id from npd_st.names_ansid) a
    inner join
    (
        select distinct clean_name as company_name, actual_capital as actual_capital_formatted, reg_capital as reg_capital_formatted,
        date_diff('day', date_parse(cast(nullif(date_establish, '0') as VARCHAR(255)), '%%Y%%m%%d'), now()) / 365.0 as duration
        from npd_st.basic_info_new
    ) b
    on a.clean_name = b.company_name
) a
group by a.ans_id;



-- patent: patent value stats
CREATE TABLE finance_data.p_metric_patent_value_stats
    WITH(external_location = %(loc)s)
AS
select ans_id, sum(avg_price) as value_sum, avg(avg_price) as value_avg from
(
    SELECT DISTINCT pbca.ans_id, family_original, technology_score, economy_score, legal_score, strategic_score, market_score, rating_date, (vo + vu) / 2 AS avg_price
    FROM patent_dw.patent_biblio_current_assignee pbca
    INNER JOIN pspv.lastest_patsnap_pv ppv ON pbca.patent_id = ppv.patent_id
    INNER JOIN (select distinct clean_name, ans_id from finance_data.{version_tag}sse_poc_company_t) b on pbca.ans_id = b.ans_id
) m
group by m.ans_id;



-- patent: patent value top 5
CREATE TABLE finance_data.p_metric_patent_value_top5
    WITH(external_location = %(loc)s)
AS
select ans_id, avg(avg_price) as value_top5_avg from
(
    select ans_id, avg_price, row_number() over (partition by ans_id order by avg_price desc) as rowNum from
    (
        SELECT DISTINCT pbca.ans_id, family_original, technology_score, economy_score, legal_score, strategic_score, market_score, rating_date, (vo + vu) / 2 AS avg_price
        FROM patent_dw.patent_biblio_current_assignee pbca
        INNER JOIN pspv.lastest_patsnap_pv ppv ON pbca.patent_id = ppv.patent_id
        INNER JOIN (select distinct clean_name, ans_id from finance_data.{version_tag}sse_poc_company) b on pbca.ans_id = b.ans_id
    ) m
) n
where rowNum <= 5
group by n.ans_id;



-- Together
CREATE TABLE finance_data.{version_tag}sse_company_all_indicators
    WITH (external_location = %(loc)s)
AS
SELECT DISTINCT c.*,
    d.money_max, d.money_sum, d.rounds,
    e.most_recent_money_formatted, e.most_recent_time,
    f.avg_funding_interval,
    g.reg_num,
    h.reg_growth_ratio,
    i.reg_3y_avg,
    j.num_trademarks_total, k.num_trademarks_registered,
    l.qualify_certificate, l.public_type,
    m.num_certificates,
    n.num_members,
    o.actual_capital_formatted, o.reg_capital_formatted, o.duration,
    p.value_sum, p.value_avg, 
    q.value_top5_avg,
    j.num_trademarks_total + c.apno_cnt + g.reg_num as techno_capacity
FROM finance_data.{version_tag}sse_company_patent_indicators c
LEFT JOIN finance_data.np_metric_fundings_stats d
    ON c.ans_id = d.ans_id
LEFT JOIN finance_data.np_metric_fundings_mostRecent e
    ON c.ans_id = e.ans_id 
LEFT JOIN finance_data.np_metric_fundings_avgFundingInterval f
    ON c.ans_id = f.ans_id
LEFT JOIN finance_data.np_metric_copy_reg_stats g
    ON c.ans_id = g.ans_id
LEFT JOIN finance_data.np_metric_copy_reg_growthRatio h
    ON c.ans_id = h.ans_id
LEFT JOIN finance_data.np_metric_copy_reg_recent3y_avg_num i
    ON c.ans_id = i.ans_id
LEFT JOIN finance_data.np_metric_trademark_total j
    ON c.ans_id = j.ans_id
LEFT JOIN finance_data.np_metric_trademark_registered k
    ON c.ans_id = k.ans_id
LEFT JOIN finance_data.np_metric_corporate_qualification l
    ON c.ans_id = l.ans_id
LEFT JOIN finance_data.np_metric_certificate m
    ON c.ans_id = m.ans_id
LEFT JOIN finance_data.np_metric_team_member n
    ON c.ans_id = n.ans_id
LEFT JOIN finance_data.np_metric_capital_duration o
    ON c.ans_id = o.ans_id
LEFT JOIN finance_data.p_metric_patent_value_stats p
    on c.ans_id = p.ans_id
LEFT JOIN finance_data.p_metric_patent_value_top5 q
    on c.ans_id = q.ans_id;