CREATE TABLE finance_data.{version_tag}invention_cnt_validity
    WITH(external_location = %(loc)s)
AS
select ans_id,
       count(distinct if(max_remaining_life_span>10,apno,null)) as invention_cnt_validity_higher_10,
       count(distinct if(max_remaining_life_span<=10 and max_remaining_life_span>=5,apno,null)) as invention_cnt_validity_higher_5_lower_10,
       count(distinct if(max_remaining_life_span<5,apno,null)) as invention_cnt_validity_lower_5
       from
(SELECT scp.ans_id,scp.apno,
         max (IF(legal_t.valid_duration - legal_t.patent_age > 0,
         cast((legal_t.valid_duration - legal_t.patent_age) as double) / 365,
        NULL)) AS max_remaining_life_span
FROM finance_data.${version_tag}sse_company_patent scp
JOIN pspv.pv_legal_status_tmp legal_t
    ON scp.patent_id = legal_t.patent_id
        where  legal_t.simple_legal_status IN ('granted') and scp.patent_type in ('A','B')
GROUP BY  scp.ans_id,scp.apno)a1
group by ans_id