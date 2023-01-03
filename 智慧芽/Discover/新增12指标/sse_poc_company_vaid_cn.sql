CREATE TABLE finance_data.{version_tag}sse_poc_company_valid_cn
    WITH(external_location = %(loc)s)
AS
select ans_id,apno_cnt_valid_5y_cn,
       coalesce(1-(apno_cnt_valid_cn/nullif(apno_cnt_cn,0)),0.0) as patent_invalid_ratio_cn
       from
(select distinct ans_id,
  COALESCE(count(distinct apno),0) as apno_cnt_cn,
  COUNT(DISTINCT if( legal_code not in (11, 13, 14, 16, 17, 19, 20, 22, 30),apno,null )) AS apno_cnt_valid_cn,
  COUNT(DISTINCT IF((to_timestamp(COALESCE(apdt, '20000101'), 'yyyyMMdd') between add_months(current_date,-60 ) AND current_date)
                        and (legal_code not in (11, 13, 14, 16, 17, 19, 20, 22, 30)) , apno, NULL)) AS apno_cnt_valid_5y_cn
    from  (select ca.ans_id, b.apno, b.legal_date, b.legal_code, p.apdt
                from (select distinct apno,
                                      legal_date,
                                      legal_code,
                                      ROW_NUMBER() OVER (PARTITION by apno order by legal_date desc) as rk
                      from (select apno, x.legal_id, x.legal_date, x.legal_code
                            from patent_dw.patent_legal_history
                                LATERAL VIEW EXPLODE(legal_status)tmt AS x
                            where (to_timestamp(COALESCE(x.legal_date,'20000101'), 'yyyyMMdd') <=current_date and x.legal_date >0  )
                           )
                     ) b
                         join finance_data.${version_tag}sse_company_patent ca
                         on b.apno=ca.apno
                    join patent_dw.patent_biblio p
                    on ca.apno=p.apno
                where rk=1
            )c

GROUP BY ans_id)d
