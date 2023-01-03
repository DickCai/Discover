CREATE TABLE npd_st.{version_tag}company_fundings_metric
    WITH (external_location = %(loc)s)
AS
with currency_trans as
       ( select '人民币' currency, 1 rate union all
        select '美元' currency, 6.38 rate union all
         select  '日元' currency,0.056 rate union all
        select '欧元' currency , 7.18 rate union all
        select null currency , 1 rate union all
        select '克郎' currency ,0.7717 rate union all
        select '瑞士法郎' currency, 6.9 rate union all
        select '港元' currency, 0.82 rate union all
        select '港币' currency , 0.85 rate union all
        select '法国法郎' currency ,1.1633 rate union all
        select '英镑' currency,8.42 rate union all
        select '澳大利亚元' currency, 4.73 rate union all
        select '澳元' currency ,4.52 rate union all
        select '澳门元' currency ,0.79 rate union all
        select '新台币' currency , 0.23 rate union all
        select '德国马克' currency,4.1 rate union all
        select '加拿大元' currency, 5.19 rate union all
        select '新加坡元' currency, 4.66 rate union all
        select '瑞典克朗' currency,0.7 rate union all
        select '韩元' currency, 0.0053 rate union all
        select '挪威克郎' currency,0.775 rate ),
 avg_interval as
( select company_id,if(pub_count>1,(cast(datediff(date_list[pub_count-1],date_list[0]) as double)/365)/(pub_count-1),null) as avg_funding_interval_all,date_list
from
(select distinct company_id ,count(distinct pub_date) AS pub_count,array_sort(collect_list(pub_date)) as date_list

	  from (select ifi.company_id,timestamp_millis(pub_time) as pub_date
          from npd_dw.innovation_financing ifi
          join npd_st.one_id_company_full oicf
          on ifi.company_id=oicf.company_id
    where pub_time is not null
		 order by ifi.company_id,pub_time desc)
group by company_id)
),
round_count as
(select distinct company_id,count(round) as round_all
from npd_dw.innovation_financing
group by company_id
)
select a.company_id,fin_money_sum_all,ai.avg_funding_interval_all,rc.round_all
    from
    (select company_id,sum(fin_money) as fin_money_sum_all
    from
    (select  ifi.company_id,
          case when ifi.money not in ('未披露', '未透露')
               then if(money_lower is null and money_upper is null, 0,
                         if(money_lower is null,money_upper *ct.rate*10000,
                         if(money_upper is null , money_lower *ct.rate*10000,
                         (money_upper + money_lower)/2 * ct.rate*10000)))

               else 0
          end as fin_money

              from npd_dw.innovation_financing ifi
              join npd_st.one_id_company_full oicf
              on ifi.company_id=oicf.company_id
              join currency_trans ct on ifi.currency=ct.currency)
         group by company_id)a
    join avg_interval ai on ai.company_id=a.company_id
    join round_count rc on a.company_id=rc.company_id


