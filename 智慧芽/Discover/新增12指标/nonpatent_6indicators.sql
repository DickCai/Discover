create table npd_st.v2_patent_monthly_nonpatent_6indicators
    WITH (external_location = %(loc)s)
AS
select a.entity_id,
       a.company_id,
       reg_num_fy1,
       reg_num_fy2,
       reg_num_fy3,
       fin_money_sum_all,
       avg_funding_interval_all,
       round_all
from npd_st.one_id_company_full a
left join  npd_st.${version_tag}company_fundings_metric cfm
    on a.company_id=cfm.company_id
left join npd_st.${version_tag}company_reg_num_metric crnm
on a.company_id=crnm.company_id

-- 7478上 s3://datalake-internal.patsnap.com/dayu_user_file/finance_data/v2_patent_monthly_nonpatent_6indicators/

create table finance_data.v2_patent_monthly_nonpatent_6indicators
with(format='parquet',external_location='s3://datalake-internal.patsnap.com/dayu_user_file/finance_data/v2_patent_monthly_nonpatent_6indicators/')
as
select entity_id,company_id,
    0 as reg_num_fy1,0 as reg_num_fy2,0 as reg_num_fy3,
     100 as fin_money_sum_all,1.0 as avg_funding_interval_all,
      2 as round_all
      from
fiannce_data.one_id_company_full limit 10