CREATE TABLE npd_st.{version_tag}company_reg_num_metric
    WITH (external_location = %(loc)s)
AS
select distinct company_id,
       count(distinct if(cast(reg_time/10000 as bigint)=cast(year(now()) as bigint),reg_num,null)) as reg_num_fy1,
       count(distinct if(cast(reg_time/10000 as bigint)=(cast(year(now()) as bigint)-1),reg_num,null)) as reg_num_fy2,
       count(distinct if(cast(reg_time/10000 as bigint)=(cast(year(now()) as bigint)-2),reg_num,null)) as reg_num_fy3
       from npd_dw.innovation_copy_reg
       group by company_id