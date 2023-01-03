CREATE TABLE finance_data.{version_tag}patent_award_label
    WITH(external_location = %(loc)s)
AS
SELECT scp.ans_id,
      array_distinct(collect_list(patent_award.award_name)) as  patent_award_label
FROM finance_data.${version_tag}sse_company_patent scp
join
(SELECT patent_id,
case
when a.name like '%实用新型%' or a.name like '%外观%' then a.name
 else concat(a.name,a.level)
end as award_name
    FROM patent_dw.patent_metadata
    LATERAL VIEW EXPLODE(award)tmt AS a
    WHERE award is not null) patent_award
on scp.patent_id=patent_award.patent_id
group by scp.ans_id


/*
 case
 when a.name='中国专利奖'  then concat(a.name,a.level)
 when a.level in ('特等奖') then concat(a.name,'特等奖')
 when a.level in ('一等奖','金奖') then concat(a.name,'金奖')
 when a.level in ('二等奖','银奖') then concat(a.name,'银奖')
 when a.level in ('三等奖','优秀奖') then concat(a.name,'优秀奖')
 when a.name like '%实用新型%' or a.name like '%外观%' then a.name
 else concat(a.name,a.level)
end as award_name
 */