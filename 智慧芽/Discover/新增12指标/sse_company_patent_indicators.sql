
CREATE TABLE finance_data.{version_tag}sse_company_patent_indicators
    WITH (external_location = %(loc)s)
AS 
SELECT DISTINCT c.ans_id, ac.company_id,
    c.apno_cnt,                                                                                                     -- v0.1.1
    c.non_design_apno_cnt,                                                                                          -- v0.1.1
    c_valid.apno_cnt_valid,                                                                                         -- v0.2.1
    c_valid.apno_cnt_valid_5y,                                                                                      -- v0.2.1
    valid_ratio.patent_valid_ratio,                                                                                 -- v0.2.1
    COALESCE(internal_concentration.tech_internal_concentration, 0) AS tech_internal_concentration,                 -- v0.1.1
    internal_concentration.top5_main_group_detail,                                                                  -- not in use
    COALESCE(global_concentration.tech_global_concentration, 0) AS tech_global_concentration,                       -- v0.1.1
    tech_width_t.tech_width,                                                                                        -- v0.1.1
    tech_quality.avg_technology_score,                                                                              -- v0.1.1
    COALESCE(ip_dependency_t.ip_dependency, 0) AS ip_dependency,                                                    -- v0.1.1
    origin_t.orignal_ratio,                                                                                         -- not in use
    COALESCE(award_t.total_patent_award_score, 0) AS total_patent_award_score,                                      -- v0.1.1
    COALESCE(pct_t.pct_apno_cnt, 0) AS pct_apno_cnt,                                                                -- v0.1.1
    COALESCE(pct_t.non_design_pct_apno_cnt, 0) AS non_design_pct_apno_cnt,                                          -- v0.2.1
    global_layout.pct_apno_ratio,                                                                                   -- not in use
    COALESCE(gi_t.granted_invention_cnt, 0) AS granted_invention_cnt,                                               -- v0.1.1
    COALESCE(gi_t.granted_invention_cnt_5y, 0) AS granted_invention_cnt_5y,                                         -- not use
    COALESCE(gi_t.granted_invention_cnt_3y, 0) AS granted_invention_cnt_3y,                                         -- not use
    COALESCE(gi_t.granted_invention_cnt, 0.0)/NULLIF(c.apno_cnt, 0.0) AS granted_invention_ratio,                   -- v0.1.1
    COALESCE(gi_t_pb.granted_invention_cnt_pb_3y, 0) AS granted_invention_cnt_pb_3y,                                -- not use
    COALESCE(gi_t_pb.granted_invention_cnt_pb_2y, 0) AS granted_invention_cnt_pb_2y,                                -- not use
    COALESCE(gi_t_pb.granted_invention_cnt_pb_1y, 0) AS granted_invention_cnt_pb_1y,                                -- v0.2.1
    COALESCE(gi_t_ap_original.granted_invention_cnt_ap_original, 0) AS granted_invention_cnt_ap_original,           -- not use
    COALESCE(gi_t_ap_original.granted_invention_cnt_ap_original_5y, 0) AS granted_invention_cnt_ap_original_5y,     -- not use
    COALESCE(gi_t_ap_original.granted_invention_cnt_ap_original_3y, 0) AS granted_invention_cnt_ap_original_3y,     -- not use
    COALESCE(gi_t_pb_original.granted_invention_cnt_pb_original_3y, 0) AS granted_invention_cnt_pb_original_3y,     -- not use
    COALESCE(gi_t_pb_original.granted_invention_cnt_pb_original_2y, 0) AS granted_invention_cnt_pb_original_2y,     -- v0.2.1
    COALESCE(gi_t_pb_original.granted_invention_cnt_pb_original_1y, 0) AS granted_invention_cnt_pb_original_1y,     -- not use
    structure_t.invention_ratio,                                                                                    -- v0.1.1
    citation_t.avg_cited_by_cnt,                                                                                    -- v0.1.1
    core_patents_t.apno_cnt_cited,                                                                                  -- v0.2.1
    core_patents_t.core_patents_cited_by_cnt,                                                                       -- v0.1.1
    core_patents_t.core_patents_cited_by_ratio,                                                                     -- v0.1.1
    v.most_cited_patents_value,                                                                                     -- v0.1.1
    COALESCE(external_licencing_t.external_licencing_ratio, 0.0) AS external_licencing_ratio,                       -- not in use
    COALESCE(external_licencing_t.licensing_apno_cnt, 0) AS external_licensing_cnt,                                 -- v0.1.1
    COALESCE(cii_t.top5_current_impact_index, 0.0) AS top5_current_impact_index,                                    -- not in use
    COALESCE(cqi_t.top5_current_quality_index, 0.0) AS top5_current_quality_index,                                  -- v0.1.1
    examining_t.in_examing_ratio,                                                                                   -- v0.1.1
    life_span_t.avg_remaining_life_span,                                                                            -- v0.1.1
    rd_efficiency.per_capita_efficiency,                                                                            -- not in use
    rd_efficiency.patenting_growth_ratio,                                                                           -- v0.1.1
    rd_efficiency.avg_3y_cnt,                                                                                       -- v0.1.1
    rd_scale.active_inventor_cnt,                                                                                   -- not in use
    rd_scale.inventor_cnt,                                                                                          -- v0.2.1
    rd_scale.active_inventor_ratio,                                                                                 -- v0.1.1
    rd_scale.invention_stability,                                                                                   -- v0.1.1
    COALESCE( rd_scale.transfer_in_out_ratio, 0.0) AS transfer_in_out_ratio,                                        -- not in use
    rd_scale.self_cited_by_ratio,                                                                                   -- v0.1.1
    ir_stat.avg_cooperation_time_span,                                                                              -- not in use
    ir_stat.joint_applicant_cnt,                                                                                    -- v0.1.1
    ir_stat.joint_application_cnt,                                                                                  -- v0.1.1
    global_layout.country_cnt,                                                                                      -- v0.1.1
    global_layout.avg_simple_family_size,                                                                           -- not in use
    COALESCE(litigation_t.litigation_case_no_as_defendant, 0) AS litigation_case_no_as_defendant,                   -- not in use
    COALESCE(litigation_t.litigation_case_no_as_plaintiff, 0) AS litigation_case_no_as_plaintiff,                   -- not in use
    rd_exp_t.patent_to_rd_exp_ratio * 1e6 AS patent_to_rd_exp_ratio,                                                -- not in use
    pv_stats.value_sum,                                                                                             -- v0.1.1
    pv_stats.value_avg,                                                                                             -- v0.1.1
    pv_top5.value_top5_avg,                                                                                         -- v0.1.1
    pv_top10.value_top10_avg,                                                                                        -- v0.1.1
    COALESCE(cisc.ipc_sub_class_cnt,0) AS ipc_sub_class_cnt
FROM 
(SELECT DISTINCT ans_id, apno_cnt, non_design_apno_cnt FROM finance_data.{version_tag}sse_poc_company_t) c
LEFT JOIN
(SELECT DISTINCT ans_id, company_id FROM {target_db_table_name}) ac
  ON c.ans_id = ac.ans_id
LEFT JOIN finance_data.{version_tag}sse_poc_company_t_valid c_valid
  ON c.ans_id = c_valid.ans_id
LEFT JOIN finance_data.{version_tag}company_patent_valid_ratio valid_ratio
  ON c.ans_id = valid_ratio.ans_id
LEFT JOIN finance_data.{version_tag}company_tech_internal_concentration internal_concentration
  ON c.ans_id = internal_concentration.ans_id
LEFT JOIN finance_data.{version_tag}company_tech_global_concentration global_concentration
  ON c.ans_id = global_concentration.ans_id 
LEFT JOIN finance_data.{version_tag}company_tech_width tech_width_t
  ON c.ans_id = tech_width_t.ans_id
LEFT JOIN finance_data.{version_tag}company_avg_tech_quality tech_quality
  ON c.ans_id = tech_quality.ans_id
LEFT JOIN finance_data.{version_tag}company_ip_dependency ip_dependency_t
  ON c.ans_id = ip_dependency_t.ans_id
LEFT JOIN finance_data.{version_tag}company_apno_origin origin_t 
  ON c.ans_id = origin_t.ans_id
LEFT JOIN finance_data.{version_tag}company_patent_award_t award_t 
  ON c.ans_id = award_t.ans_id
LEFT JOIN finance_data.{version_tag}company_pct_patents pct_t 
  ON c.ans_id = pct_t.ans_id
LEFT JOIN finance_data.{version_tag}company_granted_invention_t gi_t 
  ON c.ans_id = gi_t.ans_id
LEFT JOIN finance_data.{version_tag}company_granted_invention_t_pb gi_t_pb
  ON c.ans_id = gi_t_pb.ans_id
LEFT JOIN finance_data.{version_tag}company_granted_invention_t_ap_original gi_t_ap_original
  ON c.ans_id = gi_t_ap_original.ans_id
LEFT JOIN finance_data.{version_tag}company_granted_invention_t_pb_original gi_t_pb_original
  ON c.ans_id = gi_t_pb_original.ans_id
LEFT JOIN finance_data.{version_tag}company_patent_structure structure_t
  ON c.ans_id = structure_t.ans_id 
LEFT JOIN finance_data.{version_tag}company_patent_avg_citation citation_t 
  ON c.ans_id = citation_t.ans_id
LEFT JOIN finance_data.{version_tag}company_core_patents_citation core_patents_t 
  ON c.ans_id = core_patents_t.ans_id
LEFT JOIN finance_data.{version_tag}company_most_cited_patents_value v
  ON c.ans_id = v.ans_id 
LEFT JOIN finance_data.{version_tag}company_external_licensing external_licencing_t
  ON c.ans_id = external_licencing_t.ans_id 
LEFT JOIN finance_data.{version_tag}company_top5_sub_class_avg_cii cii_t 
  ON c.ans_id = cii_t.ans_id 
LEFT JOIN finance_data.{version_tag}top5_current_quality_index cqi_t 
  ON c.ans_id = cqi_t.ans_id 
LEFT JOIN finance_data.{version_tag}company_examing_patents examining_t 
  ON c.ans_id = examining_t.ans_id
LEFT JOIN finance_data.{version_tag}company_patent_remaining_life_span life_span_t 
  ON c.ans_id = life_span_t.ans_id
LEFT JOIN finance_data.{version_tag}company_rd_efficiency rd_efficiency 
  ON c.ans_id = rd_efficiency.ans_id AND ac.company_id = rd_efficiency.company_id
LEFT JOIN finance_data.{version_tag}company_rd_scale rd_scale 
  ON c.ans_id = rd_scale.ans_id
LEFT JOIN finance_data.{version_tag}company_industrial_research_stat ir_stat 
  ON c.ans_id = ir_stat.ans_id
LEFT JOIN finance_data.{version_tag}company_global_layout global_layout 
  ON c.ans_id = global_layout.ans_id
LEFT JOIN  finance_data.{version_tag}sse_company_litigation_stat litigation_t 
  ON c.ans_id = litigation_t.ans_id
LEFT JOIN finance_data.{version_tag}company_patent_rd_expense rd_exp_t 
  ON c.ans_id = rd_exp_t.ans_id
LEFT JOIN finance_data.{version_tag}p_metric_patent_value_stats pv_stats
  ON c.ans_id = pv_stats.ans_id
LEFT JOIN finance_data.{version_tag}p_metric_patent_value_top5 pv_top5
  ON c.ans_id = pv_top5.ans_id
LEFT JOIN finance_data.{version_tag}p_metric_patent_value_top10 pv_top10
  ON c.ans_id = pv_top10.ans_id
LEFT JOIN finance_data.${version_tag}company_ipc_sub_class cisc
  ON c.ans_id = cisc.ans_id


-- spark sql
SELECT DISTINCT oicf.entity_id,
         oicf.company_id,
         c.ans_id,
         c_valid.apno_cnt_valid,
         c_valid.apno_cnt_valid_5y,
         c_valid.non_design_apno_valid_cnt,
         valid_ratio.patent_valid_ratio,
         COALESCE(invention_t.granted_invention_cnt_5y,0) AS granted_invention_cnt_5y,
         COALESCE(c.apno_cnt,0) apno_cnt,
         COALESCE(c.non_design_apno_cnt, 0) non_design_apno_cnt,
         COALESCE(tech_global_concentration, 0) tech_global_concentration,
         tech_width,
         avg_technology_score,
         COALESCE(ip_dependency,0) ip_dependency,
         COALESCE(cpan.total_patent_award_score,0.0) total_patent_award_score,
         COALESCE(global_layout.pct_apno_cnt,0) pct_apno_cnt,
         COALESCE(global_layout.non_design_pct_apno_cnt, 0) AS non_design_pct_apno_cnt,
         COALESCE(invention_t.granted_invention_cnt,0) granted_invention_cnt,
         COALESCE(gi_t_pb_original.granted_invention_cnt_pb_original_2y,0) AS granted_invention_cnt_pb_original_2y,
         COALESCE(invention_t.granted_invention_cnt_pb_1y, 0) AS granted_invention_cnt_pb_1y,
         COALESCE(invention_t.granted_invention_ratio,0) granted_invention_ratio,
         invention_ratio,
         COALESCE(avg_cited_by_cnt, 0) avg_cited_by_cnt,
         core_patents_t.apno_cnt_cited,
         COALESCE(core_patents_t.core_patents_cited_by_cnt, 0) core_patents_cited_by_cnt,
         core_patents_t.core_patents_cited_by_ratio,
         v.most_cited_patents_value,
         COALESCE(licensing_apno_cnt, 0) external_licensing_cnt,
         COALESCE(top5_current_impact_index, 0.0) top5_current_impact_index,
         COALESCE(top5_current_quality_index, 0.0) top5_current_quality_index,
         in_examing_ratio,
         avg_remaining_life_span,
         patenting_growth_ratio,
         COALESCE(avg_3y_cnt,0.0) avg_3y_cnt,
         cai.active_inventor_cnt,
         cai.inventor_cnt,
         cai.active_inventor_ratio,
         invention_stability,
         self_cited_by_ratio,
         COALESCE(cja1.joint_applicant_cnt,0) joint_applicant_cnt,
         COALESCE(cja.joint_application_cnt, 0) joint_application_cnt,
         COALESCE(global_layout.country_cnt, 0) country_cnt,
         pv_stats.value_sum,
         pv_stats.value_avg,
         pv_top5.value_top5_avg,
         pv_top10.value_top10_avg,
         COALESCE(cisc.ipc_sub_class_cnt,0) AS ipc_sub_class_cnt,
         reg_num_fy1, reg_num_fy2,reg_num_fy3,fin_money_sum_all, avg_funding_interval_all, round_all,
         invention_cnt_validity_higher_10,invention_cnt_validity_higher_5_lower_10,invention_cnt_validity_lower_5,
         COALESCE(if(cardinality(patent_award_label)>0,patent_award_label,null),array['-']) as patent_award_label,
         apno_cnt_valid_5y_cn,patent_invalid_ratio_cn
FROM finance_data.one_id_company_full oicf
JOIN finance_data.${version_tag}sse_poc_company_t c
    ON oicf.ans_id=c.ans_id
LEFT JOIN finance_data.${version_tag}sse_poc_company_t_valid c_valid
    ON oicf.ans_id = c_valid.ans_id
LEFT JOIN finance_data.${version_tag}company_patent_valid_ratio valid_ratio
    ON oicf.ans_id = valid_ratio.ans_id
LEFT JOIN finance_data.${version_tag}company_tech_global_concentration global_concentration
    ON oicf.ans_id = global_concentration.ans_id
LEFT JOIN finance_data.${version_tag}company_tech_width tech_width_t
    ON oicf.ans_id = tech_width_t.ans_id
LEFT JOIN finance_data.${version_tag}company_avg_tech_quality tech_quality
    ON oicf.ans_id = tech_quality.ans_id
LEFT JOIN finance_data.${version_tag}company_ip_dependency ip_dependency_t
    ON oicf.ans_id = ip_dependency_t.ans_id
LEFT JOIN finance_data.${version_tag}company_granted_invention_t invention_t
    ON oicf.ans_id = invention_t.ans_id
LEFT JOIN finance_data.${version_tag}company_granted_invention_t_pb gi_t_pb_original
    ON oicf.ans_id=gi_t_pb_original.ans_id
LEFT JOIN finance_data.${version_tag}company_patent_structure structure_t
    ON oicf.ans_id = structure_t.ans_id
LEFT JOIN finance_data.${version_tag}company_patent_avg_citation citation_t
    ON oicf.ans_id = citation_t.ans_id
LEFT JOIN finance_data.${version_tag}company_core_patents_citation core_patents_t
    ON oicf.ans_id = core_patents_t.ans_id
LEFT JOIN finance_data.${version_tag}company_most_cited_patents_value v
    ON oicf.ans_id = v.ans_id
LEFT JOIN finance_data.${version_tag}company_external_licensing external_licencing_t
    ON oicf.ans_id = external_licencing_t.ans_id
LEFT JOIN finance_data.${version_tag}company_top5_sub_class_avg_cii cii_t
    ON oicf.ans_id = cii_t.ans_id
LEFT JOIN finance_data.${version_tag}top5_current_quality_index cqi_t
    ON oicf.ans_id = cqi_t.ans_id
LEFT JOIN finance_data.${version_tag}company_examing_patents examining_t
    ON oicf.ans_id = examining_t.ans_id
LEFT JOIN finance_data.${version_tag}company_patent_remaining_life_span life_span_t
    ON oicf.ans_id = life_span_t.ans_id
LEFT JOIN finance_data.${version_tag}company_global_layout global_layout
    ON oicf.ans_id = global_layout.ans_id
LEFT JOIN finance_data.${version_tag}p_metric_patent_value_stats pv_stats
    ON oicf.ans_id = pv_stats.ans_id
LEFT JOIN finance_data.${version_tag}p_metric_patent_value_top5 pv_top5
    ON oicf.ans_id = pv_top5.ans_id
LEFT JOIN finance_data.${version_tag}p_metric_patent_value_top10 pv_top10
    ON oicf.ans_id = pv_top10.ans_id
LEFT JOIN finance_data.${version_tag}company_apno_self_citation casc
    ON oicf.ans_id = casc.ans_id
LEFT JOIN finance_data.${version_tag}company_active_inventors cai
    ON oicf.ans_id = cai.ans_id
LEFT JOIN finance_data.${version_tag}company_invention_stability cis
    ON oicf.ans_id = cis.ans_id
LEFT JOIN finance_data.${version_tag}company_joint_application cja
    ON oicf.ans_id = cja.ans_id
LEFT JOIN finance_data.${version_tag}company_joint_applicant cja1
    ON oicf.ans_id = cja1.ans_id
LEFT JOIN finance_data.${version_tag}company_patenting_growth cpg
    ON oicf.ans_id = cpg.ans_id
        AND oicf.entity_id=cpg.entity_id
LEFT JOIN finance_data.${version_tag}company_patent_award_new cpan
    ON oicf.ans_id = cpan.ans_id
LEFT JOIN finance_data.${version_tag}company_ipc_sub_class cisc
    ON oicf.ans_id = cisc.ans_id
LEFT JOIN finance_data.v2_patent_monthly_nonpatent_6indicators v2pmn
    ON oicf.entity_id = v2pmn.entity_id and oicf.company_id=v2pmn.company_id
LEFT JOIN finance_data.${version_tag}patent_award_label pal
    ON oicf.ans_id = pal.ans_id
LEFT JOIN finance_data.${version_tag}invention_cnt_validity icv
    ON oicf.ans_id = icv.ans_id
LEFT JOIN finance_data.${version_tag}sse_poc_company_valid_cn spcvc
    ON oicf.ans_id = spcvc.ans_id

