
%let py=2017;

%let agg_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg;  
%let builder_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\builder_output;
%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11;  
%let py_loc=&agg_loc.\py&py.;
%let post_loc=&agg_loc.\post_py&py.;

%let tcc_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\tcc;

%let qc_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\QC;

*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

libname by1 "&builder_loc.\year_&py." access=readonly;
libname by2 "&builder_loc.\year_%sysevalf(&py.-1)" access=readonly;
libname by3 "&builder_loc.\year_%sysevalf(&py.-2)" access=readonly;

libname ay1 "&agg_loc.\year_&py." access=readonly;
libname ay2 "&agg_loc.\year_%sysevalf(&py.-1)" access=readonly;
libname ay3 "&agg_loc.\year_%sysevalf(&py.-2)" access=readonly;

libname py "&py_loc.";
libname tcc "&tcc_loc.";
libname post "&post_loc.";

libname qc "&qc_loc.";
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";


*********************************;
*** builder output and agg QC ***;
*********************************;

%macro epi_all(year);
data  epi_all_y&year.;
set 
by&year..ppi_ip_proc_trigger
by&year..ppi_iop_proc_trigger
by&year..ppi_iop_mjrle_trigger
by&year..ppi_op_proc_trigger
by&year..ppi_pr_proc_trigger
by&year..ppi_ip_med_trigger
by&year..ppi_ac_med_trigger
by&year..ppi_ro_trigger(drop=discharge)
by&year..ppi_ch_med_trigger(drop=discharge)
%if &year.=1 %then %do;
by&year..ppi_perinatal_trigger
by&year..ppi_chemo_trigger(drop=discharge)
%end;
;
if exclusion=1 then delete;
run;
%mend;
%epi_all(1);
/*%epi_all(2);*/
/*%epi_all(3);*/
data qc_builder_all_epi; set epi_all_y1/*-epi_all_y3*/; run;
proc datasets lib=work noprint; delete epi_all_y:; run; quit;

%macro trigger_all(year);
data trigger_all_y&year.;
set 
ay&year..trigger_ppi_ip:
ay&year..trigger_ppi_iop:
ay&year..trigger_ppi_op_proc
ay&year..trigger_ppi_pr_proc
ay&year..trigger_ppi_ac_med
ay&year..trigger_ppi_ro
ay&year..trigger_ppi_ch_med
%if &year.=1 %then %do;
ay&year..trigger_ppi_perinatal
ay&year..trigger_ppi_chemo
%end;
;
run;
%mend;
%trigger_all(1);
/*%trigger_all(2);*/
/*%trigger_all(3);*/
data qc_all_agg_trigger; set trigger_all_y1/*-trigger_all_y3*/tcc.bene_ppi_tcc; run;
proc datasets lib=work noprint; delete trigger_all_y:; run; quit;


%macro bene_all(year);
data bene_all_y&year.;
set 
ay&year..bene_ppi_ip_:
ay&year..bene_ppi_iop_:
ay&year..bene_ppi_op_proc
ay&year..bene_ppi_pr_proc
ay&year..bene_ppi_ac_med
ay&year..bene_ppi_ro
ay&year..bene_ppi_ch_med
%if &year.=1 %then %do;
ay&year..bene_ppi_perinatal
ay&year..bene_ppi_chemo
%end;
;
run;
%mend;
%bene_all(1);
/*%bene_all(2);*/
/*%bene_all(3);*/
data qc_all_attr_bene; set bene_all_y1/*-bene_all_y3*/ tcc.bene_cost_ppi_tcc; run;
proc datasets lib=work noprint; delete bene_all_y:; run; quit;

%macro bene_p_all(year);
data bene_p_all_y&year.;
set 
ay&year..bene_p_ppi_ip_:
ay&year..bene_p_ppi_iop_:
ay&year..bene_p_ppi_op_proc
ay&year..bene_p_ppi_pr_proc
ay&year..bene_p_ppi_ac_med
ay&year..bene_p_ppi_ro
ay&year..bene_p_ppi_ch_med
%if &year.=1 %then %do;
ay&year..bene_p_ppi_perinatal
ay&year..bene_p_ppi_chemo
%end;
;
run;
%mend;
%bene_p_all(1);
/*%bene_all(2);*/
/*%bene_all(3);*/
data qc_all_ra_bene; set bene_p_all_y1/*-bene_all_y3*/ tcc.bene_p_ppi_tcc; run;
proc datasets lib=work noprint; delete bene_p_all_y:; run; quit;

data qc_bene_3yr_all;
set py.bene_p_ppi_: tcc.bene_p_ppi_tcc;
run;


*** epi count change ***;
proc  sql;
create table qc.qc_epi_drop as
select distinct a.episode_type, a.clinical_category, a.def_sub, a.def_desc, x.*, b.*, c.*, d.*, e.*,
	agg_cnt/builder_epi_cnt as agg_pct, attr_cnt/agg_cnt as attr_pct, ra_cnt/attr_cnt as ra_pct, final_cnt/ra_cnt as final_pct
from meta2.ppi_name_mapping a
left join (select def_name, def_sub, count(*) as builder_epi_cnt from qc_builder_all_epi group by def_name, def_sub) x
on a.def_sub=x.def_sub or (a.def_name="PPI_CHEMO" and a.def_name=x.def_name) or (a.def_name="PPI_PERINATAL" and a.def_name=x.def_name)
left join (select def_sub, count(*) as agg_cnt from qc_all_agg_trigger group by def_sub) b
on a.def_sub=b.def_sub
left join (select def_sub, count(*) as attr_cnt from qc_all_attr_bene group by def_sub) c
on a.def_sub=c.def_sub
left join (select def_sub, count(*) as ra_cnt from qc_all_ra_bene group by def_sub) d
on a.def_sub=d.def_sub
left join (select def_sub, count(*) as final_cnt from qc_bene_3yr_all group by def_sub) e
on a.def_sub=e.def_sub
order by episode_type, clinical_category, def_sub, def_desc;
quit;


*** episode summary ***;
proc sql;
create table qc.qc_epi_cost as
select distinct episode_type, clinical_category, a.def_sub, def_desc, index_setting, a.year, count(distinct def_id) as cnt, 
	mean(t_epi_pay) as epi_mean, std(t_epi_pay)/mean(t_epi_pay) as cv, 
	mean(index_pay) as index_mean, mean(pre_pay) as pre_mean, mean(post_pay) as post_mean,
	mean(readm_cnt)*1000 as readm_1000, mean(er_cnt)*1000 as er_1000,
	sum(readm_cost)/sum(epi_pay) as readm_pct, sum(er_cost)/sum(epi_pay) as er_pct, sum(pc_cost)/sum(epi_pay) as pc_pct, 
	sum(pc_pay)/sum(epi_pay) as all_avoid_pct
from qc_all_ra_bene a, meta2.ppi_name_mapping b
where a.def_sub=b.def_sub
group by a.def_sub, a.index_setting, a.year
order by episode_type, clinical_category, def_sub, index_setting, year;
quit;


*** specialty summary ***;
proc sql;
create table qc_attr_spec as
select distinct episode_type, clinical_category, a.def_sub, def_desc, spec_cd, count(*) as cnt, mean(t_epi_pay) as t_epi_pay
from qc_all_attr_bene a, meta2.ppi_name_mapping b
where a.def_sub=b.def_sub
group by a.def_sub, spec_cd;
quit;

proc sql;
create table qc.qc_epi_spec as
select distinct *, cnt/sum(cnt) as pct
from (select a.*, b.spec_desc from qc_attr_spec a left join meta2.cms_spec_cd b on a.spec_cd=b.spec_cd)
group by def_sub
order by episode_type, clinical_category, def_sub, cnt desc;
quit;


********************************;
proc sql;
create table qc_npi_spec as
select a.*, b.spec_desc, a.npi=c.attr_npi as epi_covered, a.npi=d.attr_npi as tcc_covered
from meta2.npi_x_spec_&py. a
left join meta2.cms_spec_cd b on a.spec_cd=b.spec_cd
left join (select distinct attr_npi from qc_bene_3yr_all where def_sub^="PPI_TCC") c on a.npi=c.attr_npi
left join (select distinct attr_npi from qc_bene_3yr_all where def_sub^="PPI_TCC") d on a.npi=d.attr_npi;
quit;

proc sql;
create table qc.qc_spec_coverage as
select distinct spec_cd, spec_desc, count(*) as npi_cnt, 
	sum(epi_covered) as epi_npi_cnt,  sum(epi_covered)/count(*) as epi_npi_pct,
	sum(tcc_covered) as tcc_npi_cnt,  sum(tcc_covered)/count(*) as tcc_npi_pct,
	sum(epi_covered=1 or tcc_covered=1) as any_npi_cnt, sum(epi_covered=1 or tcc_covered=1)/count(*) as any_npi_pct
from qc_npi_spec
group by spec_cd, spec_desc
order by spec_cd, spec_desc;
quit;

*** provider coverage ***;
proc sql;
create table qc.qc_npi_coverage as
select distinct provider_type, episode_type, episode_id, episode_name, a.*, b.*, c.*,
	comb_npi_cnt/attr_npi_cnt as comb_npi_pct, final_npi_cnt/comb_npi_cnt as final_npi_pct
from 		(select distinct def_sub, count(distinct attr_npi) as attr_npi_cnt from qc_all_attr_bene group by def_sub) a
left join 	(select distinct def_sub, count(distinct attr_npi) as comb_npi_cnt from qc_bene_3yr_all group by def_sub) b
on a.def_sub=b.def_sub
left join 	(select distinct provider_type, episode_id, count(distinct provider_id) as final_npi_cnt from out.export_attr_npi_epi_spec where measure="t_epi_pay" group by episode_id) c
on a.def_sub=c.episode_id
left join meta2.ppi_name_mapping d on a.def_sub=d.def_sub;
quit; 


************************;
*** export output QC ***;
************************;
%macro export_qc(tbl);
data qc_&tbl.;
set 
out.export_attr_npi_&tbl.
out.export_attr_tin_&tbl.
out.export_attr_p_bill_&tbl.
out.export_attr_f_bill_&tbl.
out.export_attr_ccn_&tbl.
;
run;
proc sql;
create table qc_&tbl as
select distinct a.*, b.*
from qc_&tbl. a
left join meta2.ppi_name_mapping b
on a.episode_id=b.def_sub;
quit;
%mend;
%export_qc(epi_spec);
%export_qc(setting);
%export_qc(cost_bd);
%export_qc(pc);


proc sort data=qc_epi_spec out=a1 dupout=b1 nodupkey;
by ppi_type provider_type episode_type measure episode_id provider_id specialty;
run;
proc sql;
create table qc.qc_epi_spec_score as 
select distinct ppi_type, provider_type, episode_type, measure, "All" as episode_id, "All" as episode_name, "All" as specialty, 
	count(distinct provider_id) as provider_cnt, sum(rank_type="CBSA")/count(*) as cbsa_pct, sum(epi_cnt) as epi_cnt,
	sum(ppi_score_adj=.)/count(*) as score_missing_pct,
 	min(avg_observed) as min_obs, min(avg_expected) as min_exp,
	max(avg_observed) as max_obs, max(avg_expected) as max_exp,
	round(mean(avg_oe_adj),0.01) as mean_oe_adj, 
	round(min(avg_oe_adj),0.01) as min_oe_adj, 
	round(max(avg_oe_adj),0.01) as max_oe_adj,
	sum(avg_oe_adj>10) as high_oe_cnt, 
	sum(avg_oe_adj>10)/count(*) as high_oe_pct,
	sum((.<avg_oe_adj<1)*(ppi_score_adj<=2)) as low_oe_cnt, 
	sum((.<avg_oe_adj<1)*(ppi_score_adj<=2))/sum(ppi_score_adj<=2) as low_oe_pct,
	1 as order
from qc_epi_spec
group by ppi_type, provider_type, episode_type, measure
union corr
select distinct ppi_type, provider_type, episode_type, measure, episode_id, episode_name, specialty, 
	count(distinct provider_id) as provider_cnt, sum(rank_type="CBSA")/count(*) as cbsa_pct, sum(epi_cnt) as epi_cnt,
	sum(ppi_score_adj=.)/count(*) as missing_pct,
 	min(avg_observed) as min_obs, min(avg_expected) as min_exp,
	max(avg_observed) as max_obs, max(avg_expected) as max_exp,
	round(mean(avg_oe_adj),0.01) as mean_oe_adj, 
	round(min(avg_oe_adj),0.01) as min_oe_adj, 
	round(max(avg_oe_adj),0.01) as max_oe_adj,
	sum(avg_oe_adj>10) as high_oe_cnt, 
	sum(avg_oe_adj>10)/count(*) as high_oe_pct,
	sum((.<avg_oe_adj<1)*(ppi_score_adj<=2)) as low_oe_cnt, 
	sum((.<avg_oe_adj<1)*(ppi_score_adj<=2))/sum(ppi_score_adj<=2) as low_oe_pct,
	2 as order
from qc_epi_spec
group by ppi_type, provider_type, episode_type, measure, episode_id, episode_name, specialty
order by order, ppi_type, provider_type, episode_type, measure;
quit;



proc sql;
create table provider_cbd_xwalk as
select distinct a.*, coalesce(b.avg_observed, c.avg_observed) as obs_score, coalesce(b.avg_expected, c.avg_expected)as exp_score, 
	a.avg_observed-(calculated obs_score) as obs_diff, abs(round(a.avg_expected-(calculated exp_score),1)) as exp_diff
from qc_cost_bd(where=(phase in ("All" "N/A") and clm_type="All" and service_description="All")) a
left join qc_setting(where=(measure="t_epi_pay")) b on a.provider_type=b.provider_type and a.provider_id=b.provider_id and a.episode_id=b.episode_id and a.index_setting=b.index_setting
left join qc_epi_spec(where=(measure="t_epi_pay")) c on a.provider_type=c.provider_type and a.provider_id=c.provider_id 
	and ((a.ppi_type^="TCC" and a.episode_id=c.episode_id) or (a.ppi_type="TCC" and a.index_setting=c.episode_id));
quit; 

proc sql;
create table qc.qc_cbd_xwalk as
select distinct provider_type, episode_type, episode_id, episode_name, count(*) as provider_cnt, sum(obs_diff>0)/count(*) as obs_unmatch_pct, sum(exp_diff>0)/count(*) as exp_unmatch_pct
from provider_cbd_xwalk
group by provider_type, episode_type, episode_id
order by provider_type, episode_type, episode_id;
quit; 

data a;
set qc.qc_cbd_xwalk;
where exp_unmatch_pct>0;
run;

***;
proc sql;
create table qc_pc2 as
select distinct ppi_type, episode_type, episode_name, provider_type, provider_id, episode_id, index_setting, epi_cnt, sum(total_cost)/epi_cnt as avg_observed
from qc_pc
where description="All" and type in ("er" "readm")
group by ppi_type, provider_type, provider_id, episode_id, index_setting;
quit;

proc sql;
create table provider_pc as
select distinct a.*, coalesce(b.avg_observed, c.avg_observed) as obs_score, round(a.avg_observed-(calculated obs_score),0.01) as obs_diff
from qc_pc2 a
left join qc_setting(where=(measure="t_pc_pay")) b on a.provider_type=b.provider_type and a.provider_id=b.provider_id and a.episode_id=b.episode_id and a.index_setting=b.index_setting
left join qc_epi_spec(where=(measure="t_pc_pay")) c on a.provider_type=c.provider_type and a.provider_id=c.provider_id 
	and ((a.ppi_type^="TCC" and a.episode_id=c.episode_id) or (a.ppi_type="TCC" and a.index_setting=c.episode_id));
quit;   

proc sql;
create table qc.qc_pc as
select distinct ppi_type, provider_type, episode_type, episode_id, episode_name, count(*) as provider_cnt, sum(obs_diff=0)/count(*) as obs_match_pct
from provider_pc
group by ppi_type, provider_type, episode_type, episode_id
order by ppi_type, provider_type, episode_type, episode_id;
quit; 

