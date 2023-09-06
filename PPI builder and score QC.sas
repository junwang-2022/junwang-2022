
%let py=2021;

%let agg_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\agg;  
%let builder_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\builder_output;
%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta v10;  
%let py_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\agg\py&py.;
%let tcc_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\tcc\py&py.;

%let qc_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\QC\py&py.;

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

libname out "&py_loc.";
libname tcc "&tcc_loc.";

libname qc "&qc_loc.";
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";


********************************;
*** buildr output and agg QC ***;
********************************;

*** epi count drop ***;
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
%epi_all(2);
%epi_all(3);
data qc_all_epi_all; set epi_all_y1-epi_all_y3; run;
proc datasets lib=work noprint; delete epi_all_y:; run;

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
if index(def_sub,"PPI_CH_MED") then do; 
	epi_pay=epi_pay*month; t_epi_pay=t_epi_pay*month; t_pc_pay=t_pc_pay*month; 
	readm_cnt=readm_cnt*month; er_cnt=er_cnt*month; ins_los=ins_los*month;
end;
run;
%mend;
%trigger_all(1);
%trigger_all(2);
%trigger_all(3);
data qc_all_agg_trigger; set trigger_all_y1-trigger_all_y3; run;
proc datasets lib=work noprint; delete trigger_all_y:; run;


%macro bene_all(year);
data bene_all_y&year.;
set ay&year..bene_ppi_:;
if index(def_sub,"PPI_CH_MED") then do; 
	epi_pay=epi_pay*month; t_epi_pay=t_epi_pay*month; t_pc_pay=t_pc_pay*month; 
	readm_cnt=readm_cnt*month; er_cnt=er_cnt*month; ins_los=ins_los*month;
end;
run;
%mend;
%bene_all(1);
%bene_all(2);
%bene_all(3);
data qc_all_agg_bene; set bene_all_y1-bene_all_y3; run;
proc datasets lib=work noprint; delete bene_all_y:; run;


proc sql;
create table epi_cnt as
select def_name, def_sub, count(*) as epi_cnt
from qc_all_epi_all
group by def_name, def_sub;
quit;

proc  sql;
create table qc.qc_epi_drop as
select distinct b.epi_type, b.clin_cat, a.def_name, a.def_sub, b.def_desc, a.epi_cnt, coalesce(b.epi_cnt, x1.epi_cnt) as agg_cnt, coalesce(c.epi_cnt, x2.epi_cnt) as attr_cnt,
	(calculated agg_cnt)/a.epi_cnt as agg_pct, (calculated attr_cnt)/a.epi_cnt as attr_pct
from epi_cnt a
left join (select clin_cat, epi_type, def_sub, def_name, def_desc, count(*) as epi_cnt from qc_all_agg_trigger group by clin_cat, epi_type, def_sub, def_name, def_desc) b
on a.def_name=b.def_name and a.def_sub=b.def_sub
left join (select clin_cat, epi_type, def_sub, def_name, def_desc, count(*) as epi_cnt from qc_all_agg_bene group by clin_cat, epi_type, def_sub, def_name, def_desc) c
on a.def_name=c.def_name and a.def_sub=c.def_sub
left join (select def_name, count(*) as epi_cnt from qc_all_agg_trigger where def_name="PPI_CHEMO" group by def_name) x1 on a.def_name=x1.def_name
left join (select def_name, count(*) as epi_cnt from qc_all_agg_bene where def_name="PPI_CHEMO" group by def_name) x2 on a.def_name=x2.def_name
order by epi_type, clin_cat, def_sub, def_desc;
quit;


*** episode summary ***;
proc sql;
create table qc.qc_epi_summary as
select distinct clin_cat, epi_type, def_sub, def_desc, index_setting, count(distinct def_id) as n, 
	mean(t_epi_pay) as epi_mean, std(t_epi_pay)/mean(t_epi_pay) as cv, 
	mean(index_pay) as index_mean, mean(post_pay) as post_mean,
	mean(readm_cnt)*1000 as readm_1000, mean(er_cnt)*1000 as er_1000,
	sum(readm_cost)/sum(epi_pay) as readm_pct, sum(er_cost)/sum(epi_pay) as er_pct, sum(pc_cost)/sum(epi_pay) as pc_pct, 
	sum(readm_cost+er_cost+pc_cost)/sum(epi_pay) as all_avoid_pct
from qc_all_agg_bene
group by clin_cat, epi_type, def_sub, def_desc, index_setting
order by epi_type, def_sub, def_desc;
quit;


*** specialty summary ***;
proc sql;
create table qc_attr_spec as
select clin_cat, epi_type, def_sub, def_desc, spec_cd, count(*) as cnt, mean(t_epi_pay) as t_epi_pay
from qc_all_agg_bene
group by clin_cat, epi_type, def_sub, def_desc, spec_cd
order by clin_cat, epi_type, def_desc, cnt desc;
quit;

proc sql;
create table qc.qc_spec_summary as
select distinct *, cnt/sum(cnt) as pct
from (select a.*, b.spec_desc from qc_attr_spec a left join meta2.cms_spec_cd b on a.spec_cd=b.spec_cd)
group by clin_cat, epi_type, def_desc
order by clin_cat, epi_type, def_desc, cnt desc;
quit;


*** provider summary ***;
proc sql;
create table npi_summary as
select clin_cat, epi_type, def_desc, attr_npi, count(*) as cnt
from qc_all_agg_bene
group by clin_cat, epi_type, def_desc, attr_npi
order by clin_cat, epi_type, def_desc, cnt desc;
quit;

proc sql;
create table qc.qc_npi_coverage as
select distinct clin_cat, epi_type, def_desc, 
	count(*) as npi_cnt, sum(cnt>10) as covered_cnt10, sum(cnt>10)/count(*) as covered_npi_pct10, 
	sum(cnt>5) as covered_cnt5, sum(cnt>5)/count(*) as covered_npi_pct5,
	sum(cnt) as epi_cnt, sum(cnt*(cnt>10)) as covered_epi_cnt10, sum(cnt*(cnt>10))/sum(cnt) as covered_epi_pct10,
	sum(cnt*(cnt>5)) as covered_epi_cnt5, sum(cnt*(cnt>5))/sum(cnt) as covered_epi_pct5
from npi_summary
group by clin_cat, epi_type, def_desc
order by clin_cat, epi_type, def_desc;
quit;


***********************;
*** score output QC ***;
***********************;

*** provider coverage ***;

data qc_qc_all_epi;
set 
out.bene_p_ppi_:
tcc.bene_p_ppi_tcc
;
run;

%macro qc_by_provider(prvdr);

data qc_score_&prvdr.;
set 
out.r_sc1_&prvdr._:
%if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_sc_&prvdr._:;
;
run;

*** check episode count ***;
proc sql;
create table qc_score_summary_&prvdr. as
select distinct "EPI" as type, measure, epi_type, lvl_1_value, def_desc, provider_type,
	count(distinct provider_id) as provider_cnt, sum(rank_type="CBSA")/count(*) as cbsa_pct, sum(epi_cnt) as epi_cnt,
	sum(ppi_score_adj=.)/count(*) as missing_pct,
 	min(avg_observed) as min_obs, min(avg_expected) as min_exp,
	max(avg_observed) as max_obs, max(avg_expected) as max_exp,
	round(mean(avg_oe_adj),0.01) as mean_oe_adj, 
	round(min(avg_oe_adj),0.01) as min_oe_adj, 
	round(max(avg_oe_adj),0.01) as max_oe_adj,
	sum(avg_oe_adj>10) as high_oe_cnt, 
	sum(avg_oe_adj>10)/count(*) as high_oe_pct,
	sum((avg_oe_adj<1)*(ppi_score_adj=1)) as low_oe_cnt, 
	sum((avg_oe_adj<1)*(ppi_score_adj=1))/sum(ppi_score_adj=1) as low_oe_pct
from qc_score_&prvdr.
group by measure, epi_type, lvl_1_value, def_desc, provider_type
union corr
select distinct "All" as type, measure, epi_type, "All" as lvl_1_value, "All" as def_desc, provider_type,
	count(distinct provider_id) as provider_cnt, sum(rank_type="CBSA")/count(*) as cbsa_pct, sum(epi_cnt) as epi_cnt,
	sum(ppi_score_adj=.)/count(*) as missing_pct,
	min(avg_observed) as min_obs, min(avg_expected) as min_exp,
	max(avg_observed) as max_obs, max(avg_expected) as max_exp,
 	round(mean(avg_oe_adj),0.01) as mean_oe_adj, 
	round(min(avg_oe_adj),0.01) as min_oe_adj, 
	round(max(avg_oe_adj),0.01) as max_oe_adj,
	sum(avg_oe_adj>10) as high_oe_cnt, 
	sum(avg_oe_adj>10)/count(*) as high_oe_pct,
	sum((avg_oe_adj<1)*(ppi_score_adj=1)) as low_oe_cnt, 
	sum((avg_oe_adj<1)*(ppi_score_adj=1))/sum(ppi_score_adj=1) as low_oe_pct
from qc_score_&prvdr.
group by measure, epi_type, provider_type
order by type, measure, epi_type, provider_type, lvl_1_value;
quit;

proc sql;
create table qc_epi_covered_&prvdr. as
select distinct "EPI" as type, a.epi_type, a.def_sub, a.def_desc, a.epi_cnt, a.&prvdr._cnt, 
	b.epi_cnt as cov_&prvdr._epi_cnt, b.&prvdr._cnt as cov_&prvdr._cnt, 
	b.epi_cnt/a.epi_cnt as cov_&prvdr._epi_pct, b.&prvdr._cnt/a.&prvdr._cnt as cov_&prvdr._pct
from 
	(select distinct epi_type, def_sub, def_desc, count(*) as epi_cnt, count(distinct &prvdr.) as &prvdr._cnt
	from qc_qc_all_epi
	group by epi_type, def_sub, def_desc) a
left join (select distinct epi_type, lvl_1_value, def_desc, sum(epi_cnt) as epi_cnt, count(distinct provider_id) as &prvdr._cnt
	from qc_score_&prvdr.
	where measure="t_epi_pay" 
	group by epi_type, lvl_1_value, def_desc) b
on a.epi_type=b.epi_type and a.def_sub=b.lvl_1_value
union corr
select distinct "All" as type, a.epi_type, "All" as def_sub, "All" as def_desc, a.epi_cnt, a.&prvdr._cnt, 
	b.epi_cnt as cov_&prvdr._epi_cnt, b.&prvdr._cnt as cov_&prvdr._cnt, 
	b.epi_cnt/a.epi_cnt as cov_&prvdr._epi_pct, b.&prvdr._cnt/a.&prvdr._cnt as cov_&prvdr._pct
from 
	(select distinct epi_type, count(*) as epi_cnt, count(distinct &prvdr.) as &prvdr._cnt
	from qc_qc_all_epi
	group by epi_type) a
left join (select distinct epi_type, sum(epi_cnt) as epi_cnt, count(distinct provider_id) as &prvdr._cnt
	from qc_score_&prvdr.
	where measure="t_epi_pay" 
	group by epi_type) b
on a.epi_type=b.epi_type 
order by type, epi_type, def_sub;
quit;

%mend;

%qc_by_provider(attr_npi);
%qc_by_provider(attr_tin);
%qc_by_provider(attr_p_bill);
%qc_by_provider(attr_f_bill);
%qc_by_provider(attr_ccn);


data qc.qc_all_score_summary;
set qc_score_summary_:;
run;

data qc.qc_epi_all_covered;
merge qc_epi_covered_:;
by type epi_type def_sub;
run;

********************************;
proc sql;
create table qc_npi_spec as
select a.*, b.spec_desc, a.npi=c.provider_id as epi_covered, a.npi=d.provider_id as tcc_covered
from meta2.npi_x_spec_&py. a
left join meta2.cms_spec_cd b on a.spec_cd=b.spec_cd
left join (select distinct provider_id from qc_score_attr_npi where epi_type^="TCC") c on a.npi=c.provider_id
left join (select distinct provider_id from qc_score_attr_npi where epi_type="TCC") d on a.npi=d.provider_id;
quit;

proc sql;
create table qc.qc_npi_spec_covered as
select distinct spec_cd, spec_desc, count(*) as npi_cnt, 
	sum(epi_covered) as epi_npi_cnt,  sum(epi_covered)/count(*) as epi_npi_pct,
	sum(tcc_covered) as tcc_npi_cnt,  sum(tcc_covered)/count(*) as tcc_npi_pct,
	sum(epi_covered=1 or tcc_covered=1) as any_npi_cnt, sum(epi_covered=1 or tcc_covered=1)/count(*) as any_npi_pct
from qc_npi_spec
group by spec_cd, spec_desc
order by spec_cd, spec_desc;
quit;

***********************;
*** export files QC ***;
***********************;

%macro dup(prvdr);
proc sort data=out.export_&prvdr._epi_spec out=a1 dupout=b1 nodupkey;
by provider_id measure episode_id specialty;
run;
proc freq data=b1; tables provider_type*episode_type; run;

/*proc sql;*/
/*create table cbd_&prvdr. as*/
/*select distinct a.*, b.avg_observed as obs_score, b.avg_expected as exp_score, b.avg_oe as oe_score, */
/*	a.avg_observed-b.avg_observed as obs_diff, abs(round(a.avg_expected-b.avg_expected,1)) as exp_diff*/
/*from out.export_&prvdr._cost_bd(where=(phase in("All" "N/A") and clm_type="All" and service_description="All")) a*/
/*left join out.export_&prvdr._epi_spec(where=(measure in ("Average Episode Payment" "Average PMPY Payment"))) b*/
/*on a.provider_id=b.provider_id and a.episode_id=b.episode_id;*/
/*quit; */
/**/
/*proc sql;*/
/*create table cbd_xwalk_&prvdr. as*/
/*select distinct provider_type, episode_type, episode_id, count(*) as cnt, sum(exp_diff>0)/count(*) as unmatch_pct*/
/*from cbd_xwalk_&prvdr.*/
/*group by provider_type, episode_type, episode_id*/
/*order by provider_type, episode_type, episode_id;*/
/*quit; */

proc sql;
create table pc_&prvdr. as
select distinct a.*, b.avg_observed as obs_score, b.avg_expected as exp_score, b.avg_oe as oe_score, 
	a.avg_observed-b.avg_observed as obs_diff, abs(round(a.avg_expected-b.avg_expected,1)) as exp_diff
from out.export_&prvdr._pc(where=(description="All")) a
left join out.export_&prvdr._epi_spec(where=(measure in ("Quality Improvement by Episode" "Quality Improvement by Population"))) b
on a.provider_id=b.provider_id and a.episode_id=b.episode_id
order by episode_id, provider_id, index_setting, pc_type;
quit;  

proc sql;
create table pc_xwalk_&prvdr. as
select distinct provider_type, episode_type, episode_id, count(*) as cnt, sum(.<exp_diff<0)/count(*) as unmatch_pct
from pc_&prvdr.
group by provider_type, episode_type, episode_id
order by provider_type, episode_type, episode_id;
quit; 
%mend;

%dup(attr_npi);
%dup(attr_tin);
%dup(attr_p_bill);
%dup(attr_f_bill);
%dup(attr_ccn);

data qc.cbd_xwalk; set cbd_xwalk_:; run;
data qc.pc_xwalk; set pc_xwalk_:; run;
