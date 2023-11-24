%macro export(prvdr);

*** export epi x spec score ***;
data out.export_&prvdr._epi_spec;
retain py ppi_type provider_type measure state cbsa lvl_1_value spec_desc;
length lvl_1_value $50.;

set epi.r_sc1_&prvdr.: %if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_sc_&prvdr.:(in=t);;
if t then ppi_type="TCC    "; else ppi_type="Episode";

if ppi_score_adj=. then do;
	avg_expected=.;
	avg_oe=.;
	credibility=.;
	avg_oe_adj=.; 
	rank_type="";
end;

if ppi_score_adj=. then ppi_final_score="N/R"; else ppi_final_score=put(ppi_score_adj,3.);
if lvl_1_value="PPI_TCC" then lvl_1_value="All";

rename 
lvl_1_value=episode_id 
spec_desc=specialty;

drop spec_type rpt_lvl_1 del_flag;
run;

proc sort data=out.export_&prvdr._epi_spec; 
by py provider_type measure episode_id specialty cbsa ppi_final_score;
run;


*** export setting report ***;
data out.export_&prvdr._setting;
retain py ppi_type provider_type measure state cbsa def_sub index_setting ;
length provider_type $50. measure $100. index_setting $50.;

set epi.r_s1_&prvdr.:;
ppi_type="Episode";

if index_setting="PHYS" then index_setting="OFFICE";
rename def_sub=episode_id;

if epi_cnt<11 then epi_cnt=.;
drop spec_type credibility avg_oe_adj;
run;

*** export pc report ***;
data out.export_&prvdr._pc;
retain py ppi_type provider_type provider_id state cbsa def_sub type index_setting description epi_cnt visit_cnt obs_cnt_pct exp_cnt_pct total_cost;
length index_setting $50.;

set epi.r_pc_&prvdr.: %if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_pc_&prvdr.:(rename=(frailty_cat=index_setting));;
if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";

if index_setting="PHYS" then do;
	if index(def_sub, "PPI_PR_") then index_setting="OFFICE";
	else index_setting="N/A";
end;

if epi_cnt<11 then epi_cnt=.;
if visit_cnt<11 then visit_cnt=.;

rename 
def_sub=episode_id 
visit_cnt=pc_visit_cnt;
run;

*** export cost breakdown report ***;
data out.export_&prvdr._cbd;
retain py ppi_type provider_type provider_id state cbsa def_sub index_setting phase clm_type svc_cat svc_sub epi_cnt obs_cnt_pct exp_cnt_pct total_cost;
length index_setting $50.;

set epi.r_cbd_&prvdr.: %if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_cbd_&prvdr.:(rename=(frailty_cat=index_setting));;
if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";

if index(def_sub, "TCC") or index(def_sub, "CH_MED") or index(def_sub, "CHEMO") then phase="N/A";
if index_setting="PHYS" and index(def_sub, "PPI_PR_") then index_setting="OFFICE";
else if index_setting="PHYS" then index_setting="N/A";
if epi_cnt<11 then epi_cnt=.;

rename 
def_sub=episode_id 
svc_cat=service_description
svc_sub=service_details;

run;
%mend;


%macro geo(grp, geo_type, geo_name);
proc sql;
create table geo_&geo_type. as
select &py. as py, def_sub, "&geo_type." as geo_type length=10, &geo_name. as geo_name, count(*) as epi_cnt, mean(t_epi_pay) as avg_epi_pay,
	std(t_epi_pay/n_p_t_epi_pay) as cv, mean(t_pc_pay)/mean(t_epi_pay) as pc_pct, mean(index_pay)/mean(epi_pay) as trigger_pct, 
	mean(epi_pay) as avg_episode_cost, mean(phys_pay+dme_pay) as avg_partb_cost, mean(ip_pay) as avg_inpatient_cost, mean(op_pay) as avg_outpatient_cost, 
	mean(snf_pay) as avg_snf_cost, mean(hha_pay) as avg_hha_cost, mean(hsp_pay) as avg_hospice_cost, mean(rx_pay) as avg_rx_cost
from epi_all
group by def_sub &grp.;
quit;
%mend;
%macro npi_xwalk(type);
proc sql;
create table npi_x_&type. as
select &py. as py, attr_npi, state, cbsa, "&type." as xwalk_type length=15, def_sub, &type. as xwalk_provider, count(*) as epi_cnt
from epi_all
group by state, cbsa, attr_npi, &type., def_sub
union corr
select &py. as py, attr_npi, state, cbsa, "&type." as xwalk_type length=15, "All" as def_sub, &type. as xwalk_provider, count(*) as epi_cnt
from epi_all
group by state, cbsa, attr_npi, &type.
order by state, cbsa, attr_npi, xwalk_type, def_sub, epi_cnt desc;
quit;
%mend;
%macro pcp_spec(type);
proc sql;
create table pcp_spec_&type. as
select distinct &py. as py, "&type." as provider_type length=15, pcp_&type. as pcp_id, %if &type.=npi %then pcp_spec; %else "  "; as pcp_specialty,  
				spec_&type. as specialist_id, %if &type.=npi %then spec_spec; %else "  "; as spec_specialty, "All" as def_sub length=30, count(*) as epi_cnt
from pcp_spec
group by pcp_&type., spec_&type.
union corr
select distinct &py. as py, "&type." as provider_type length=15, pcp_&type. as pcp_id, %if &type.=npi %then pcp_spec; %else "  "; as pcp_specialty,  
				spec_&type. as specialist_id, %if &type.=npi %then spec_spec; %else "  "; as spec_specialty, def_sub length=30, count(*) as epi_cnt
from pcp_spec
group by pcp_&type., spec_&type., def_sub
order by provider_type, pcp_id, specialist_id, def_sub, epi_cnt desc;
quit;
%mend;

%macro export_epi_var;

data epi_all ; 
set epi.bene_p_ppi_: tcc.bene_p_ppi_tcc;
keep bene_id year def_id def_sub county state cbsa t_epi_pay n_p_t_epi_pay t_pc_pay index_pay epi_pay phys_pay dme_pay ip_pay op_pay snf_pay hha_pay hsp_pay rx_pay attr_: spec_cd;
county=strip(state)||", "||strip(county);
run;

*** export epi variation report ***;

%geo(%str(, cbsa), CBSA, cbsa);
%geo(%str(, county), County, county);
%geo(%str(, state), State, state);
%geo(%str(), Nation, "Nation");

data out.export_epi_var;
retain py ppi_type;
set geo_:;

if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";
if epi_cnt<11 then epi_cnt=.;

rename def_sub=episode_id;
run;

*** export NPI xwalk report ***;

%npi_xwalk(attr_tin);
%npi_xwalk(attr_p_bill);
%npi_xwalk(attr_f_bill);
%npi_xwalk(attr_ccn);

data out.export_npi_xwalk;
retain py ppi_type;
set npi_x_:;
where xwalk_provider^="";

if def_sub="All" then ppi_type="All    ";
else do;
	if def_sub="PPI_TCC" then ppi_type="TCC    "; 
	else ppi_type="Episode";
end;

if epi_cnt<11 then epi_cnt=.;

rename def_sub=episode_id;
run;

*** export PCP-Specialist xwalk report ***;

proc sql;
create table pcp_spec as
select a.bene_id, a.spec_cd as pcp_spec, a.attr_npi as pcp_attr_npi, a.attr_tin as pcp_attr_tin, a.attr_p_bill as pcp_attr_p_bill, 
	b.def_sub, b.def_id, b.year, b.spec_cd as spec_spec, b.attr_npi as spec_attr_npi, b.attr_tin as spec_attr_tin, b.attr_p_bill as spec_attr_p_bill
from tcc.bene_p_ppi_tcc a
left join epi_all(where=(def_sub^="PPI_TCC")) b
on a.bene_id=b.bene_id
order by bene_id, pcp_attr_npi, spec_attr_npi, year;
quit;

%pcp_spec(attr_npi);
%pcp_spec(attr_tin);
%pcp_spec(attr_p_bill);

data out.export_pcp_spec_xwalk;
set pcp_spec_:;

if epi_cnt<11 then epi_cnt=.;

rename def_sub=episode_id;
run;

%mend;


%macro char_to_num(tbl, var);
proc sql;
create table &tbl._&var. as
select distinct &var. from out.export_attr_npi_&tbl.
union corr
select distinct &var. from out.export_attr_tin_&tbl.
union corr
select distinct &var. from out.export_attr_p_bill_&tbl.
union corr
select distinct &var. from out.export_attr_f_bill_&tbl.
union corr
select distinct &var. from out.export_attr_ccn_&tbl.;
quit;
proc sort data=&tbl._&var. nodupkey; by &var.; run;

data value_&tbl._&var.;
set &tbl._&var.;
retain num;
num+1;
length var $30. char $100.;
var="&var.";
char=&var.;
drop &var.;
run;
%mend;

%macro export_num(tbl);

data out.var_value_&tbl.; set value_&tbl._:; run;

proc sql; select distinct var into: varlist separated by " " from out.var_value_&tbl.; quit;

%do i=1 %to &sqlobs.;
%let curr_var=%scan(&varlist.,&i.);

%macro prvdr(prvdr);
proc sql;
create table out.export_&prvdr._&tbl. as
select a.*, b.num as &curr_var._code
from out.export_&prvdr._&tbl. a
left join out.var_value_&tbl. b
on a.&curr_var.=b.char and b.var="&curr_var.";
quit;
data out.export_&prvdr._&tbl.; set out.export_&prvdr._&tbl.; drop &curr_var.; run;
%mend;

%prvdr(attr_npi);
%prvdr(attr_tin);
%prvdr(attr_p_bill);
%prvdr(attr_f_bill);
%prvdr(attr_ccn);
%end;

%mend;

