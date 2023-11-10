%macro desc;
if provider_type="attr_npi" then provider_type="Physician NPI";
if provider_type="attr_tin" then provider_type="Physician TIN";
if provider_type="attr_p_bill" then provider_type="Physician Billing NPI";
if provider_type="attr_f_bill" then provider_type="Facility Billing NPI";
if provider_type="attr_ccn" then provider_type="Facility CCN";

if ppi_type="TCC" then do;
if measure="death" then measure="Mortality Rate";
if measure="er_cnt" then measure="ER Visits per 1,000 Person Years";
if measure="readm_cnt" then measure="Unplanned Hospitalizations per 1,000 Person Years";
if measure="ins_los" then measure="Average LOS (Days) of Institutional Long-term Stay";
if measure="t_epi_pay" then measure="Average PMPY Payment";
if measure="t_pc_pay" then measure="Quality Improvement by Population";
end;
else do;
if measure="death" then measure="Mortality Rate";
if measure="er_cnt" then measure="ER Visits per 1,000 Episodes";
if measure="readm_cnt" then measure="Unplanned Readmissions per 1,000 Episodes";
if measure="ins_los" then measure="Average LOS (Days) of Institutional Long-term Stay";
if measure="t_epi_pay" then measure="Average Episode Payment";
if measure="t_pc_pay" then measure="Quality Improvement by Episode";
end;
%mend;

%macro epi_report(tbl);
proc sql;
create table export_&prvdr._&tbl. as
select a.*, b.episode_name, b.clinical_category, b.episode_type
from in.export_&prvdr._&tbl. a
left join meta2.ppi_name_mapping b
on a.episode_id=b.def_sub or (a.ppi_type="TCC" and b.episode_type="TCC");
quit;

%if &tbl.=COST_BD or &tbl.=PC %then %do;
	data out.export_&prvdr._&tbl.;
	length provider_type $50. ;
	retain py ppi_type provider_type episode_type clinical_category episode_id episode_name state cbsa pc_type;
	set export_&prvdr._&tbl.;

	if provider_type="attr_npi" then provider_type="Physician NPI";
	if provider_type="attr_tin" then provider_type="Physician TIN";
	if provider_type="attr_p_bill" then provider_type="Physician Billing NPI";
	if provider_type="attr_f_bill" then provider_type="Facility Billing NPI";
	if provider_type="attr_ccn" then provider_type="Facility CCN";

	%if &tbl.=PC %then %do;
		length pc_type $100.;
		if type="er" then pc_type="ER Visits";
		if type="pc" then pc_type="Potentially Preventable Complications";
		if type="readm" then do;
			if ppi_type="TCC" then pc_type="Unplanned Hospitalizations";
			else pc_type="Unplanned Readmissions";
		end;
		drop type;
	%end;
	run;

%end;

%else %do;
	data out.export_&prvdr._&tbl.;
	length provider_type $50. measure $100.;
	retain py provider_type episode_type clinical_category episode_id episode_name;
	set export_&prvdr._&tbl.;
	%desc;
	run;
%end;

proc sort data=out.export_&prvdr._&tbl.; 
by py ppi_type provider_type episode_type clinical_category episode_id episode_name cbsa;
run;
%mend;



%macro comb_rank;

proc sql;
create table r_comb_&prvdr. as
select py, ppi_type, state, cbsa, provider_type, provider_id, measure, "Episode ID" as rollup_type length=30, episode_id as rollup_description, episode_name as rollup_description2, sum(epi_cnt) as epi_cnt, 
	sum(ppi_score_adj*epi_cnt)/sum((ppi_score_adj^=.)*epi_cnt) as mean_score_adj
from out.export_&prvdr._epi_spec
group by py, ppi_type, state, cbsa, provider_type, provider_id, measure, episode_id, episode_name
union corr
select py, ppi_type, state, cbsa, provider_type, provider_id, measure, "Clinical Category" as rollup_type, clinical_category as rollup_description, "" as rollup_description2, sum(epi_cnt) as epi_cnt, 
	sum(ppi_score_adj*epi_cnt)/sum((ppi_score_adj^=.)*epi_cnt) as mean_score_adj
from out.export_&prvdr._epi_spec
where ppi_type^="TCC"
group by py, ppi_type, state, cbsa, provider_type, provider_id, measure, clinical_category
union corr
select py, ppi_type, state, cbsa, provider_type, provider_id, measure, "Physician Specialty" as rollup_type, specialty as rollup_description, "" as rollup_description2, sum(epi_cnt) as epi_cnt, 
	sum(ppi_score_adj*epi_cnt)/sum((ppi_score_adj^=.)*epi_cnt) as mean_score_adj
from out.export_&prvdr._epi_spec
group by py, ppi_type, state, cbsa, provider_type, provider_id, measure, specialty
union corr
select py, ppi_type, state, cbsa, provider_type, provider_id, measure, "All" as rollup_type, "All" as rollup_description, "" as rollup_description2, sum(epi_cnt) as epi_cnt, 
	sum(ppi_score_adj*epi_cnt)/sum((ppi_score_adj^=.)*epi_cnt) as mean_score_adj
from out.export_&prvdr._epi_spec
where ppi_type^="TCC"
group by py, ppi_type, state, cbsa, provider_type, provider_id, measure
order by py, ppi_type, state, cbsa, provider_type, measure, rollup_type, rollup_description;
quit;

proc rank data=r_comb_&prvdr. out=out.export_&prvdr._comb group=5;
by py ppi_type cbsa provider_type measure rollup_type rollup_description;
var mean_score_adj;
ranks comb_rank;
run;

data out.export_&prvdr._comb; 
set out.export_&prvdr._comb; 
if ppi_type="TCC" and rollup_type="Episode ID" then rollup_type="Frailty Category";

ppi_score_adj=comb_rank+1; 
if ppi_score_adj=. then ppi_final_score="N/R"; 
else ppi_final_score=put(ppi_score_adj,3.);

drop comb_rank; 
run;
proc sort data=out.export_&prvdr._comb; by py ppi_type state cbsa provider_type measure rollup_type rollup_description ppi_score_adj mean_score_adj; run;
%mend comb_rank;

%macro report_var;
proc sql;
create table out.export_epi_var as
select a.*, b.episode_name, b.clinical_category, b.episode_type
from in.export_epi_var a
left join meta2.ppi_name_mapping b
on a.episode_id=b.def_sub or (a.ppi_type="TCC" and b.episode_type="TCC")
order by py, ppi_type, episode_type, clinical_category, episode_id, geo_type;
quit;

data out.export_epi_var;
retain py ppi_type episode_type clinical_category episode_id episode_name geo_type geo_name;
set out.export_epi_var;
run;
%mend;



