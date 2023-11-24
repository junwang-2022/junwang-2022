*********************************************************************************;
*** ad hoc steps to create cost breakdown by specialty report using PPI 2022 ****;
*********************************************************************************;

/* run STEP 1 & 2 in each year separately */

/* step 1: re-pull deleted serviceline claims from CCM for each type of episode and TCC */
	/*1.1 use bene_id and clm_id from "epi_clm_&epi." table to pull records from CCM serviceline table. 
		  Only include def_sub that are in the final report (we dropped 35+10) to reduce the data size.
		  Only pull bene_id, clm_id, syskey, cpt, and allowed_amt */
	/*1.2 add def_id, def_sub from epi_clm_&epi. to pulled serviceline records. */
	/*1.3 save "&epi._serviceline" tables in library IN (builder output in normal PPI run)*/

/* step 2: run the revised %cost_bd_by_specialty below to generate "epi_cbd_&epi." table for both epi and TCC */
	/*for TCC add %let year=&py.;*/

%macro cost_bd_by_specialty;

proc sql; /* service line level breakdown for physician and OP claims */
create table svc_&epi. as
select a.*, b.phase, b.clm_type, %if &epi. = PPI_CH_MED or &epi. = PPI_CHEMO or &epi. = PPI_PERINATAL %then b.def_sub,; b.episode_in_rank
from in.&epi._serviceline(drop=clm_type %if &epi. = PPI_CH_MED or &epi. = PPI_CHEMO or &epi. = PPI_PERINATAL %then def_sub;) a, out.epi_clm_&epi. b
where b.clm_type in ("OP" "PHYS" "ASC" "DME") %if &epi. ne PPI_CH_MED and &epi. ne PPI_CHEMO and &epi. ne PPI_PERINATAL %then and a.def_sub=b.def_sub;
	and a.def_id=b.def_id and a.syskey=b.syskey;
quit;

proc sql;
create table svc_&epi. as
select a.*, b.rbcs_cat_desc, b.rbcs_subcat_desc, b.svc_desc, b.cpt_desc/*, c.svc_cat as cat_cpt*/
from svc_&epi. a
left join meta.cpt b on a.cpt=b.cpt
;
quit;

data svc_&epi.;
set svc_&epi.;

length svc_cat $100. svc_sub $100.;

if rbcs_cat_desc^="" then svc_cat=strip(rbcs_cat_desc)||" - "||strip(rbcs_subcat_desc);
else svc_cat="";
if rbcs_subcat_desc="Injections and Infusions (nononcologic)" then do;
	if strip(svc_desc)="No RBCS Family" then svc_desc="Injections and Infusions - Others";
	svc_cat="Treatment - "||strip(svc_desc);
end;

svc_sub=strip(svc_desc);
if strip(rbcs_subcat_desc)="Injections and Infusions (nononcologic)" then do;
	if svc_cat in ( "Treatment - Injection Administration"
					"Treatment - Intravenous Infusion, Hydration"
					"Treatment - Injections and Infusions - Others"
					"Treatment - Vaccine - Toxoids"
					"Treatment - Vaccine Admin - Flu, Pneum, and Hep B") 
	then svc_sub="All";
	else svc_sub=strip(cpt)||" - "||strip(cpt_desc);
end;
if strip(svc_desc)="Chemotherapeutic Agent" then svc_sub=strip(cpt)||" - "||strip(cpt_desc);

if svc_cat="" and allowed_amt<=0 then delete;

drop rbcs_cat_desc rbcs_subcat_desc episode_in_rank cpt_desc;
run;

%if &epi.=PPI_TCC %then %do;
proc sql;
create table svc3_&epi. as
select def_sub, def_id, phase, clm_type, svc_cat, "All" as svc_sub length=100, sum(allowed_amt) as tot_amt
from svc_&epi.
group by def_sub, def_id, phase, clm_type, svc_cat
order by def_sub, def_id, phase, clm_type, svc_cat;
quit;
%end;
%else %do;
proc sql;
create table svc2_&epi. as
select a.*, b.sub_f3
from svc_&epi. a
left join meta.def_list b on a.def_sub=b.def_sub;
quit;

data svc2_&epi.;
set svc2_&epi.;
length clin_cat $50.;
if sub_f3="A" then clin_cat="Infectious diseases";
if sub_f3="B" then clin_cat="Blood diseases";
if sub_f3="C" then clin_cat="Endocrine/metabolic diseases";
if sub_f3="D" then clin_cat="Mental health";
if sub_f3="E" then clin_cat="Nervous system diseases";
if sub_f3="F" then clin_cat="Eye and adnexa diseases";
if sub_f3="G" then clin_cat="Ear and mastoid diseases";
if sub_f3="H" then clin_cat="Circulatory system diseases";
if sub_f3="I" then clin_cat="Respiratory system diseases";
if sub_f3="J" then clin_cat="GI system diseases";
if sub_f3="K" then clin_cat="Skin/subcutaneous diseases";
if sub_f3="L" then clin_cat="Musculoskeletal system diseases";
if sub_f3="M" then clin_cat="GU system diseases";
if sub_f3="XXX" then clin_cat="Injury";

%if &epi.=PPI_CHEMO or &epi.=PPI_RO %then %do; clin_cat="Cancer"; %end;
%if &epi.=PPI_PERINATAL %then %do; clin_cat="Perinatal care"; %end;
run;

proc sql;
create table svc2_&epi. as
select a.*, c.spec_details, coalescec(strip(c.spec_details), strip(a.svc_sub)) as svc_sub2
from svc2_&epi. a
left join meta2.cbd_spec_mapping c on a.clin_cat=strip(c.clin_cat) and a.svc_cat=strip(c.service_description) and a.svc_sub=strip(c.service_details);
quit;

data svc2_&epi.;
set svc2_&epi.;
if index(def_sub, "PROC") or index(def_sub, "MJRLE") or index(def_sub, "PERINATAL") then do;
	if svc_cat="Procedure - Skin" then do;
		if svc_sub2="All" and svc_sub in ("Debridement" "Incision and drainage, skin and subcutaneous tissue" "Skin Grafting" "Wound Repair - All Levels") then svc_sub2=svc_sub;
		else if svc_sub2="All" then svc_sub2="Others";
	end;
	if svc_cat="Treatment - Treatment - Miscellaneous" then do;
		if svc_sub2="All" and svc_sub="Hyperbaric Oxygen" then svc_sub2=svc_sub;
		else if svc_sub2="All" then svc_sub2="Others";
	end;
end;
run;

proc sql;
create table svc3_&epi. as
select def_sub, def_id, phase, clm_type, svc_cat, svc_sub2 as svc_sub length=100, sum(allowed_amt) as tot_amt
from svc2_&epi.
group by def_sub, def_id, phase, clm_type, svc_cat, svc_sub2
order by def_sub, def_id, phase, clm_type, svc_cat, svc_sub;
quit;
%end;

/* claim level breakdown by setting */
proc sql; 
create table clm_type_&epi. as
select distinct def_sub, def_id, phase, clm_type, . as ipa, . as ipl, . as readm, "Total" as svc_cat length=60, sum(ppi_amt) as tot_amt
from out.epi_clm_&epi.
group by def_sub, def_id, phase, clm_type
union corr
select distinct def_sub, def_id, phase, clm_type, 
	setting in ("IPA" "IPF" "CAH") as ipa, setting in ("IRF" "LTCH") as ipl, readm, "" as svc_cat, sum(ppi_amt) as tot_amt
from out.epi_clm_&epi.
where clm_type="IP" and phase="POST"
group by def_sub, def_id, phase, clm_type, setting in ("IPA" "IPF" "CAH"), setting in ("IRF" "LTCH"), readm
order by def_sub, def_id, phase, clm_type, ipa, ipl, readm;
quit;

data clm_type_&epi.;
set clm_type_&epi.;
if ipa=1 then do;
	if readm=1 then svc_cat="Unplanned Readmission";
	else if phase="POST" then svc_cat="Planned Readmission";
end;
if ipl=1 then svc_cat="IRF/LTCH";
svc_sub="All";
drop ipa ipl readm;
run;

/* Part D drug breakdown */

/*%if &epi.=PPI_CHEMO %then %do;*/
/*proc sql; */
/*create table rx_svc_&epi. as*/
/*select a.*, b.gnn*/
/*from out.rx_&epi. a*/
/*left join meta.ndc b on a.ndc=b.ndc;*/
/*quit;*/
/**/
/*data rx_svc_&epi.; */
/*set rx_svc_&epi.;*/
/*if gnn="" then gnn="Others";*/
/*if episode_in_rank="EPISODE_IN_09" then svc_cat="Part D Drugs - Chemotherapy";*/
/*if episode_in_rank="EPISODE_IN_16" then svc_cat="Part D Drugs - Other";*/
/*svc_sub=gnn;*/
/*run;*/
/**/
/*proc sql; */
/*create table rx_svc2_&epi. as*/
/*select def_sub, def_id, phase, clm_type, svc_cat, svc_sub, sum(ppi_amt) as ppi_amt*/
/*from rx_svc_&epi.*/
/*group by def_sub, def_id, phase, clm_type, svc_cat, svc_sub*/
/*union corr*/
/*select def_sub, def_id, phase, clm_type, "Total" as svc_cat length=60, "" as svc_sub, sum(ppi_amt) as ppi_amt*/
/*from rx_svc_&epi.*/
/*group by def_sub, def_id, phase, clm_type*/
/*order by def_sub, def_id, phase, clm_type, svc_cat, svc_sub;*/
/*quit;*/
/*%end;*/

data out.epi_cbd_&epi.;
set svc3_&epi. clm_type_&epi. /*%if &epi.=PPI_CHEMO %then %do; rx_svc2_&epi. %end;*/;
year=&year.;
if svc_cat="" then svc_cat="Others";
if svc_sub="" then svc_sub="All";
run;
proc sort data=out.epi_cbd_&epi.; by def_sub def_id phase clm_type svc_cat svc_sub; run;
%mend;

/* step 3: run %combine_pc_cbd below to combine 3-year epi_cbd_&epi. */
%macro combine_pc_cbd(type);
data epi_&type._&epi.;
set y1.epi_&type._&epi.
%if &epi. ne PPI_CHEMO and &epi. ne PPI_PERINATAL %then y2.epi_&type._&epi. y3.epi_&type._&epi.;
;
run;

proc sql;;
create table out.epi_&type._&epi. as
select a.*
from epi_&type._&epi. a, out.bene_p_&epi. b
where a.def_sub=b.def_sub and a.def_id=b.def_id and a.year=b.year;
quit;
%mend;
%combine_pc_cbd(cbd);


/* step 4: run 2.1. PPI PY Score v11.sas, but only %map_cbsa and %add_benchmark steps like below */
/* for epi */
%macro score(prvdr);
	%map_cbsa;
	%add_benchmark(cbd, index_setting, phase, clm_type, svc_cat, svc_sub);
%mend;

%score(attr_npi);
%score(attr_tin);
%score(attr_p_bill);
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_TCC and &epi. ne PPI_RO %then %do;
	%score(attr_f_bill);
	%score(attr_ccn);
%end;

	/* for TCC */
%macro score(prvdr);
%map_cbsa;
%add_benchmark(cbd, frailty_cat, phase, clm_type, svc_cat, svc_sub);
%mend;

%score(attr_npi);
%score(attr_tin);
%score(attr_p_bill);


/* step 5: run 3. PPI Export v11.sas, but only %export below */
%macro export(prvdr);
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
%export(attr_npi);
%export(attr_tin);
%export(attr_p_bill);
%export(attr_f_bill);
%export(attr_ccn);

/* step 6: export below steps like below */
%macro epi_report(tbl);
proc sql;
create table export_&prvdr._&tbl. as
select a.*, b.episode_name, b.clinical_category, b.episode_type
from in.export_&prvdr._&tbl. a
left join meta2.ppi_name_mapping b
on a.episode_id=b.def_sub or (a.ppi_type="TCC" and b.episode_type="TCC");
quit;

data out.export_&prvdr._&tbl.;
length provider_type $50. ;
retain py ppi_type provider_type episode_type clinical_category episode_id episode_name state cbsa pc_type;
set export_&prvdr._&tbl.;

if provider_type="attr_npi" then provider_type="Physician NPI";
if provider_type="attr_tin" then provider_type="Physician TIN";
if provider_type="attr_p_bill" then provider_type="Physician Billing NPI";
if provider_type="attr_f_bill" then provider_type="Facility Billing NPI";
if provider_type="attr_ccn" then provider_type="Facility CCN";

run;

proc sort data=out.export_&prvdr._&tbl.; 
by py ppi_type provider_type episode_type clinical_category episode_id episode_name cbsa;
run;
%mend;

%macro report(prvdr);
	%epi_report(cbd);
%mend;

%report(attr_npi);
%report(attr_tin);
%report(attr_p_bill);
%report(attr_f_bill);
%report(attr_ccn);





