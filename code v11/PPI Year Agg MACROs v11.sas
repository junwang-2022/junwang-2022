%macro pc_report(type, type_var, var, desc);
%if &type.=readm %then %do;
	proc sql;
	create table &type._&epi. as
	select a.*, coalescec(d.bundle_name, d.ms_drg_grp_desc) as description
	from out.epi_clm_&epi.(where=(&type.=1)) a
	left join meta.ms_drg d on a.&type_var.=d.&var.
	order by def_sub, def_id, phase, thru_dt;
	quit;
%end;
%else %do;	
	proc sql;
	create table &type._&epi. as
	select distinct a.*, c.&desc. as description
	from out.epi_clm_&epi.(where=(&type.=1)) a
	left join (select distinct &var., &desc. from meta.icd_10_dx) c on a.&type_var.=c.&var.
	order by def_sub, def_id, phase, thru_dt;
	quit;
%end;
	proc sql;
	create table &type._&epi. as
	select distinct def_sub,  def_id, "&type." as type, description, 
		count(distinct case_id) as visit_cnt, sum(ppi_amt) as tot_amt
	from &type._&epi.
	group by def_sub, def_id, description;
	quit;
%mend pc_report;

%macro pc;
%pc_report(readm, readm_drg, ms_drg,);
%pc_report(er, er_dx3, dx3, dx3_desc);
%pc_report(pc, pac_rf, pac_rf, pac_desc);
data out.epi_pc_&epi.; set readm_&epi. er_&epi. pc_&epi.; year=&year.;run;
proc datasets lib=work noprint; delete readm_&epi. er_&epi. pc_&epi.; run;
%mend pc;

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
left join out.trigger_&epi. b on a.def_sub=b.def_sub and a.def_id=b.def_id;
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

%if &epi.=PPI_CHEMO %then %do;
proc sql; 
create table rx_svc_&epi. as
select a.*, b.gnn
from out.rx_&epi. a
left join meta.ndc b on a.ndc=b.ndc;
quit;

data rx_svc_&epi.; 
set rx_svc_&epi.;
if gnn="" then gnn="Others";
if episode_in_rank="EPISODE_IN_09" then svc_cat="Part D Drugs - Chemotherapy";
if episode_in_rank="EPISODE_IN_16" then svc_cat="Part D Drugs - Other";
svc_sub=gnn;
run;

proc sql; 
create table rx_svc2_&epi. as
select def_sub, def_id, phase, clm_type, svc_cat, svc_sub, sum(ppi_amt) as ppi_amt
from rx_svc_&epi.
group by def_sub, def_id, phase, clm_type, svc_cat, svc_sub
union corr
select def_sub, def_id, phase, clm_type, "Total" as svc_cat length=60, "" as svc_sub, sum(ppi_amt) as ppi_amt
from rx_svc_&epi.
group by def_sub, def_id, phase, clm_type
order by def_sub, def_id, phase, clm_type, svc_cat, svc_sub;
quit;
%end;

data out.epi_cbd_&epi.;
set svc3_&epi. clm_type_&epi. %if &epi.=PPI_CHEMO %then %do; rx_svc2_&epi. %end;;
year=&year.;
if svc_cat="" then svc_cat="Others";
if svc_sub="" then svc_sub="All";
run;
proc sort data=out.epi_cbd_&epi.; by def_sub def_id phase clm_type svc_cat svc_sub; run;
%mend;


%macro epi_agg;

/* assign cancer type for PPI_CHEMO episodes*/

%if &epi.=PPI_CHEMO %then %do; 
	%do i=1 %to 7;
		proc sql;
		create table cancer_type_&i. as
		select "PPI_CHEMO_0"||"&i." as def_sub length=30, bene_id, def_id, count(distinct thru_dt) as cnt, max(thru_dt) as last_dt
		from in.&epi._episode_medical
		where episode_in_0&i.=1 and dnl_flag^=1
		group by bene_id, def_id
		order by bene_id, def_id, cnt desc;
		quit;
	%end;

	data all_cancer_type;
		set cancer_type_1 - cancer_type_7;
	run;

	proc sort data=all_cancer_type; by bene_id def_id descending cnt descending last_dt; run;
	proc sort data=all_cancer_type nodupkey; by bene_id def_id; run;
%end;


/* clean up the triggers */

data trigger_&epi.;
set in.&epi._trigger;
year=&year.;

index_setting=clm_type;
%if &epi.=PPI_CH_MED or &epi.=PPI_AC_MED or &epi.=PPI_RO or &epi.=PPI_CHEMO %then %do; index_discharge=""; %end;
%else %do; index_discharge=discharge; %end;

%if &enroll_type.=COMM %then %do;
if setting='ASC' then index_setting='ASC';
%end;
%else %do;
if clm_type='PHYS' and setting='ASC' then index_setting='ASC';
%end;

%if &epi.=PPI_CH_MED or &epi.=PPI_AC_MED or &epi.=PPI_CHEMO or &epi.=PPI_RO %then %do;
	index_setting="PHYS"; index_er=0; /* these episodes do not have different settings */
%end;

if exclusion=1 then delete; /* exclude triggers that are flagged as exclusion in the builder steps */
%if &epi.=PPI_PR_PROC %then %do; /* exclude physician triggers that are associated with an OP claim */
if syskey^=case_id then delete; 
%end; 

drop excl: from_dt thru_dt pdx_cd allowed_amt adm_dt dis_dt case_id line_: trigger_in_rank discharge;

run;

%if &epi.=PPI_CHEMO %then %do; /* reassign cancer type as def_sub */
	proc sql;
	create table trigger_&epi. as
	select a.*, b.def_sub, " " as sub_f3
	from trigger_&epi.(drop=def_sub) a, all_cancer_type b 
	where a.bene_id=b.bene_id and a.def_id=b.def_id;
	quit;
%end;
%else %if &epi.=PPI_CH_MED %then %do; /* only keep episodes with at least one E&M service. Otherwise the episode will be dropped at later step anyway */
	proc sql;
	create table em_&epi. as
	select distinct a.bene_id, a.def_sub, a.def_id
	from in.&epi._episode_medical a, in.&epi._rf b
	where a.bene_id=b.bene_id and a.syskey=b.syskey and b.risk_in_rank="RISK_IN_03";
	quit;
	proc sql;
	create table trigger_&epi. as
	select a.*, c.sub_f3
	from trigger_&epi. a, em_&epi. b, meta.def_list c
	where a.bene_id=b.bene_id and a.def_sub=b.def_sub and a.def_id=b.def_id and a.def_sub=c.def_sub;
	quit;
%end;
%else %do;
	proc sql;
	create table trigger_&epi. as
	select a.*, b.sub_f3
	from trigger_&epi. a, meta.def_list b 
	where a.def_name=b.def_name and a.def_sub=b.def_sub;
	quit;
%end;


/* clean up episode claims */

%if &epi.=PPI_CHEMO %then %do; /* need to reassign cancer type as def_sub in the claim file */
	proc sql;
	create table episode_&epi. as
	select a.*, b.year, b.def_sub, b.index_setting
	from in.&epi._episode_medical(drop=def_sub index_setting) a, trigger_&epi. b
	where a.bene_id=b.bene_id and a.def_id=b.def_id;
	quit;
	proc sql;
	create table rx_&epi. as
	select a.*, b.year, b.def_sub, b.index_setting, "POST" as phase, allowed_amt as ppi_amt
	from in.&epi._episode_pharmacy a, trigger_&epi. b
	where a.bene_id=b.bene_id and a.def_id=b.def_id;
	quit;
%end;
%else %do;
	proc sql;
	create table episode_&epi. as
	select a.*, b.year, b.sub_f3, b.index_setting
	from in.&epi._episode_medical %if &epi. ne PPI_CH_MED %then(drop=index_setting); a, trigger_&epi. b
	where a.bene_id=b.bene_id and a.def_sub=b.def_sub and a.def_id=b.def_id;
	quit;
%end;

%if &epi.=PPI_CH_MED %then %do; /* remove dup claims in chronic */
proc sort data=episode_&epi.; by bene_id def_sub def_id syskey episode_in_rank; run;
proc sort data=episode_&epi. nodupkey; by bene_id def_sub def_id syskey; run;
%end;

data episode_&epi.;
set episode_&epi.;

if index_case=1 then phase="INDEX"; else phase="POST ";
%if &epi.=PPI_PERINATAL %then %do; if phase^="INDEX" and thru_dt<=index_beg then phase="PRE"; %end; /* only perinatal care has PRE phase */
%if &epi.=PPI_CH_MED or &epi.=PPI_CHEMO or &epi.=PPI_RO %then %do; phase="POST "; %end; /* these episodes do not have index phase */
%if &epi.=PPI_PERINATAL %then def_sub="PPI_PERINATAL";;

if index_case=1 and index_claim=0 and clm_type in ("IP" "SNF") then delete;
if index_setting='ASC' and index_claim=1 then clm_type='ASC';

%if &db.=LDS %then %do;
	if adm_dt^=. and dis_dt^=. then los=min(ana_end,dis_dt)-max(index_beg,adm_dt);
	else los=min(ana_end,thru_dt)-max(index_beg,from_dt);
	ppi_amt=allowed_amt-max(0,sum(of cptl_ime_amt, cptl_dsh_amt, ime_op_amt, dsh_op_amt, ucc_amt));
	if thru_dt>ana_end then ppi_amt=ppi_amt*(ana_end-from_dt)/(thru_dt-from_dt);
	if ppi_amt<0 then ppi_amt=0;
%end;
%else %do;
	los=min(ana_end,thru_dt)-max(index_beg,from_dt);
		%if &enroll_type.=COMM %then %do;
		ppi_amt=allowed_amt;
		%end;
		%else %do;
		ppi_amt=allowed_amt-max(0,sum(of cptl_ime_amt, cptl_dsh_amt, ime_op_amt, dsh_op_amt, ucc_amt));
		%end;
	ppi_amt=ppi_amt*los/(thru_dt-from_dt);
	if ppi_amt<0 then ppi_amt=0;
%end;

run;


/* clean up episode claims */

proc sql;
create table episode_&epi.2 as
select a.*, c.dx3, c.pac_rf, d.ms_drg_type
from episode_&epi. a
left join meta.icd_10_dx c on a.pdx_cd=c.dx7
left join meta.ms_drg d on a.ms_drg=d.ms_drg
order by def_sub, def_id, phase, thru_dt;
quit;

/* assign complications */
%if &epi.= PPI_CH_MED or &epi.= PPI_CHEMO or &epi.= PPI_RO or &epi.= PPI_PERINATAL %then %do; /* no PC exempt due to POA */
data episode_&epi.2;
set episode_&epi.2;
pac_poa="";
run;
%end;

%else %do; /* PC exempt due to POA */
proc sql;
create table pc_poa_&epi. as
select distinct a.index_setting, a.clm_type, a.def_sub, a.def_id, b.dx_cd, b.poa, c.pac_rf
from episode_&epi.2 a, in.&epi._diagnosis b, meta.icd_10_dx c
where a.syskey=b.syskey and a.phase="INDEX" and b.dx_cd=c.dx7 and c.pac_rf^="";
quit;

proc sql;
create table pc_poa2_&epi. as
select distinct def_sub, def_id, pac_rf
from pc_poa_&epi.
where index_setting^='IP' or (index_setting='IP' and clm_type='IP' and poa^='N')
group by def_sub, def_id
order by def_sub, def_id;
quit;

proc sql;
create table episode_&epi.2 as
select a.*, c.pac_rf as pac_poa 
from episode_&epi.2 a
left join pc_poa2_&epi. c 
on a.def_sub=c.def_sub and a.def_id=c.def_id and a.pac_rf=c.pac_rf
order by def_sub, def_id, phase, thru_dt;
quit;
%end;

%if &epi.= PPI_AC_MED or &epi.= PPI_CH_MED %then %do;
data _null_; 
	set meta.def_list;
	where def_name="&epi.";
		call symputx("sub"||put(_n_,5. -l),def_sub);
		call symputx("pc"||put(_n_,5. -l),sub_f4);
		call symputx("cnt",_n_);
run;

data pc_&epi.;
%do i=1 %to &cnt.;
	%do p=1 %to %sysfunc(countw(&&pc&i.,'/')); 
		def_sub="&&sub&i.";
		def_pc="%scan(&&pc&i.,&p,'/')"; 
		output;
	%end;
%end;
run;

proc sql;
create table episode_&epi.2 as
select a.*, c.def_pc 
from episode_&epi.2 a
left join pc_&epi. c on a.def_sub=c.def_sub and a.pac_rf=c.def_pc;
quit;

data episode_&epi.2;
set episode_&epi.2;
if def_pc="" then pac_rf="";
drop def_pc;
run;
%end;

/* assign ER and readmissions */
%let planned_readm="001" "002" "005" "006" "007" "008" "009" "010" "011" "012" "013" "014" "015" "016" "017" "018" "019" 
					"650" "651" "652" "837" "838" "839" "846" "847" "848" "849" "945" "946" "949" "950";

proc sql;
create table out.epi_clm_&epi. as
select a.*, b.dx3 as er_dx3, b.case_id is not null as er, c.ms_drg as readm_drg, c.case_id is not null as readm, 
		case when pac_rf^="" and pac_poa="" then 1 else 0 end as pc
from episode_&epi.2 a
left join (select distinct def_id, case_id, dx3 from episode_&epi.2 where phase^="INDEX" and clm_type="OP" and setting="ER") b
	on a.def_id=b.def_id and a.case_id=b.case_id
left join (select distinct def_id, case_id, ms_drg from episode_&epi.2 where phase^="INDEX" and clm_type="IP" and setting in ("IPA" "IPF" "CAH") 
					and (ms_drg_type="M" or adm_type="1") and ms_drg not in (&planned_readm.)) c
	on a.def_id=c.def_id and a.case_id=c.case_id
order by def_sub, def_id, phase, index_claim desc, thru_dt, syskey, readm_drg;
quit;
proc sort data=out.epi_clm_&epi. nodupkey; by def_sub def_id syskey; run;


/* identify post-discharge provider */
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_CHEMO and &epi. ne PPI_RO %then %do;
proc sort data=out.epi_clm_&epi. out=first_pac_&epi.;
where phase="POST" and clm_type in ("IP" "SNF" "HHA" "HSP");
by def_sub def_id clm_type setting from_dt;
run;
proc sort data=first_pac_&epi. nodupkey; by def_sub def_id clm_type setting; run;

data dis_status_&epi.;
set out.epi_clm_&epi.;
where index_claim=1 and dis_status^="";
run;
%end;

/* roll up episode cost */
proc sql;
create table epi_summary_&epi. as
select distinct def_sub, def_id, index_setting, ana_end,
	sum(ppi_amt) as epi_pay, 
	sum(ppi_amt*(phase="INDEX")*(clm_type="PHYS")) as index_phys_pay, 
	sum(ppi_amt*(phase="INDEX")) as index_pay, sum(ppi_amt*(phase="POST")) as post_pay,
	sum(ppi_amt*(phase="PRE")) as pre_pay,
	sum(ppi_amt*(clm_type="IP")) as ip_pay, sum(ppi_amt*(clm_type="OP")) as op_pay,
	sum(ppi_amt*(clm_type="SNF")) as snf_pay, sum(ppi_amt*(clm_type="HHA")) as hha_pay,
	sum(ppi_amt*(clm_type in ("PHYS" "ASC"))) as phys_pay, sum(ppi_amt*(clm_type="DME")) as dme_pay,
	sum(ppi_amt*(clm_type="HSP")) as hsp_pay, 
	sum(los*index_claim) as index_los,
	sum(los*(clm_type in ("IP" "SNF") and setting in ("IRF" "LTCH" "SNF"))*(phase^="INDEX")) as ins_los,
	sum(readm*(clm_type="IP")) as readm_cnt, sum(er*(clm_type="OP")) as er_cnt,
	sum(ppi_amt*(clm_type in ("IP" "SNF") and setting in ("IRF" "LTCH" "SNF"))*(phase^="INDEX")) as ins_cost,
	sum(ppi_amt*readm) as readm_cost, sum(ppi_amt*er) as er_cost, sum(ppi_amt*(readm=0 and er=0 and pc=1)) as pc_cost
from out.epi_clm_&epi.
group by def_sub, def_id
order by def_sub, def_id;
quit;

%if &epi.=PPI_CH_MED %then %do; /* convert chronic episode measures to PMPY */
data epi_summary_&epi.;
set epi_summary_&epi.;
month=month(ana_end);
array x epi_pay post_pay pre_pay ip_pay op_pay snf_pay hha_pay phys_pay dme_pay hsp_pay ins_los reamd_cnt er_cnt ins_cost readm_cost er_cost pc_cost;
	do over x;
	x=x/month*12;
	end;
run;
%end;

%if &epi.=PPI_CHEMO %then %do;
	proc sql; /* add Part D cost */
	create table rx2_&epi. as
	select distinct def_sub, def_id, phase, clm_type, sum(ppi_amt) as ppi_amt
	from rx_&epi.
	group by def_sub, def_id, phase, clm_type
	order by def_sub, def_id, phase, clm_type;
	quit;

	proc sql;
	create table epi_summary_&epi. as
	select a.*, max(b.ppi_amt,0) as rx_pay
	from epi_summary_&epi. a
	left join rx2_&epi. b on a.def_sub=b.def_sub and a.def_id=b.def_id
	order by def_sub, def_id;
	quit;

	data epi_summary_&epi.;
	set epi_summary_&epi.;
	epi_pay=epi_pay+rx_pay;
	run;

	proc sql; /* flag surgery, radiation, bmt, prior acute IP, prior long-term IP, and prior chemo for chemo episodes */
	create table rf_&epi. as
	select a.*, b.*
	from
		(select def_sub, def_id, sum(episode_in_10)>0 as rf_surg, sum(episode_in_11)>0 as rf_rad,
			sum(episode_in_12)>0 as rf_bmt_allo, sum(episode_in_13)>0 as rf_bmt_auto
		from in.&epi._episode_medical
		group by def_sub, def_id) a
	left join
		(select def_sub, def_id, sum(risk_in_rank="RISK_IN_01")>0 as rf_prior_ipa, 
				sum(risk_in_rank="RISK_IN_02")>0 as rf_prior_ipl, sum(risk_in_rank="RISK_IN_03")>0 as rf_prior_chemo
		from in.&epi._rf
		group by def_sub, def_id) b
	on a.def_sub=b.def_sub and a.def_id=b.def_id;
	quit;
%end;
%else %if &epi.=PPI_RO %then %do; 
	proc sql; /* flag surgery and chemo for radiation episodes */
	create table rf_&epi. as
	select def_sub, def_id, sum(risk_in_rank="RISK_IN_01")+sum(risk_in_rank="RISK_IN_02")>0 as rf_chemo, 
		sum(risk_in_rank="RISK_IN_03") as rf_surg
	from in.&epi._rf
	group by def_sub, def_id;
	quit;
%end;
%else %do;
	proc sql; /* flag prior acute IP and long term IP for all other episodes */
	create table rf_&epi. as
	select def_sub, def_id, sum(risk_in_rank="RISK_IN_01")>0 as rf_prior_ipa, sum(risk_in_rank="RISK_IN_02")>0 as rf_prior_ipl
	from in.&epi._rf
	group by def_sub, def_id;
	quit;
%end;

%if &epi.=PPI_IOP_MJRLE %then %do; /* use ms_drg, not ICD proc code for case-mix adj.*/
	data trigger_drg_&epi.;
		set out.epi_clm_&epi.;
		where index_claim=1;
		keep def_sub def_id ms_drg;
	run;
	proc sql;
		create table trigger_&epi. as
		select a.*, b.ms_drg
		from trigger_&epi.(drop=ms_drg) a
		left join trigger_drg_&epi. b on a.def_sub=b.def_sub and a.def_id=b.def_id;
	quit;
%end;

proc sql; /* use bene zip code with the highest ppi_amt as the episode bene zip code */
create table bene_zip_&epi. as
select def_sub, def_id, bene_zip_cd, sum(ppi_amt) as tot_amt
from out.epi_clm_&epi.
group by def_sub, def_id, bene_zip_cd
order by def_sub, def_id, tot_amt desc;
quit;
proc sort data=bene_zip_&epi. out=bene_zip_&epi. nodupkey;
where bene_zip_cd^="";
by def_sub def_id;
run;

%if &epi.=PPI_PERINATAL %then %do;
data trigger_&epi.; set trigger_&epi.; def_sub="PPI_PERINATAL"; run;
%end;

proc sql;
create table out.trigger_&epi. as
select distinct a.*, b.*, c.*, d.bene_zip_cd, 
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_CHEMO and &epi. ne PPI_RO %then %do;
e.dis_status, f.bill_npi as pac_bill_npi, f.ccn as pac_ccn, f.adm_dt format=mmddyy10. as pac_start_dt, 
%end;
y.bene_id is not null as dual, case when a.bene_dod_dt^=. and a.bene_dod_dt<=a.ana_end then 1 else 0 end as death
from trigger_&epi. a
left join rf_&epi. b on %if &epi.^= PPI_CH_MED and &epi.^= PPI_CHEMO and &epi.^= PPI_PERINATAL %then a.def_sub=b.def_sub and; a.def_id=b.def_id
left join epi_summary_&epi. c on a.def_sub=c.def_sub and a.def_id=c.def_id
left join bene_zip_&epi. d on a.def_sub=d.def_sub and a.def_id=d.def_id
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_CHEMO and &epi. ne PPI_RO %then %do;
left join dis_status_&epi. e on a.def_sub=e.def_sub and a.def_id=e.def_id
left join first_pac_&epi. f on a.def_sub=f.def_sub and a.def_id=f.def_id and a.index_discharge=f.setting
%end;
left join ccm.dual y on a.bene_id=y.bene_id and %if &epi.= PPI_CH_MED %then %do; a.prf_beg %end; %else %do; a.index_dt %end; between y.start_dt and y.end_dt
having epi_pay>0;
quit;

data out.trigger_&epi.;
set out.trigger_&epi.;

if code_type="CPT" then index_cm=code_apc;
else index_cm=coalescec(code_apc, code);
%if &epi.=PPI_IOP_MJRLE %then index_cm=coalescec(ms_drg, code_apc);;
%if &epi.=PPI_CH_MED or &epi.=PPI_AC_MED or &epi.=PPI_CHEMO or &epi.=PPI_RO %then index_cm="x";;
if index_cm="" then index_cm="x";

%if &epi.=PPI_CH_MED %then %do; age=floor((prf_beg-bene_dob_dt)/365.25); %end;
%else %do; age=floor((index_dt-bene_dob_dt)/365.25); %end;

%if &enroll_type.=COMM %then %do;
if age<18 then age_grp="<18  ";
else if 18<=age<=34 then age_grp="18-34";
else if 35<=age<=54 then age_grp="35-54";
else if 55<=age<=64 then age_grp="55-64";
else if age>=65 then age_grp="65+  ";
%end;
%else %do;
if age<65 then age_grp="<65  ";
else if 65<=age<=74 then age_grp="65-74";
else if 75<=age<=84 then age_grp="75-84";
else if age>=85 then age_grp="85+  ";
%end;

pc_pay=sum(of readm_cost er_cost pc_cost);

array miss rf:;
do over miss;
if miss=. then miss=0;
end;

if def_sub in ("PPI_CHEMO_04" "PPI_CHEMO_05") then do;
	rf_bmt=0; if rf_bmt_allo=1 then rf_bmt_auto=0;
end;

else if def_sub="PPI_CHEMO_02" then do;
	if rf_bmt_allo=1 or rf_bmt_auto=1 then rf_bmt=1; else rf_bmt=0;
end;

else do;
	rf_bmt_allo=0; rf_bmt_auto=0; rf_bmt=0;
end;

if epi_pay<=0 then delete;
%if &epi.^=PPI_CH_MED and &epi.^=PPI_CHEMO and &epi.^=PPI_RO %then %do; if index_pay<=0 then delete; %end;
if index_setting^="PHYS" and index_phys_pay=0 then delete; /* for proc and IP med episods, exclude if no physician claims for the trigger event */
%if &epi.=PPI_PERINATAL %then %do; if pre_pay=0 then delete; %end;

drop prf_beg prf_end index_phys_pay;
run;

*** winsorization ***;
proc sort data=out.trigger_&epi.; by def_sub; run;
proc univariate data=out.trigger_&epi. noprint;
by def_sub;
var epi_pay pc_pay;
output out=outlier_&epi. n=cnt pctlpre=epi_p pc_p pctlpts=1,2.5,97.5,99;
run;

proc sql;
create table out.trigger_&epi. as
select a.*, min(max(a.epi_pay,epi_p1),epi_p99) as t_epi_pay, min(max(a.pc_pay,pc_p1),pc_p99) as t_pc_pay
from out.trigger_&epi. a, outlier_&epi. b
where a.def_sub=b.def_sub
order by def_sub, def_id;
quit;

proc sql;
create table out.epi_clm_&epi. as
select a.*
from out.epi_clm_&epi. a, out.trigger_&epi. b
where a.def_sub=b.def_sub and a.def_id=b.def_id;
quit;

%if &epi.=PPI_CHEMO %then %do;
proc sql;
create table out.rx_&epi. as
select a.*
from rx_&epi. a, out.trigger_&epi. b
where a.def_sub=b.def_sub and a.def_id=b.def_id;
quit;
%end;

%mend epi_agg;

%macro chemo_attr_type(type);
proc sql;
create table phys_&type._&epi. as
	select distinct def_sub, def_id, &type. as attr_&type., prf_npi as attr_npi, count(*) as npi_cnt, 
		min(from_dt) as npi_first_dt, max(from_dt) as npi_last_dt
	from out.epi_clm_&epi.
	where clm_type='PHYS' and "1"<=substr(episode_in_rank,13,1)<="7"
	group by def_sub, def_id, &type., prf_npi;
quit;
proc sql;
create table phys_&type._&epi. as
	select distinct a.*, c.spec_cd, c.spec_type, c.spec_cd^="" as phys_flag
	from phys_&type._&epi. a
left join npi.npi_x_spec_&year. b on a.attr_npi^="" and a.attr_npi=b.npi
left join meta2.ppi_spec_mapping c on a.def_sub=c.def_sub and b.spec_cd^="" and b.spec_cd=c.spec_cd;
quit;
proc sql;
create table phys_&type._&epi. as
	select *, sum(npi_cnt) as &type._cnt, min(npi_first_dt) as &type._first_dt, max(npi_last_dt) as &type._last_dt
	from phys_&type._&epi.
	group by def_sub, def_id, attr_&type.
	order by def_sub, def_id, &type._cnt desc, npi_cnt desc;
quit;
proc sql;
create table phys_&type._&epi. as
	select *, &type._cnt/sum(npi_cnt)>=0.25 as &type._pct_flag, &type._first_dt=min(&type._first_dt) as first_&type._flag
	from phys_&type._&epi.
	group by def_sub, def_id
	order by def_sub, def_id, &type._pct_flag desc, first_&type._flag desc, &type._cnt desc, &type._first_dt, npi_cnt desc;
quit;
proc sort data=phys_&type._&epi. nodupkey; where phys_flag=1; by def_sub def_id; run;

%mend chemo_attr_type;

%macro attribution;

%if &epi.= PPI_CH_MED %then %do;

proc sql;
create table phys_spec_&epi. as
select distinct a.def_sub, a.def_id, coalescec(a.prf_npi, a.at_npi, a.op_npi) as attr_npi, 
	coalescec(a.tin, a.ccn) as attr_tin, a.bill_npi as attr_p_bill,
	count(distinct a.thru_dt) as clm_cnt, sum(ppi_amt) as npi_amt
from out.epi_clm_&epi. a, in.&epi._rf b
where a.bene_id=b.bene_id and a.syskey=b.syskey and b.risk_in_rank="RISK_IN_03"
group by a.def_sub, a.def_id, coalescec(a.prf_npi, a.at_npi, a.op_npi), coalescec(a.tin, a.ccn), a.bill_npi;
quit;

proc sql;
create table phys_spec_&epi. as
select distinct a.*, c.spec_cd, c.spec_type
from phys_spec_&epi. a
left join npi.npi_x_spec_&year. b on a.attr_npi^="" and a.attr_npi=b.npi
left join meta2.ppi_spec_mapping c on a.def_sub=c.def_sub and b.spec_cd^="" and b.spec_cd=c.spec_cd
having spec_cd^=""
order by def_sub, def_id, clm_cnt desc, npi_amt desc;
quit;
proc sort data=phys_spec_&epi. nodupkey; by def_sub def_id; run;

proc sql;
create table bene_&epi. as 
select a.*, b.attr_npi, b.spec_cd, b.spec_type, b.attr_p_bill, b.attr_tin,  "" as attr_f_bill length=10, "" as attr_ccn length=6
from out.trigger_&epi. a
left join phys_spec_&epi. b on a.def_sub=b.def_sub and a.def_id=b.def_id
having spec_cd^=""
order by def_sub, def_id;
quit;

%end;

%else %if &epi.= PPI_CHEMO %then %do;

%chemo_attr_type(tin);
%chemo_attr_type(bill_npi);

proc sql;
create table fac_bill_&epi. as
select def_sub, def_id, bill_npi, sum(ppi_amt) as bill_amt
from out.epi_clm_&epi.
where episode_in_rank="EPISODE_IN_08" and clm_type='OP'
group by def_sub, def_id, bill_npi
order by def_sub, def_id, bill_amt desc;
quit;
proc sort data=fac_bill_&epi. nodupkey; by def_sub def_id; run;

proc sql;
create table fac_ccn_&epi. as
select def_sub, def_id, ccn, sum(ppi_amt) as ccn_amt
from out.epi_clm_&epi.
where episode_in_rank="EPISODE_IN_08" and clm_type='OP'
group by def_sub, def_id, ccn
order by def_sub, def_id, ccn_amt desc;
quit;
proc sort data=fac_ccn_&epi. nodupkey; by def_sub def_id; run;

proc sql;
create table bene_&epi. as 
select a.*, b.attr_tin, b.attr_npi, b.spec_cd, b.spec_type, c.attr_bill_npi as attr_p_bill, d.bill_npi as attr_f_bill length=10, e.ccn as attr_ccn length=6
from out.trigger_&epi. a
left join phys_tin_&epi. b on a.def_sub=b.def_sub and a.def_id=b.def_id
left join phys_bill_npi_&epi. c on a.def_sub=c.def_sub and a.def_id=c.def_id
left join fac_bill_&epi. d on a.def_sub=d.def_sub and a.def_id=d.def_id
left join fac_ccn_&epi. e on a.def_sub=e.def_sub and a.def_id=e.def_id
having spec_cd^=""
order by def_sub, def_id;
quit;
%end;

%else %do;
proc sql;
create table asc_npi_&epi. as
select a.*, c.spec_cd, c.spec_type
from out.epi_clm_&epi.(where=(index_setting='ASC' and phase="INDEX" and index_claim=0)) a
left join npi.npi_x_spec_&year. b on a.prf_npi^="" and a.prf_npi=b.npi
left join meta2.ppi_spec_mapping c on a.def_sub=c.def_sub and b.spec_cd^="" and b.spec_cd=c.spec_cd
having spec_cd^=""
order by def_sub, def_id, allowed_amt desc;
quit;
proc sort data=asc_npi_&epi. nodupkey; by def_sub def_id; run;

data index_prvdr_&epi.;
set out.epi_clm_&epi.;
where index_claim=1;
length attr_f_bill $10 trigger_p_bill $10;
if index_setting^='ASC' then do;
	if index(def_sub, "PROC")>0 or index(def_sub, "MJRLE")>0 then attr_npi=coalescec(op_npi, at_npi, prf_npi);
	else attr_npi=coalescec(at_npi, op_npi, prf_npi);
	attr_ccn=ccn;
	if clm_type="PHYS" then do; trigger_p_bill=bill_npi; attr_f_bill=" "; end;
	else do; trigger_p_bill=""; attr_f_bill=bill_npi; end;
	%if &epi.=PPI_AC_MED %then %do;
		tin=ccn; trigger_p_bill=bill_npi; 
		attr_ccn=""; attr_f_bill=" ";
	%end;
end;
if index_setting='ASC' then do; attr_f_bill=bill_npi; tin=""; end;
index_er=(er_flag=1 or setting="ER"); 
run;

proc sql;
create table trigger_npi_&epi. as 
select a.*, c.index_er, c.attr_ccn, c.attr_f_bill, c.trigger_p_bill, coalescec(d.tin,c.tin) as trigger_tin,
	coalescec(d.prf_npi,c.attr_npi) as attr_npi, f.spec_cd, f.spec_type
from out.trigger_&epi. a
left join index_prvdr_&epi. c on a.def_sub=c.def_sub and a.def_id=c.def_id
left join asc_npi_&epi. d on a.def_sub=d.def_sub and a.def_id=d.def_id
left join npi.npi_x_spec_&year. e on coalescec(d.prf_npi,c.attr_npi)^="" and coalescec(d.prf_npi,c.attr_npi)=e.npi
left join meta2.ppi_spec_mapping f on a.def_sub=f.def_sub and e.spec_cd^="" and e.spec_cd=f.spec_cd
having spec_cd^=""
order by def_sub, def_id;
quit;

*** map attr TIN/billing NPI ***;
proc sql;
create table epi_tin_&epi. as
select def_sub, def_id, prf_npi, tin, sum(allowed_amt) as tot_amt
from out.epi_clm_&epi.
where clm_type='PHYS' and phase="INDEX"
group by def_sub, def_id, prf_npi, tin
order by def_sub, def_id, prf_npi, tot_amt desc;
quit;
proc sort data=epi_tin_&epi. nodupkey; by def_sub def_id prf_npi; run;

proc sql;
create table epi_bill_&epi. as
select def_sub, def_id, prf_npi, bill_npi, sum(allowed_amt) as tot_amt
from out.epi_clm_&epi.
where clm_type='PHYS' and phase="INDEX"
group by def_sub, def_id, prf_npi, bill_npi
order by def_sub, def_id, prf_npi, tot_amt desc;
quit;
proc sort data=epi_bill_&epi. nodupkey; by def_sub def_id prf_npi; run;

proc sql;
create table bene_&epi.(drop=trigger_tin trigger_p_bill) as 
select distinct a.*, 
coalescec(a.trigger_tin, b1.tin, b2.tin) as attr_tin, 
coalescec(a.trigger_p_bill, c1.bill_npi, c2.bill_npi) as attr_p_bill
from trigger_npi_&epi. a
left join epi_tin_&epi. b1 on a.def_sub=b1.def_sub and a.def_id=b1.def_id and b1.prf_npi=a.attr_npi
left join npi.npi_x_tin_&year. b2 on b2.prf_npi=a.attr_npi
left join epi_bill_&epi. c1 on a.def_sub=c1.def_sub and a.def_id=c1.def_id and c1.prf_npi=a.attr_npi
left join npi.npi_x_bill_&year. c2 on c2.prf_npi=a.attr_npi
order by def_sub, def_id;
quit;

%end;


*** map attr cbsa ***;
proc sql;
create table out.bene_&epi. as
select x.*, z.cbsa, z.state, z.county
from bene_&epi. x
left join npi.provider_x_cbsa_&year. z on x.attr_npi=z.provider_id and z.provider_type="phys_npi";
quit;

%if &db.=LDS %then %do;
data out.bene_&epi.;
set out.bene_&epi.;
if attr_npi^="" then attr_npi=spec_cd||substr(attr_npi,2,1)||"XXXXXX";
if attr_tin^="" then attr_tin=spec_cd||substr(attr_tin,2,1)||"XXXXXX";
if attr_f_bill^="" then attr_f_bill=spec_cd||substr(attr_f_bill,2,1)||"XXXXXX";
if attr_p_bill^="" then attr_p_bill=spec_cd||substr(attr_p_bill,2,1)||"XXXXXX";
if attr_ccn^="" then attr_ccn=spec_cd||substr(attr_ccn,1,1)||"XXX";
if substr(attr_npi,3,1) in ("0" "1" "2" "3" "4") then cbsa="1XXXXX"; else cbsa="2XXXXX";
state="VA";
run;
%end;

%mend attribution;

%macro model(y);
proc hpgenselect data=sample_&sub. noprint maxtime=10;
	class cbsa spec_type index_setting index_cm bene_gender age_grp cnt_hcc_grp ;
	model &y.=cbsa spec_type index_setting index_cm bene_gender age_grp dual index_er cnt_hcc_grp rf_: &hcc_list. 
		%if &y.= t_epi_pay %then %do; /include=8 dist=gamma link=log; %end;
		%if &y.= t_pc_pay %then %do; /include=8 dist=tweedie; %end;
		%if &y.= readm_cnt or &y.= er_cnt or &y.= ins_los %then %do; 
				/include=8 dist=zinb; 
				zeromodel cbsa spec_type index_setting index_cm bene_gender age_grp dual index_er cnt_hcc_grp rf_: &hcc_list. ;
		%end;
		%if &y.= death %then %do; /include=8 dist=binary link=logit; %end;
	selection method=forward(sle=0.15);
code file="&model.\sc_&sub._&y..sas";
run;
quit;

data mo_&y._&sub.;
set model_&sub.;
%include "&model.\sc_&sub._&y..sas";
run;
%mend;

%macro re_norm(y);
data p_&y._&epi.; 
set out.p_&y._&epi.;
	if p_&y.=. then p_&y.=mean_&y.; 
	if p_&y.=0 then p_&y.=0.00000001; 
run;

*** winsorization ***;
proc sort data=p_&y._&epi.; by def_sub cbsa spec_type; run;
proc univariate data=p_&y._&epi. noprint;
	by def_sub cbsa spec_type;
	var p_&y.;
	output out=p_o_&y._&epi. n=cnt pctlpre=&y._p pctlpts=1,99;
run;

proc sql;
	create table p_&y._&epi. as
	select a.*, min(max(a.p_&y.,b.&y._p1),b.&y._p99) as t_p_&y.
	from p_&y._&epi. a, p_o_&y._&epi. b
	where a.def_sub=b.def_sub and a.cbsa=b.cbsa and a.spec_type=b.spec_type
	order by def_sub, def_id;
quit;

proc datasets lib=work noprint; delete p_o_:; run;

*** renormalization by cbsa and specialty type ***;
proc sql;
create table out.p_&y._&epi. as
select *, t_p_&y.*(sum(&y.)/sum(t_p_&y.)) as n_p_&y.
from p_&y._&epi.
group by def_sub, cbsa, spec_type;
quit;

%mend;

%macro update_bad_model(y);
proc sql;
create table bad_mo_&y._&epi. as
select distinct def_sub, sum(oe>10)/count(*) as high_oe_pct
from
	(select distinct def_sub, attr_npi, count(*) as cnt, sum(&y.)/sum(n_p_&y.) as oe
	from out.p_&y._&epi.
	group by def_sub, attr_npi)
where cnt>10
group by def_sub;
quit;

proc sql noprint;
select distinct "'"||strip(def_sub)||"'" into :sub_list separated by " "
from bad_mo_&y._&epi.
where high_oe_pct>0.02;
quit;

%if &sqlobs.>0 %then %do;
	data out.p_&y._&epi.;
	set out.p_&y._&epi.(drop=t_p_&y. n_p_&y.);
	if def_sub in (&sub_list.) then p_&y.=.;
	run;
	%re_norm(&y.);
%end;
%mend;

%macro risk_adj;
proc datasets lib=work noprint; delete hcc: model: pred_: ; run;

proc sql;
create table hcc_&epi. as
	select a.*, b.*
	from out.bene_&epi. a 
	left join out.hcc_&epi. b
	on %if &epi.^=PPI_CH_MED and &epi.^=PPI_CHEMO and &epi.^=PPI_PERINATAL %then %do; a.def_sub=b.def_sub and %end; a.def_id=b.def_id
	order by def_sub, def_id;
quit;

data hcc_&epi.;
set hcc_&epi.;

array x hcc:;
do over x;
if x=. then x=0;
end;

cnt_hcc=sum(of hcc:);

%if &enroll_type.=COMM %then %do;
if cnt_hcc=0 then cnt_hcc_grp="0  ";
else if cnt_hcc=1 then cnt_hcc_grp="1  ";
else if 2<=cnt_hcc<=3 then cnt_hcc_grp="2-3";
else if 4<=cnt_hcc<=6 then cnt_hcc_grp="4-6";
else cnt_hcc_grp="7+ ";
%end;
%else %do;
if cnt_hcc=0 then cnt_hcc_grp="0  ";
else if 1<=cnt_hcc<=3 then cnt_hcc_grp="1-3";
else if 4<=cnt_hcc<=6 then cnt_hcc_grp="4-6";
else cnt_hcc_grp="7+ ";
%end;

%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED %then %do; /* adj for specialty type only for OP acute and chronic episodes */
spec_type="x";
%end;

run;

proc sql noprint;
select distinct def_sub into:sublst separated by " " from hcc_&epi.; quit;

/*proc sql noprint;*/
/*select distinct def_sub into:sublst separated by " " from hcc_&epi. */
/*where def_sub not in ("PPI_CH_MED_Y020" "PPI_CH_MED_Y026" "PPI_CH_MED_Y057" "PPI_CH_MED_Y084" "PPI_CH_MED_Y103" "PPI_CH_MED_Y104"); */
/*quit;*/

%do i=1 %to %sysfunc(countw(&sublst.));
	%let sub=%scan(&sublst., &i.);

	proc sql;
		create table model_&sub. as
		select distinct *, mean(t_epi_pay) as mean_t_epi_pay, mean(t_pc_pay) as mean_t_pc_pay, mean(readm_cnt) as mean_readm_cnt,
			mean(er_cnt) as mean_er_cnt, mean(ins_los) as mean_ins_los, mean(death) as mean_death
		from hcc_&epi.
		where def_sub="&sub."
		group by cbsa, spec_type, index_setting, index_cm, bene_gender, age_grp, dual, index_er;
	quit;

	proc sql noprint; select count(*) into:cnt from model_&sub.; quit;
	%if &cnt.>&model_max_samplesize. %then %do;
		proc sort data=model_&sub.; 
		by cbsa spec_type index_setting index_cm bene_gender age_grp dual index_er;
		run;
		proc surveyselect data=model_&sub. method=srs n=&model_max_samplesize. seed=1234 out=sample_&sub.;
	   	strata cbsa spec_type index_setting index_cm bene_gender age_grp dual index_er/alloc=proportional;
		run;
	%end;
	%else %do;
		data sample_&sub.; set model_&sub.; run;
	%end;

	proc sort data=sample_&sub.; by def_id; run;
	proc transpose data=sample_&sub.(keep=def_id hcc:) out=sample_&sub._trans; by def_id; run;
	proc sql; select count(*) into:cnt from sample_&sub; quit;
	proc sql;
	create table hcc_cnt_&sub. as
	select distinct _name_ as hcc, sum(col1) as hcc_cnt, sum(col1)/&cnt. as hcc_pct
		from sample_&sub._trans
		group by _name_;
	quit;

	proc sql;
	select distinct hcc into:hcc_list separated by " " from hcc_cnt_&sub. where /*hcc_cnt>=15 and*/ hcc_pct>0.001; quit;

	%model(t_epi_pay);
	%model(t_pc_pay);
	%model(readm_cnt);
	%model(er_cnt);
	%model(ins_los);
	%model(death);
%end;

data out.p_t_epi_pay_&epi.(drop=hcc:); 	set mo_t_epi_pay_:; run;
data out.p_t_pc_pay_&epi.(drop=hcc:); 	set mo_t_pc_pay_:; run;
data out.p_readm_cnt_&epi.(drop=hcc:);	set mo_readm_cnt_:; run;
data out.p_er_cnt_&epi.(drop=hcc:); 	set mo_er_cnt_:; run;
data out.p_ins_los_&epi.(drop=hcc:); 	set mo_ins_los_:; run;
data out.p_death_&epi.(drop=hcc:); 		set mo_death_:; run;

proc datasets lib=work noprint; delete mo_: model: sample: hcc: ; run;

%re_norm(t_epi_pay);	%update_bad_model(t_epi_pay);
%re_norm(t_pc_pay); 	%update_bad_model(t_pc_pay);
%re_norm(readm_cnt); 	%update_bad_model(readm_cnt);
%re_norm(er_cnt); 		%update_bad_model(er_cnt);
%re_norm(ins_los); 		%update_bad_model(ins_los);
%re_norm(death); 		%update_bad_model(death);

%mend risk_adj;

%macro bene_output;
proc sql;
create table out.bene_p_&epi. as
	select a.*, b.n_p_t_epi_pay, c.n_p_t_pc_pay, d.n_p_readm_cnt, e.n_p_er_cnt, f.n_p_ins_los, g.n_p_death
	from out.bene_&epi. a 
	left join out.p_t_epi_pay_&epi. b on a.def_sub=b.def_sub and a.def_id=b.def_id
	left join out.p_t_pc_pay_&epi. c on a.def_sub=c.def_sub and a.def_id=c.def_id
	left join out.p_readm_cnt_&epi. d on a.def_sub=d.def_sub and a.def_id=d.def_id
	left join out.p_er_cnt_&epi. e on a.def_sub=e.def_sub and a.def_id=e.def_id
	left join out.p_ins_los_&epi. f on a.def_sub=f.def_sub and a.def_id=f.def_id
	left join out.p_death_&epi. g on a.def_sub=g.def_sub and a.def_id=g.def_id
	order by def_sub, def_id;
quit;

data out.bene_p_&epi.;
set out.bene_p_&epi.;
drop clm_type def_desc epi_type def_name px_cd rf_: seq_num setting svc_: syskey;
run;
%mend;

