libname meta "C:\Users\jun.wang\Episode Builder\Episode builder v9\meta v9";
libname out "C:\Users\jun.wang\Episode Builder\Episode builder v9\meta v9\def doc";
libname eom "C:\Users\jun.wang\EOM";


%macro ppi_list;

data ppi_chemo;
set meta.def_list;
where def_name="PPI_CHEMO";
	%do i=1 %to 7;
		def_sub="PPI_CHEMO_0"||"&i.";
		output;
	%end;
run;

data out.ppi_list;
retain episode_category epi_type clinical_category epi_id epi_name epi_length attr_provider;

set meta.def_list(where=(def_name^="PPI_CHEMO")) ppi_chemo;
if substr(def_name,1,3)="PPI";

length epi_type $50. epi_length $10. attr_provider $100.;

if def_name="PPI_IP_PROC" then do;
	epi_type="IP Procedure";
	epi_length="90 days";
	attr_provider="Trigger operating NPI";
end;
if def_name="PPI_IOP_PROC" or def_name="PPI_IOP_MJRLE" then do;
	epi_type='IP or OP/ASC Procedure';
	epi_length="90 days";
	attr_provider="Trigger operating NPI";
end;
if def_name="PPI_OP_PROC" then do;
	epi_type='OP/ASC Procedure';
	epi_length="30 days";
	attr_provider="Trigger operating NPI";
end;
if def_name="PPI_PR_PROC" then do;
	epi_type='OP/ASC or Office Procedure';
	epi_length="30 days";
	attr_provider="Trigger operating/performing NPI";
end;
if def_name="PPI_IP_MED" then do;
	epi_type='IP Medical';
	epi_length="90 days";
	attr_provider="Trigger attending NPI";
end;
if def_name="PPI_AC_MED" then do;
	epi_type='OP Acute Medical';
	epi_length="90 days";
	attr_provider="Trigger performing NPI";
end;
if def_name="PPI_CH_MED" then do;
	epi_type='Chronic Medical';
	epi_length="1 year";
	attr_provider="Performing NPI with plurality of service";
end;
if def_name="PPI_CHEMO" then do;
	if def_sub="PPI_CHEMO_01" then def_desc="Med Onc - Breast cancer";
	if def_sub="PPI_CHEMO_02" then def_desc="Med Onc - Chronic leukemia";
	if def_sub="PPI_CHEMO_03" then def_desc="Med Onc - Lung cancer";
	if def_sub="PPI_CHEMO_04" then def_desc="Med Onc - Lymphoma";
	if def_sub="PPI_CHEMO_05" then def_desc="Med Onc - Multiple myeloma";
	if def_sub="PPI_CHEMO_06" then def_desc="Med Onc - Prostate cancer";
	if def_sub="PPI_CHEMO_07" then def_desc="Med Onc - Small intestine/colorectal cancer";
	epi_type='Medical Oncology';
	epi_length="180 days";
	attr_provider="Trigger performing NPI or NPI with plurality of service";
end;
if def_name="PPI_RO" then do;
	epi_type='Radiation Oncology';
	epi_length="90 days";
	attr_provider="Trigger performing NPI";
end;

if index(epi_type,"Procedure")>0 then episode_category="tx"; else episode_category="cd";

length clinical_category $50.;
if sub_f3="A" then clinical_category="Infectious diseases";
if sub_f3="B" then clinical_category="Blood diseases";
if sub_f3="C" then clinical_category="Endocrine/metabolic diseases";
if sub_f3="D" then clinical_category="Mental health";
if sub_f3="E" then clinical_category="Nervous system diseases";
if sub_f3="F" then clinical_category="Eye and adnexa diseases";
if sub_f3="G" then clinical_category="Ear and mastoid diseases";
if sub_f3="H" then clinical_category="Circulatory system diseases";
if sub_f3="I" then clinical_category="Respiratory system diseases";
if sub_f3="J" then clinical_category="GI system diseases";
if sub_f3="K" then clinical_category="Skin/subcutaneous diseases";
if sub_f3="L" then clinical_category="Musculoskeletal system diseases";
if sub_f3="M" then clinical_category="GU system diseases";
if sub_f3="XXX" then clinical_category="Injury";
if def_name in ("PPI_CHEMO" "PPI_RO") then clinical_category="Cancer";

drop def_name;

epi_id=def_sub;
epi_name=def_desc;

keep episode_category epi_type clinical_category epi_id epi_name epi_length attr_provider;
run;
%mend;
%ppi_list;

%macro trigger_dx(epi);
proc sql;
create table trigger_&epi._dx as
select a.def_sub, "ICD_10_DX" as code_type length=10, b.dx7 as code length=12 format=$12., b.dx7_desc as code_description length=100
from meta.def_list(where=(def_name="&epi.")) a
left join meta.icd_10_dx b
on a.sub_f1=substr(b.&epi., 1,4)
order by def_sub, code;
quit;
%mend;
%trigger_dx(PPI_AC_MED);
%trigger_dx(PPI_CH_MED);

%macro trigger_px(epi);
proc sql;
create table trigger_&epi._px as
select a.def_sub, "ICD_10_PX" as code_type length=10, b.px7 as code length=12 format=$12., b.px7_desc as code_description length=100
from meta.def_list(where=(def_name="&epi.")) a
left join meta.icd_10_px b
on a.sub_f1=substr(b.&epi., 1,5)
order by def_sub, code;
quit;
%mend;
%trigger_px(PPI_IOP_MJRLE);

%macro trigger_drg(epi, num);
proc sql;
create table trigger_&epi._drg as
select a.def_sub, "MS-DRG" as code_type length=10, b.ms_drg as code length=12 format=$12., b.ms_drg_desc as code_description length=100
from meta.def_list(where=(def_name="&epi.")) a
left join meta.ms_drg b
on a.sub_f1=substr(b.&epi., 1, &num.)
order by def_sub, code;
quit;
%mend;
%trigger_drg(PPI_IP_PROC,3);
%trigger_drg(PPI_IOP_PROC,3);
%trigger_drg(PPI_IP_MED,4);

data trigger_ppi_mjrle;
length def_sub $30 code_type $10 code $12;
input def_sub $ code_type $ code $;
datalines;
PPI_IOP_MJRLE_ANKLE MS-DRG 469
PPI_IOP_MJRLE_ANKLE MS-DRG 470
PPI_IOP_MJRLE_ANKLE MS-DRG 461
PPI_IOP_MJRLE_ANKLE MS-DRG 462
PPI_IOP_MJRLE_KNEE MS-DRG 469
PPI_IOP_MJRLE_KNEE MS-DRG 470
PPI_IOP_MJRLE_KNEE MS-DRG 461
PPI_IOP_MJRLE_KNEE MS-DRG 462
PPI_IOP_MJRLE_HIP MS-DRG 469
PPI_IOP_MJRLE_HIP MS-DRG 470
PPI_IOP_MJRLE_HIP MS-DRG 461
PPI_IOP_MJRLE_HIP MS-DRG 462
PPI_IOP_MJRLE_HIP MS-DRG 521
PPI_IOP_MJRLE_HIP MS-DRG 522
;
run;

proc sql;
create table trigger_ppi_mjrle as
select a.*, b.ms_drg_desc as code_description
from trigger_ppi_mjrle a
left join meta.ms_drg b
on a.code=b.ms_drg;
quit;

%macro trigger_cpt(epi, num);
proc sql;
create table trigger_&epi._cpt as
select a.def_sub, "HCPCS/CPT" as code_type length=10, b.cpt as code length=12 format=$12., b.cpt_desc as code_description length=100
from meta.def_list(where=(def_name="&epi.")) a
left join meta.cpt b
on a.sub_f1=substr(b.&epi., 1, &num.) and (b.term_dt=. or year(b.term_dt)>=2015)
order by def_sub, code;
quit;
%mend;

%trigger_cpt(PPI_IOP_PROC,3);
%trigger_cpt(PPI_IOP_MJRLE,5);
%trigger_cpt(PPI_OP_PROC,4);
%trigger_cpt(PPI_PR_PROC,4);

%macro chemo_dx;
data _null_; 
	set meta.def_spec;
	where def_name="PPI_CHEMO" and index(def_cat, "EPI_IN_")>0 and code_type="LINE_DX_CD";
		call symputx("code_desc"||put(_n_,5. -l),code_desc);
		call symputx("dx"||put(_n_,5. -l),code);
		call symputx("cnt",_n_);
run;

data trigger_ppi_chemo_dx(keep=def_sub code_type code code_description);
length def_sub $15. code $12. code_type $10. code_description $50.;
%do i=1 %to &cnt.;
	%do p=1 %to %sysfunc(countw(&&dx&i.,'/')); 
		def_sub="PPI_CHEMO_0"||"&i.";
		code="%scan(&&dx&i.,&p,'/')"; 
		code_type="ICD_10_DX";
		code_description="&&code_desc&i.";
		output;
	%end;
%end;
run;
%mend;
%chemo_dx;

proc sql;
create table trigger_chemo_cpt as
select distinct "cd" as episode_category, "Medical Oncology" as epi_type, "Cancer" as clinical_category, 
	"PPI_CHEMO - All" as epi_id, "Med Onc - All" as epi_name, "HCPCS/CPT" as code_type length=10, 
	b.cpt as code length=12 format=$12., b.cpt_desc as code_description length=100
from meta.def_list(where=(def_name="PPI_CHEMO")) a
left join meta.cpt b
on b.ppi_chemo="A"
order by def_sub, code;
quit;

data trigger_ndc;
set eom.eom_ndc;
episode_category="cd";
epi_type="Medical Oncology";
clinical_category="Cancer";
epi_id="PPI_CHEMO - All";
epi_name="Med Onc - All";
code_type="NDC";
code=ndc;
code_description=generic_name;
drop ndc generic_name;
run;

%macro ro_dx;
data _null_; 
	set meta.def_list;
	where def_name="PPI_RO";
		call symputx("def_name"||put(_n_,5. -l),def_desc);
		call symputx("def_sub"||put(_n_,5. -l),def_sub);
		call symputx("dx"||put(_n_,5. -l),sub_f1);
		call symputx("cnt",_n_);
run;

data trigger_ppi_ro_dx(keep=def_sub code_type code code_description);
length def_sub $15. code $12. code_type $10. code_description $100.;
%do i=1 %to &cnt.;
	%do p=1 %to %sysfunc(countw(&&dx&i.,'/')); 
		def_sub="&&def_sub&i.";
		code="%scan(&&dx&i.,&p,'/')"; 
		code_type="ICD_10_DX";
		code_description="%substr(&&def_name&i.,%index(&&def_name&i.,-)+2,%length(&&def_name&i.)-(%index(&&def_name&i.,-)+1))";
		output;
	%end;
%end;
run;
%mend;
%ro_dx;

proc sql;
create table trigger_ro_cpt as
select distinct "cd" as episode_category, "Radiation Oncology" as epi_type, "Cancer" as clinical_category, 
	"PPI_RO - All" as epi_id, "Rad Onc - All" as epi_name, "HCPCS/CPT" as code_type length=10, 
	b.cpt as code length=12 format=$12., b.cpt_desc as code_description length=100
from meta.def_list(where=(def_name="PPI_RO")) a
left join meta.cpt b
on b.ppi_ro="P"
order by def_sub, code;
quit;

data ppi_def_trigger;
set trigger_ppi_:;
if code="" then delete;
run;

proc sql;
create table ppi_def_trigger as
select b.episode_category, b.epi_type, b.clinical_category, b.epi_id, b.epi_name, a.code_type, a.code, a.code_description
from ppi_def_trigger a
left join out.ppi_list b
on b.epi_id=a.def_sub
order by episode_category, epi_type, clinical_category, epi_id,epi_name, code_type, code;
run;

data out.ppi_def_trigger;
set ppi_def_trigger trigger_ro_cpt trigger_chemo_cpt trigger_ndc ;
run;
proc sort data=out.ppi_def_trigger; 
by episode_category epi_type clinical_category epi_id epi_name code_type code;
run;

proc export data=out.ppi_list outfile="C:\Users\jun.wang\Episode Builder\Episode builder v9\meta v9\def doc\ppi_list.csv" replace; run;
proc export data=out.ppi_def_trigger outfile="C:\Users\jun.wang\Episode Builder\Episode builder v9\meta v9\def doc\ppi_def_trigger.csv" replace; run;

