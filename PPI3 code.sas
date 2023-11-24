libname a "C:\Users\jun.wang\PPI3.0\def doc";
libname meta "C:\Users\jun.wang\Episode Builder\Episode builder v10\meta v10";

%macro epi_in(epi, cat, day);

data &epi.;
set a.whole_codelst_&epi.;
where index(def_cat, "EPI_IN_") and code^="";
keep id def_name def_sub def_cat code_position code_int code_desc code t_var check_beg_offset check_end_offset;
run;

proc sql;
create table all_&epi. as
select distinct b.id, b.def_name, a.def_sub, b.*
from (select distinct def_sub from &epi. where def_sub^="ALL") a, &epi. b
where b.def_sub="ALL";
quit;

data &epi._all;
set &epi.(where=(def_sub^="ALL")) all_&epi.;
%if &epi.=PPI_PERINATAL or &epi.=PPI_CHEMO or &epi.=PPI_RO %then %do; def_sub=def_name; %end;
run;

***;
%if &epi.^=PPI_RO %then %do;
proc sql;
create table epi_in_&epi. as
select a.*, coalescec(b.dx7, c.ms_drg) as final_code, coalescec(b.dx7_desc, c.ms_drg_desc) as final_code_desc, b.sex_flag
from &epi._all a
left join meta.icd_10_dx b on index(a.t_var,"DX_CD")>0 and a.code=substr(b.dx7,1,length(a.code))
left join meta.ms_drg c on a.t_var="MS_DRG" and a.code=c.ms_drg
order by def_sub, def_cat, code_position, code_int, t_var, final_code, id;
quit;

proc sort data=epi_in_&epi. nodupkey;
by def_sub def_cat code_int t_var final_code;
run;

proc sql;
create table &epi._drg_excl as
select distinct a1.*, a.*, case when a.code_int="X" then "EPI_IN_XX" else "" end length=10 as def_cat, b.final_code is not null as del_flag
from epi_in_&epi.(where=(t_var="MS_DRG") drop=def_cat id check: code_desc) a
left join a.ppi_list a1 on a.def_sub=a1.epi_id
left join epi_in_&epi.(where=(t_var="MS_DRG" and code_int="")) b
	on a.code_int="X" and a.def_sub=b.def_sub and a.t_var=b.t_var and a.final_code=b.final_code
having del_flag=0
order by def_sub, code_int, code;
quit;

proc sql;
create table &epi._dx_excl as
select a1.*, a.*, b.final_code is not null as del_flag
from epi_in_&epi.(where=(t_var^="MS_DRG")) a
left join a.ppi_list a1 on a.def_sub=a1.epi_id
left join epi_in_&epi.(where=(def_cat="EPI_IN_0&cat.")) b
	on a.def_cat^="EPI_IN_0&cat." and a.def_sub=b.def_sub and a.t_var=b.t_var and a.final_code=b.final_code and b.code_int^="2"
having del_flag=0
order by def_sub, def_cat, code_int, code_position, t_var, final_code;
quit;

data a.epi_in_&epi.;
set &epi._dx_excl &epi._drg_excl;
run;
%end;
%else %do;
proc sql;
create table a.epi_in_&epi. as
select distinct a1.episode_category, epi_type, clinical_category, "PPI_RO" as epi_id, "Rad Onc - All" as epi_name,
	a.*, a.code as final_code, b.cpt_desc as final_code_desc, b.sex_flag
from &epi._all a
left join a.ppi_list a1 on a1.epi_type="Radiation Oncology"
left join meta.cpt b on a.code=b.cpt
order by def_sub, def_cat, code_position, code_int, t_var, final_code, id;
quit;
%end;

proc sort data=a.epi_in_&epi.;
by def_sub def_cat code_int code_position t_var final_code;
run;

data a.epi_in_&epi.;
set a.epi_in_&epi.;
length time_window $50. code_p $100. in_excl $10. day $10.;

if index(t_var, "DX_CD")>0 then do;
	code_type="ICD_10_DX"; 
	if code_position="1" then do;
		if code_int="" then code_p="Principal, any seconday code"; else code_p="Principal, with a secondary code from the seconday code list";
	end;
	else if code_position="2" then code_p="Secondary code list";
	else code_p="Any position";
end;
else code_type=t_var;
if code_int="X" then in_excl="Exclude"; else in_excl="Include";

day=strip(put(check_beg_offset,3.))||"-"||strip(put(check_end_offset,3.));
if code_type="MS_DRG" then day="&day.";
time_window="Day "||strip(day)||" post-discharge";
%if &epi.=PPI_PERINATAL %then %do; 	time_window="9-month before to 2-month after delivery"; %end;
%if &epi.=PPI_CH_MED %then %do; 	time_window="Performance Year"; %end;
%if &epi.=PPI_CHEMO %then %do; 		time_window="180 days post trigger"; %end;
%if &epi.=PPI_RO %then %do; 		time_window="90 days post trigger"; epi_id="All"; %end;

if epi_id in ("PPI_IOP_PROC_P70" "PPI_IOP_PROC_P74" "PPI_AC_MED_X034" "PPI_AC_MED_X329" "PPI_CH_MED_Y265") then sex="M";
if epi_id in ("PPI_IOP_PROC_P75" "PPI_OP_PROC_P207" "PPI_OP_PROC_P208" "PPI_AC_MED_X040" "PPI_AC_MED_X083" "PPI_AC_MED_X346" "PPI_CH_MED_Y267") 
	then sex="F";
if sex="M" and sex_flag="F" then delete;
if sex="F" and sex_flag="M" then delete;

keep episode_category epi_type clinical_category epi_id epi_name in_excl time_window code_type code_p final_code final_code_desc;
run;

data a.epi_in_&epi.;
retain episode_category epi_type clinical_category epi_id epi_name in_excl time_window code_type code_p final_code final_code_desc;
set a.epi_in_&epi.;
run;

proc export data=a.epi_in_&epi. outfile="C:\Users\jun.wang\PPI3.0\def doc\epi_in_&epi..csv" replace; run;

%mend;

%epi_in(PPI_IP_PROC,4,%str(0-90));
%epi_in(PPI_IOP_PROC,4,%str(0-90));
%epi_in(PPI_IOP_MJRLE,3,%str(0-90));
%epi_in(PPI_OP_PROC,4,%str(0-30));
/*%epi_in(PPI_PR_PROC,4,%str(0-30));*/
/*%epi_in(PPI_IP_MED,3,%str(0-90));*/
%epi_in(PPI_AC_MED,2,%str(0-90));
%epi_in(PPI_CH_MED,1, );
%epi_in(PPI_RO,1, );


%epi_in(PPI_PERINATAL,1, );


data a;
set a.epi_in_&epi.;
where def_sub="PPI_IP_PROC_P09" and final_code="G450";
run;



data b;
set test;
where def_sub="PPI_IP_PROC_P09" and final_code="G450";
run;
