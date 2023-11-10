
***************************************************;
*********** CJ Episode Builder Run Code ***********;
***************************************************;

%let input_loc= 	D:\SASData\SAS_Shared_Data\shared\CCM\PROD\chc;  /*put the location of the input data*/
%let elig_loc=		D:\SASData\SAS_Shared_Data\shared\CCM\PROD\elig;	/*put the location of the input data for enrollment and esrd*/
%let output_loc= 	D:\SASData\dua_052882\Sndbx\Jun_W\CHC QM\builder output;  /*put the location where to save list and log and output data*/
%let meta_loc= 		D:\SASData\dua_052882\Sndbx\Jun_W\CHC QM\meta\VRDC_25OCT2023;  /*put the location of the metadata tables*/
%let task_file= 	D:\SASData\dua_052882\Sndbx\Jun_W\CHC QM\meta\VRDC_25OCT2023\CHC_builder_task_run_all.csv; /*put the location and name for task file*/

*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

/*dm "log; clear; output; clear;";*/

/*%LET util_root=/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Scott_W/utils; */
%let py=2021;
libname in "&input_loc." access=readonly; 
libname elig "&elig_loc." access=readonly; 
libname out "&output_loc.\PY&py."; 
libname meta "&meta_loc." access=readonly;

/*%INCLUDE "&util_root./Rsubmit_Macros.sas";*/

data WORK.TASK_RUN  ;
infile "&task_file"	delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
 informat ACTION_FLAG best32. ;
 informat DEF_SRC $10. ;
 informat DEF_TYPE $10. ;
 informat DEF_NAME $20. ;
 informat PRF_BEG mmddyy10. ;
 informat PRF_END mmddyy10. ;
 informat ENROLL_TYPE  $15. ;
 informat MIN_AGE best32. ;
 informat MAX_AGE best32. ;
 format ACTION_FLAG best12. ;
 format DEF_SRC $10. ;
 format DEF_TYPE $10. ;
 format DEF_NAME $20. ;
 format DATA_BEG mmddyy10. ;
 format DATA_END mmddyy10. ;
 format PRF_BEG mmddyy10. ;
 format PRF_END mmddyy10. ;
 format ENROLL_TYPE  $15. ;
 format MIN_AGE best32. ;
 format MAX_AGE best32. ;
input
          ACTION_FLAG
          DEF_SRC  $
          DEF_TYPE  $
          DEF_NAME  $
          PRF_BEG
          PRF_END
		  ENROLL_TYPE $
          MIN_AGE  
          MAX_AGE  
;
run;

data out.def_to_run;
	set TASK_RUN;
	where not missing(action_flag) and prf_beg=mdy(1,1,&py.);;
	call symputx('n_def_to_run', _n_);
run;

%put note: There are &n_def_to_run. definition(s) to run in this session.;

** determine the start and end year/month of CCM data **;
/*proc sql noprint;*/
/*select 	year(min(intnx(COALESCE(check_beg_offset_unit,'DAY'), a.prf_beg, COALESCE(check_beg_offset,0), 'S'))), */
/*		month(min(intnx(COALESCE(check_beg_offset_unit,'DAY'), a.prf_beg, COALESCE(check_beg_offset,0), 'S'))),*/
/*		year(max(intnx(COALESCE(check_end_offset_unit,'DAY'), a.prf_end, COALESCE(check_end_offset,0), 'S'))), */
/*		month(max(intnx(COALESCE(check_end_offset_unit,'DAY'), a.prf_end, COALESCE(check_end_offset,0), 'S')))*/
/*		into :beg_year, :beg_month, :end_year, :end_month*/
/*from out.def_to_run a, meta.def_spec b*/
/*where a.def_name=b.def_name and b.def_cat='LEVEL';*/
/*quit;*/

%macro setdata(table);
/*data &table.;*/
/*set in.&table._%trim(&beg_year.)-in.&table._%trim(&end_year.);*/
/*where mdy(&beg_month.,1,&beg_year.)<= %if &table.=pharmacy %then fill_dt; %else thru_dt; <=mdy(&end_month.,day(intnx('month',mdy(&end_month.,1,&end_year.),0,'e')),&end_year.);*/
/*run;*/
data &table.;
set in.&table:;
run;
%mend;
%setdata(medical);
%setdata(diagnosis);
%setdata(procedure);
%setdata(serviceline);
%setdata(pharmacy);
D:\SASData\dua_052882\Sndbx\Jun_W\CHC QM\builder output
%let enroll	= elig.chc_enrollment;
%let esrd	= elig.chc_mdcr_status;

/**************************************************************************************/

filename logfile "&output_loc./PY&py./CHC_QM_log_&sysdate9..log";
proc printto log=logfile new;
run;

%let timer_beg = %sysfunc(datetime());

%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\Episode buider MACROs v11.sas";

%macro run_def;
data _null_;
set out.def_to_run;
call symputx('n_def_to_run', _n_);
run;

%do i_def = 28 %to 28 /*&n_def_to_run.*/;
	proc datasets lib=WORK nolist memtype=data nodetails ;
	save medical diagnosis procedure serviceline pharmacy ;
	quit;

	%let timer_start = %sysfunc(datetime());

	data curr_def_to_run;
		set out.def_to_run;
		if _n_=&i_def.;
	run;

	%reset_global_macro_var;
	%prep_def;
	%parse_code;
	%trigger_identification;
	%eligibility_check;
	%claim_subset;
	%pt_exclusion;
	%service_inclusion;
	%rf;

	data _null_; dur = datetime() - &timer_start.; put 30*'-' / " DURATION FOR Part_&part. &DEF_NAME.:" dur time13.2 / 30*'-'; run;
%end;

%mend run_def;

%run_def;

data _null_; dur = datetime() - &timer_beg.; put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-'; run;

proc printto;
run;

D:\SASData\dua_052882\Sndbx\Jun_W\CHC QM\builder output


D:\SASData\SAS_Shared_Data\shared\CCM\PROD\chc

D:\SASData\SAS_Shared_Data\shared\CCM\TEST\chc
