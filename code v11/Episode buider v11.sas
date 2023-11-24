
***************************************************;
*********** CJ Episode Builder Run Code ***********;
***************************************************;
%let year=2017;

%let input_loc=D:\SASData\SAS_Shared_Data\LDS_CCM;  /*put the location of the input data*/
%let output_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\builder_output\year_&year.;  /*put the location where to save list and log and output data*/
%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11;  /*put the location of the metadata tables*/
%let task=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\Builder Task Run.csv; /*put the location and name for task file*/


*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

/*dm "log; clear; output; clear;";*/

libname in "&input_loc." access=readonly; 
libname out "&output_loc."; 
libname meta "&meta_loc" access=readonly;

filename logfile "&output_loc.\engine_&sysdate9..log";
filename listfile "&output_loc.\engine_&sysdate9..lst";


data TASK  ;
infile "&output_loc.\Builder Task Run.csv" 
	delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
 informat ACTION_FLAG best32. ;
 informat DEF_SRC $10. ;
 informat DEF_TYPE $10. ;
 informat DEF_NAME $20. ;
 informat PRF_BEG mmddyy10. ;
 informat PRF_END mmddyy10. ;
 informat ENROLL_TYPE  $10. ;
 informat MIN_AGE best32. ;
 informat MAX_AGE best32. ;
 format ACTION_FLAG best12. ;
 format DEF_SRC $10. ;
 format DEF_TYPE $10. ;
 format DEF_NAME $20. ;
 format PRF_BEG mmddyy10. ;
 format PRF_END mmddyy10. ;
 format ENROLL_TYPE  $10. ;
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
	set task;
	where not missing(action_flag);
	call symputx('n_def_to_run', _n_);
run;


/*** determine the start and end year/month of CCM data **;*/
/*proc sql noprint;*/
/*select 	year(min(intnx(check_beg_offset_unit, a.prf_beg, check_beg_offset, 'S'))), */
/*		month(min(intnx(check_beg_offset_unit, a.prf_beg, check_beg_offset, 'S'))),*/
/*		year(max(intnx(check_end_offset_unit, a.prf_end, check_end_offset, 'S'))), */
/*		month(max(intnx(check_end_offset_unit, a.prf_end, check_end_offset, 'S')))*/
/*		into :beg_year, :beg_month, :end_year, :end_month*/
/*from out.def_to_run a, meta.def_spec b*/
/*where a.def_name=b.def_name and b.def_cat='LEVEL';*/
/*quit;*/
/**/
%macro setdata(table);
data &table.;
set in.&table._2016 in.&table._2017;
run;
%mend;
%setdata(medical);
%setdata(diagnosis);
%setdata(procedure);
%setdata(serviceline);
%setdata(pharmacy);

%let enroll	= in.enrollment;
%let esrd	= in.mdcr_status;

proc printto log=logfile print=listfile new;
run;
%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\Episode buider MACROs v11.sas";
%check_max;

%macro run_def;
%let timer_beg = %sysfunc(datetime());

%do i_def =31 %to 31 /*&n_def_to_run.*/;
proc datasets lib=work nolist memtype=data nodetails ;
save medical diagnosis procedure serviceline pharmacy;
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
data _null_; dur = datetime() - &timer_start.; put 30*'-' / " DURATION FOR &DEF_NAME.:" dur time13.2 / 30*'-'; run;
%end;

data _null_; dur = datetime() - &timer_beg.; put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-'; run;
%mend run_def;
%run_def;

proc printto;
run;

quit;







