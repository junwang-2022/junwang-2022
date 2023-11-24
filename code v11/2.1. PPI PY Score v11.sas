************************************************;
*********** CJ PPI3 Provider Scoring ***********;
************************************************;

%let py=2017; /* set the performance year, which is the last year of 3-year aggregation */

%let agg_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg;  /*put the location of the input data (Year Agg output)*/
%let meta2_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2;  /*put the location of the common metadata tables*/
%let npi_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\NPI; /*put the location of the NPI xwalk tables*/
%let tcc_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\tcc; /*put the location of the TCC output tables*/

%let db=LDS; /* set this only if run with LDS data*/

*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

libname out "&agg_loc.\py&py." compress=char; 
libname tcc "&tcc_loc.\py&py." compress=char; 

libname meta "&meta_loc" access=readonly;
libname meta2 "&meta2_loc" access=readonly;
libname npi "&npi_loc" access=readonly;

libname y1 "&agg_loc.\year_&py." access=readonly;
libname y2 "&agg_loc.\year_%sysevalf(&py.-1)" access=readonly;
libname y3 "&agg_loc.\year_%sysevalf(&py.-2)" access=readonly;

filename logfile "&agg_loc.\py&py.\ppi_score_&sysdate9..log";
filename listfile "&agg_loc.\py&py.\ppi_score_&sysdate9..list";

proc printto log=logfile print=listfile new;
run;
%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI PY Score MACROs v11.sas";


%macro run_def;
%let timer_beg = %sysfunc(datetime());

%do i_def = 31 %to 41;

proc datasets lib=work nolist memtype=data nodetails ;
quit;

proc sql;
select def_name into :epi
from out.def_to_run
where monotonic()=&i_def.;
quit;
%let epi=%trim(&epi.);

%combine_year;

%macro score(prvdr);
	%map_cbsa;
	%rank(def_sub,index_setting,1);
		%if %index(&epi., _IOP_) or %index(&epi., _OP_) or %index(&epi., _PR_) %then %do;
		%rollup_setting;
		%end;
	%add_benchmark(pc, index_setting, type, type, description, description);
	%add_benchmark(cbd, index_setting, phase, clm_type, svc_cat, svc_sub);
%mend;

	%score(attr_npi);
	%score(attr_tin);
	%score(attr_p_bill);
	%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_TCC and &epi. ne PPI_RO %then %do;
		%score(attr_f_bill);
		%score(attr_ccn);
	%end;
%end;

data _null_; dur = datetime() - &timer_beg.; put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-'; run;
%mend run_def;
%run_def;

proc printto;
run;

