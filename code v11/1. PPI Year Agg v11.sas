***************************************************;
*********** CJ PPI3 Episode Aggregation ***********;
***************************************************;

%let year=2017; /* set the individual year for running this step */

%let input_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\builder_output;  /*put the location of the input data (builder output)*/
%let output_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg;  /*put the location of the output data and log file*/

%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11;  /*put the location of the builder metadata tables*/
%let meta2_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2;  /*put the location of the common metadata tables*/
%let npi_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\NPI; /*put the location of the NPI xwalk tables*/
%let ccm_loc=D:\SASData\SAS_Shared_Data\LDS_CCM; /*put the location of the CCM enrollment files */

%let model_max_samplesize=100000; /* set the max sample threshold for risk-adjustment model */

%let db=LDS; /* set this only if run with LDS data*/


*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

%let model=&output_loc.\year_&year.\model;

libname in "&input_loc.\year_&year." access=readonly; 
libname out "&output_loc.\year_&year." compress=char; 
libname meta "&meta_loc" access=readonly;
libname meta2 "&meta2_loc" access=readonly;
libname npi "&npi_loc" access=readonly;
libname ccm "&ccm_loc" access=readonly;

filename logfile "&output_loc.\year_&year.\ppi_year_agg_&sysdate9..log";
filename listfile "&output_loc.\year_&year.\ppi_year_agg_&sysdate9..list";


proc printto log=logfile print=listfile new;
run;

%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Year Agg MACROs v11.sas";

%macro run_def;
%let timer_beg = %sysfunc(datetime());

%do i_def = 31 %to 41;

proc datasets lib=work nolist memtype=data nodetails ;
quit;

proc sql;
select def_name, enroll_type into :epi, :enroll_type
from in.def_to_run
where monotonic()=&i_def.;
quit;
%let epi=%trim(&epi.);

%if &enroll_type.=COMM %then %do;
%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Score HHS-HCC MACRO v11.sas";
%end;
%else %do;
%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Score CMS-HCC MACRO v11.sas";
%end;

%epi_agg;
%pc;
%cost_bd_by_specialty;
%attribution;
%hcc;
%risk_adj;
%bene_output;
%end;

data _null_; dur = datetime() - &timer_beg.; put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-'; run;
%mend run_def;
%run_def;

proc printto;
run;



