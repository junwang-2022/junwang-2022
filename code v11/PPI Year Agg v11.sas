***************************************************;
*********** CJ PPI3 Episode Aggregation ***********;
***************************************************;

%let year=2017;

%let input_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\builder_output\year_&year.;  /*put the location where to save list and log and output data*/
%let output_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg\year_&year.;  /*put the location of the input data*/
%let model=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg\year_&year.\model;

%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11;  /*put the location of the metadata tables*/
%let meta2_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2;  /*put the location of the metadata tables*/

%let db=LDS;
%let model_max_samplesize=100000;
*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

libname in "&input_loc." access=readonly; 
libname out "&output_loc."; 
libname meta "&meta_loc" access=readonly;
libname meta2 "&meta2_loc" access=readonly;
libname ccm "D:\SASData\SAS_Shared_Data\LDS_CCM";

filename logfile "&output_loc.\ppi_year_agg_&sysdate9..log";



data out.del_epi;
input del_epi $30. ;
datalines; 
PPI_AC_MED_X043
PPI_AC_MED_X044
PPI_AC_MED_X502
PPI_AC_MED_X075
PPI_AC_MED_X087
PPI_AC_MED_X512
PPI_AC_MED_X103
PPI_AC_MED_X031
PPI_AC_MED_X024
PPI_AC_MED_X021
PPI_AC_MED_X093
PPI_AC_MED_X108
PPI_AC_MED_X110
PPI_AC_MED_X121
PPI_AC_MED_X128
PPI_AC_MED_X130
PPI_AC_MED_X526
PPI_AC_MED_X229
PPI_AC_MED_X233
PPI_AC_MED_X234
PPI_AC_MED_X237
PPI_AC_MED_X542
PPI_AC_MED_X543
PPI_AC_MED_X246
PPI_AC_MED_X291
PPI_AC_MED_X294
PPI_AC_MED_X546
PPI_AC_MED_X322
PPI_AC_MED_X362
PPI_AC_MED_X394
PPI_AC_MED_X395
PPI_CH_MED_Y057
PPI_CH_MED_Y212
PPI_AC_MED_X118
PPI_AC_MED_X242
;
run;

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



