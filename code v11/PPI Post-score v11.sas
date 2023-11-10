***************************************************;
*********** CJ PPI3 Episode Aggregation ***********;
***************************************************;
%let py=2017;

%let agg_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg;  
%let builder_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\builder_output;
%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11;  

*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

libname in "&agg_loc.\py&py." access=readonly;
libname out "&agg_loc.\post_py&py." compress=char; 
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2" access=readonly;

filename logfile "&agg_loc.\post_py_&py.\ppi_post_score_&sysdate9..log";

proc printto log=logfile print=listfile new;
run;
%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Post-score MACROs v11.sas";

%macro report(prvdr);
	%epi_report(epi_spec);
	%epi_report(setting);
	%epi_report(cost_bd);
	%epi_report(pc);
	%comb_rank;
%mend;

%report(attr_npi);
%report(attr_tin);
%report(attr_p_bill);
%report(attr_f_bill);
%report(attr_ccn);

%report_var;


proc printto;
run;


