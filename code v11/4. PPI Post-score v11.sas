***************************************************;
*********** CJ PPI3 Episode Aggregation ***********;
***************************************************;
%let py=2017;

%let export_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\export;  /*put the location of the export tables*/ 
%let final_report_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\final_report;  /*put the location of the final reports*/
%let meta2_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2;  

*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

libname in "&export_loc.\py&py." access=readonly;
libname out "&final_report_loc.\py&py." compress=char; 
libname meta2 "&meta2_loc" access=readonly;

filename logfile "&final_report_loc.\py&py.\ppi_final_report_&sysdate9..log";
filename listfile "&final_report_loc.\py&py.\ppi_final_report_&sysdate9..list";

proc printto log=logfile print=listfile new;
run;
%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Post-score MACROs v11.sas";

%macro report(prvdr);
	%epi_report(epi_spec);
	%epi_report(setting);
	%epi_report(cbd);
	%epi_report(pc);
	%comb_rank_report;
%mend;

%report(attr_npi);
%report(attr_tin);
%report(attr_p_bill);
%report(attr_f_bill);
%report(attr_ccn);

%convert_char(pc);
%convert_char(cbd);

%epi_var_report;


proc printto;
run;
