**************************************;
*********** CJ PPI3 Export ***********;
**************************************;

%let py=2017; /* set the performance year, which is the last year of 3-year aggregation */

%let epi_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\agg;  /*put the location of the input data (Year Agg output)*/
%let tcc_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\tcc; /*put the location of the TCC output tables*/
%let export_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\export;  /*put the location of the export tables*/


*****************************;
****** no change below ******;
*****************************;
options mlogic mprint;

libname epi "&epi_loc.\py&py." access=readonly; 
libname tcc "&tcc_loc.\py&py." access=readonly; 
libname out "&export_loc.\py&py." compress=char; 

filename logfile "&export_loc.\py&py.\ppi_export_&sysdate9..log";
filename listfile "&export_loc.\py&py.\ppi_export_&sysdate9..list";

proc printto log=logfile print=listfile new; run;

%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Export MACROs v11.sas";

%export(attr_npi);
%export(attr_tin);
%export(attr_p_bill);
%export(attr_f_bill);
%export(attr_ccn);

%export_epi_var;

%char_to_num(pc, description);
%char_to_num(cbd, service_description);
%char_to_num(cbd, service_details);

%export_num(pc);
%export_num(cbd);

proc printto; run;



