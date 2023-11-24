***************************************************;
*********** CJ PPI3 Episode Aggregation ***********;
***************************************************;
%let py=2021;
%let epi=PPI_IOP_MJRLE;

libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2" access=readonly;
libname ref "D:\SASData\SAS_Shared_Data\shared\ref" access=readonly;

%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v10\code v10\CHC PPI nested bundle MACROs.sas";

%macro nest(year);
%let input_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\agg\year_&year.;  
%let output_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\nested bundle\year_&year.;  
%let meta_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta v10;  
%let py_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\nested bundle\py&py.;  

*****************************;
****** no change below ******;
*****************************;
/*options mlogic mprint;*/

libname in "&input_loc." access=readonly; 
libname out "&output_loc." compress=char; 
libname meta "&meta_loc" access=readonly;
libname py "&py_loc.";
%let model=&output_loc.\model;


%readm_wt;
%risk_adj;
%bene_output;

%mend;
%nest(2019);
%nest(2020);
%nest(2021);

*************************************;
libname y1 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\nested bundle\year_&py.";
libname y2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\nested bundle\year_%sysevalf(&py.-1)";
libname y3 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\nested bundle\year_%sysevalf(&py.-2)";
libname out "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\nested bundle\py&py." compress=char; 
libname in "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\prod v10\agg\py&py.";

%combine_year;
%saving(attr_npi);


** physician episode count by ACO ***;

/*1. map ACO number to out.bene_attr_npi_ppi_iop_mjrle using bene_id and year*/
/*2. run the following rollup to get physician epsiode count for each ACO*/
/*3. export the output table*/

proc sql;
create table attr_npi_aco_epi_cnt as
select attr_npi, def_sub, aco_num, count(*) as aco_epi_cnt 
from out.bene_attr_npi_ppi_iop_mjrle
group by attr_npi, def_sub, aco_num
having aco_epi_cnt>=11
order by attr_npi, def_sub, aco_num;
quit;
