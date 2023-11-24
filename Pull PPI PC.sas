%let year=2017;

libname out "D:\SASData\dua_052882\Sndbx\Jun_W\Others\PPI PC";
libname in "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\builder_output\year_&year." access=readonly; 
libname meta "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11" access=readonly;
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2" access=readonly;


%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\Episode buider MACROs v11.sas";

%macro run_def;
%do i_def =31 %to 41;
proc datasets lib=work nolist memtype=data nodetails ; quit;

data curr_def_to_run;
	set in.def_to_run;
	if _n_=&i_def.;
run;

%reset_global_macro_var;
%prep_def;
%parse_code;

proc append base=out.whole_code data=whole_codelst; run;
%end;

%mend run_def;
%run_def;


proc sql;
create table a as
select a.*, b.pac_rf, pac_desc
from a.whole_code a
left join meta.icd_10_dx b
on a.code=b.dx7 and ((a.code_type="PC" and def_name in ("PPI_AC_MED" "PPI_CH_MED")) or (a.code_type="DX" and def_name not in ("PPI_AC_MED" "PPI_CH_MED")));
quit;

proc sql;
create table b as
select distinct episode_type, clinical_category, a.def_name, a.def_sub, b.def_desc, episode_name, pac_rf, pac_desc
from a(where=(index(def_cat, "EPI_IN_")>0 and code_int^="X")) a
left join meta2.ppi_name_mapping b
on a.def_sub=b.def_sub
having pac_rf^=""
order by episode_type, clinical_category, def_name, def_sub, pac_rf;
quit;

proc sql;
create table out.ppi_pc_list as
select a.*, a.pac_rf=b.pac_rf as flag
from b(where=(def_sub^="ALL")) a
left join (select distinct pac_rf from b where def_sub="ALL") b
on a.pac_rf=b.pac_rf
order by episode_type, clinical_category, def_name, def_sub, flag, pac_rf;
quit;
proc export data=out.ppi_pc_list outfile="D:\SASData\dua_052882\Sndbx\Jun_W\Others\PPI PC\ppi_pc_list.csv" replace; run;
