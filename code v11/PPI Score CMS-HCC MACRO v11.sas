*** 2021 ***;
%let package_loc=D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2\V24_2021SAS;
%let package=V24_2021SAS;
%let hcc_label=V24H86L1;
%let hcc_hierarchy=V24H86H1;
%let N_CC=204; 

%let IAS1=IAS1012124;
%let IAS2=IAS2012124;
%let IAS3=IAS3012124;
%let FMNAME0=Y20Y21MC;

***;

libname library "&package_loc.\&package.\FORMAT";
filename in0 "&package_loc.\&package.\MACROS";

%include in0(&hcc_label.)/source2; /* HCC label */
%include in0(&hcc_hierarchy.)/source2; /* HCC hierarchy */

%macro hcc;

%if epi.=PPI_CH_MED %then %do;
proc sql;
create table &epi._hcc_dx as
select a.*, a.dx_cd as diag, b.bene_gender as sex, floor((b.prf_beg-b.bene_dob_dt)/365.25) as age
from in.&epi._hcc_dx a
left join (select distinct def_id, prf_beg from in.&epi._trigger) b
on a.def_id=b.def_id
order by def_sub, def_id; 
quit;
%end;

%else %do;
proc sql;
create table &epi._hcc_dx as
select a.*, a.dx_cd as diag, b.bene_gender as sex, floor((b.index_dt-b.bene_dob_dt)/365.25) as age
from in.&epi._hcc_dx a
left join in.&epi._trigger b
on a.def_sub=b.def_sub and a.def_id=b.def_id
order by def_sub, def_id; 
quit;
%end;

data out.hcc_&epi.(keep=def_sub def_id hcc:);
%&hcc_label.;
length CC $4. CC1-CC&N_CC. HCC1-HCC&N_CC. 3.;
retain CC1-CC&N_CC. 0;

array C(&N_CC.)  CC1-CC&N_CC.;
array HCC(&N_CC.) HCC1-HCC&N_CC.;

set &epi._hcc_dx;
by def_sub def_id;

if first.def_id then do;
  	do i=1 to &N_CC.;
  	  C(i)=0;
	end;
end;         

CC="9999";

if sex="2" and diag in ("D66", "D67") then CC="48"; 
else if age<18 and diag in ("J410", "J411", "J418", "J42",  "J430", "J431", "J432", "J438", "J439", 
	"J440", "J441", "J449", "J982", "J983") then CC="112";
else if (age < 6 or age > 18) and diag = "F3481" then CC="-1.0";
     
if CC ne "-1.0" and CC ne "9999" then do;
   IND=input(CC,4.);
   if 1 <= IND <= &N_CC. then C(IND)=1;
end;

else if CC="9999" then do;
   ** assignment 1 **;
   IND = input(left(put(diag,$&IAS1.&FMNAME0..)),4.);
   if 1 <= IND <= &N_CC. then C(IND)=1;
   ** assignment 2 **;
   IND = input(left(put(diag,$&IAS1.&FMNAME0..)),4.);
   if 1 <= IND <= &N_CC. then C(IND)=1;
   ** assignment 3 **;
   IND = input(left(put(diag,$&IAS1.&FMNAME0..)),4.);
   if 1 <= IND <= &N_CC. then C(IND)=1;
end; 

if last.def_id then do;
	%&hcc_hierarchy.;
	output;
end;
run;

%mend;

