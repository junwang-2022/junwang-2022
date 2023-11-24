libname o1 "D:\sas\cj\data\Demo_Data\Output1\update";
libname o2 "D:\sas\cj\data\Demo_Data\Output2\update";
libname o3 "D:\sas\cj\data\Demo_Data\Output3\update";
libname sample "D:\sas\cj\data\Demo_Data\sample";
libname demo "D:\sas\cj\data\Demo_Data\demo data";

options mprint mlogic;

*** sampling ***;

%macro bene_sample(output,n);
proc sql;
create table &output._mbi as
select distinct src_bene_mbi_id, "&output." as source
from &output..cclf_8_bene_demo
where src_bene_mbi_id ne "" and substr(put(src_bene_zip_cd,5.),1,2) in ('60' '46' '47')
order by src_bene_mbi_id;
quit;

data &output._mbi;
set &output._mbi(obs=&n.);
run;
%mend bene_sample;
%bene_sample(o1,9000);
%bene_sample(o2,9000);
%bene_sample(o3,2000);

data demo_bene;
set o1_mbi o2_mbi o3_mbi;
run;

proc sort data=demo_bene out=sample.demo_bene nodupkey;
by src_bene_mbi_id;
run;

%macro pull(tbl);
proc sql;
create table &output._&tbl. as 
select a.* 
from &output..&tbl. a, demo_bene b
where b.source="&output." and a.src_bene_mbi_id=b.src_bene_mbi_id;
quit;
%mend;

%macro data_sample(output);

%pull(cclf_1_pt_a_clm_hdr);
%pull(cclf_2_pt_a_clm_rev_ctr_det);
%pull(cclf_3_pt_a_proc_cd);
%pull(cclf_4_pt_a_diag_cd);
%pull(cclf_5_pt_b_phys);
%pull(cclf_6_pt_b_dme);
%pull(cclf_7_pt_d);
%pull(cclf_8_bene_demo);

proc sql;
create table &output._cclf_9_bene_xref as 
select * 
from &output..cclf_9_bene_xref
where src_crnt_num in (select src_bene_mbi_id from &output._mbi) 
	or src_prvs_num in (select src_bene_mbi_id from &output._mbi) ;
quit;

%pull(cclf_assgn_1_hcc);
%pull(cclf_assgn_1_summ);
%pull(cclf_assgn_2_tin);
%pull(cclf_assgn_3_ccn);
%pull(cclf_assgn_4_tin_npi);
%pull(cclf_assgn_5_turnover);
%pull(cclf_assgn_6_assgnbl);
%mend;

%data_sample(o1);
%data_sample(o2);
%data_sample(o3);

%macro combine(tbl);
data sample.&tbl.;
set o1_&tbl. o2_&tbl. o3_&tbl.;
run;
%mend;

%combine(cclf_1_pt_a_clm_hdr);
%combine(cclf_2_pt_a_clm_rev_ctr_det);
%combine(cclf_3_pt_a_proc_cd);
%combine(cclf_4_pt_a_diag_cd);
%combine(cclf_5_pt_b_phys);
%combine(cclf_6_pt_b_dme);
%combine(cclf_7_pt_d);
%combine(cclf_8_bene_demo);
%combine(cclf_9_bene_xref);

%combine(cclf_assgn_1_hcc);
%combine(cclf_assgn_1_summ);
%combine(cclf_assgn_2_tin);
%combine(cclf_assgn_3_ccn);
%combine(cclf_assgn_4_tin_npi);

%macro combine2(tbl);
data sample.&tbl.;
set o1_&tbl. o2_&tbl.;
run;
%mend;
%combine2(cclf_assgn_5_turnover);
%combine2(cclf_assgn_6_assgnbl);

%macro set(tbl);
data sample.&tbl.;
set o1.&tbl.;
run;
%mend;
%set(cclf_assgn_0_header);
%set(cclf_benchmark_1_detail);
%set(cclf_expu_0_header);
%set(cclf_expu_1_detail);
%set(cclf_expu_2_regional);
%set(cclf_expu_3_snf);

*** create demo data tables ***;

*** reverse FIPS state code in cclf_1_pt_a_clm_hdr ***;
proc freq data=sample.cclf_1_pt_a_clm_hdr;
tables src_prvdr_fac_fips_st_cd;
run;

data fips;
set sample.cclf_1_pt_a_clm_hdr;
length x $2.;
if src_prvdr_fac_fips_st_cd="03" then x="00";
if src_prvdr_fac_fips_st_cd="26" then x="01";
if src_prvdr_fac_fips_st_cd="13" then x="02";
if src_prvdr_fac_fips_st_cd="52" then x="04";
if src_prvdr_fac_fips_st_cd="44" then x="05";
if src_prvdr_fac_fips_st_cd="01" then x="06";
if src_prvdr_fac_fips_st_cd="30" then x="08";
if src_prvdr_fac_fips_st_cd="93" then x="09";
if src_prvdr_fac_fips_st_cd="85" then x="10";
if src_prvdr_fac_fips_st_cd="27" then x="11";
if src_prvdr_fac_fips_st_cd="11" then x="12";
if src_prvdr_fac_fips_st_cd="29" then x="13";
if src_prvdr_fac_fips_st_cd="73" then x="15";
if src_prvdr_fac_fips_st_cd="74" then x="16";
if src_prvdr_fac_fips_st_cd="10" then x="17";
if src_prvdr_fac_fips_st_cd="32" then x="18";
if src_prvdr_fac_fips_st_cd="91" then x="19";
if src_prvdr_fac_fips_st_cd="31" then x="20";
if src_prvdr_fac_fips_st_cd="28" then x="21";
if src_prvdr_fac_fips_st_cd="25" then x="22";
if src_prvdr_fac_fips_st_cd="78" then x="23";
if src_prvdr_fac_fips_st_cd="24" then x="24";
if src_prvdr_fac_fips_st_cd="76" then x="25";
if src_prvdr_fac_fips_st_cd="17" then x="26";
if src_prvdr_fac_fips_st_cd="83" then x="27";
if src_prvdr_fac_fips_st_cd="47" then x="28";
if src_prvdr_fac_fips_st_cd="64" then x="29";
if src_prvdr_fac_fips_st_cd="80" then x="30";
if src_prvdr_fac_fips_st_cd="58" then x="31";
if src_prvdr_fac_fips_st_cd="69" then x="32";
if src_prvdr_fac_fips_st_cd="08" then x="33";
if src_prvdr_fac_fips_st_cd="23" then x="34";
if src_prvdr_fac_fips_st_cd="86" then x="35";
if src_prvdr_fac_fips_st_cd="16" then x="36";
if src_prvdr_fac_fips_st_cd="94" then x="37";
if src_prvdr_fac_fips_st_cd="06" then x="38";
if src_prvdr_fac_fips_st_cd="97" then x="39";
if src_prvdr_fac_fips_st_cd="90" then x="40";
if src_prvdr_fac_fips_st_cd="84" then x="41";
if src_prvdr_fac_fips_st_cd="07" then x="42";
if src_prvdr_fac_fips_st_cd="38" then x="44";
if src_prvdr_fac_fips_st_cd="71" then x="45";
if src_prvdr_fac_fips_st_cd="70" then x="46";
if src_prvdr_fac_fips_st_cd="68" then x="47";
if src_prvdr_fac_fips_st_cd="98" then x="48";
if src_prvdr_fac_fips_st_cd="88" then x="49";
if src_prvdr_fac_fips_st_cd="92" then x="50";
if src_prvdr_fac_fips_st_cd="96" then x="51";
if src_prvdr_fac_fips_st_cd="99" then x="53";
if src_prvdr_fac_fips_st_cd="61" then x="54";
if src_prvdr_fac_fips_st_cd="81" then x="55";
if src_prvdr_fac_fips_st_cd="02" then x="56";
if src_prvdr_fac_fips_st_cd="35" then x="66";
if src_prvdr_fac_fips_st_cd="19" then x="72";
if src_prvdr_fac_fips_st_cd="00" then x="78";
drop src_prvdr_fac_fips_st_cd;
run;

data demo.cclf_1_pt_a_clm_hdr;
set fips;
rename x=src_prvdr_fac_fips_st_cd;
run;

*** truncate outliers ***;
%macro outlier(tbl);
%local varlst ptlname currvar p_var;
proc sql;
select name into :varlst separated by " "
from sashelp.vcolumn
where libname="SAMPLE" and type="num" and memname="%upcase(&tbl.)" and 
	(substr(name, length(name)-3, 4)="_AMT" or substr(name, length(name)-2, 3)="_AM" 
	or substr(name, length(name)-1, 2)="_A" or index(name,"_QTY")>0);
quit;

proc sql;
select "P_"||substr(name,5,length(name)-4) into :ptlname separated by " "
from sashelp.vcolumn
where libname="SAMPLE" and type="num" and memname="%upcase(&tbl.)" and 
	(substr(name, length(name)-3, 4)="_AMT" or substr(name, length(name)-2, 3)="_AM" 
	or substr(name, length(name)-1, 2)="_A" or index(name,"_QTY")>0);
quit;

proc univariate data=sample.&tbl. noprint;
var &varlst.;
output out=&tbl._ol pctlpre=&ptlname. pctlpts=99;
run;

data demo.&tbl.;
merge sample.&tbl. &tbl._ol;
%do i=1 %to %sysfunc(countw(&varlst.));
	%let currvar=%scan(&varlst.,&i);
	%let p_var=P_%substr(&currvar.,5,%length(&currvar.)-4)99;
	&currvar.=min(&currvar., &p_var.);
drop &p_var.;
%end;
run;
%mend;
%outlier(cclf_1_pt_a_clm_hdr);
%outlier(cclf_2_pt_a_clm_rev_ctr_det);
%outlier(cclf_5_pt_b_phys);
%outlier(cclf_6_pt_b_dme);
%outlier(cclf_7_pt_d);

proc sql;
select src_bene_race_cd, count(distinct src_bene_mbi_id) as bene_cnt
from sample.cclf_8_bene_demo
group by src_bene_race_cd;
quit;

data demo.cclf_8_bene_demo;
set sample.cclf_8_bene_demo;
if year(src_bene_dob)<1933 then do;
	src_bene_dob=mdy(01,01,1933);
	src_bene_age=90;
end;
if src_bene_race_cd=6 then src_bene_race_cd=3;
run;



*** check rare dx ***;
%macro dx;

data dx_b(keep=src_bene_mbi_id src_cur_clm_uniq_id dx);
set sample.cclf_5_pt_b_phys;
%do i=1 %to 9;
dx=src_clm_dgns_&i._cd;
if dx^="" then output;
%end;
run;
%mend;
%dx;

data demo_dx_all;
set dx_b sample.cclf_4_pt_a_diag_cd(keep=src_bene_mbi_id src_cur_clm_uniq_id src_clm_dgns_cd rename=(src_clm_dgns_cd=dx));
run;

proc sql;
create table demo_dx_cnt as
select distinct substr(dx,1,6) as dx6, count(distinct src_bene_mbi_id) as pt_cnt
from demo_dx_all
group by substr(dx,1,6)
order by pt_cnt;
quit;

libname ccm "D:\sas\ccm";

data diagnosis;
set ccm.diagnosis_2017 ccm.diagnosis_2016;
run;

proc sql;
create table ccm_dx_cnt as
select distinct substr(dx_cd,1,6) as dx6, count(distinct bene_id) as pt_cnt
from diagnosis
group by substr(dx_cd,1,6)
order by pt_cnt;
quit;

proc sql;
create table demo_dx_rare as
select *
from demo_dx_cnt
where pt_cnt<=2 and anyalpha(substr(dx6,1,1)) 
	and dx6 not in (select distinct dx6 from ccm_dx_cnt where pt_cnt>=2)
group by dx6
order by dx6;
quit;

proc sql noprint;
select "'"||strip(dx6)||"'" into:rare_dx_lst separated by " "
from demo_dx_rare; quit;

%macro change_dx;
data demo.cclf_5_pt_b_phys;
set demo.cclf_5_pt_b_phys;
%do i=1 %to 9;
if substr(src_clm_dgns_&i._cd,1,6) in (&rare_dx_lst.) then src_clm_dgns_&i._cd="";
%end;
if substr(src_clm_line_dgns_cd,1,6) in (&rare_dx_lst.) then src_clm_line_dgns_cd="";
if src_clm_dgns_1_cd="" then src_clm_dgns_1_cd=coalescec(src_clm_dgns_2_cd,src_clm_dgns_3_cd,src_clm_dgns_4_cd);
run;
%mend;
%change_dx;
data demo.cclf_4_pt_a_diag_cd;
set sample.cclf_4_pt_a_diag_cd;
if substr(src_clm_dgns_cd,1,6) in (&rare_dx_lst.) then src_clm_dgns_cd="";
run;



%macro rename_aco(tbl, type, name);
data demo.&tbl.;
set demo.&tbl.;
org_id="A0000";
fk_aco_id="A0000";

%if &type.=assgn or &type.=expu %then %do;
pk_&type._&name._id = tranwrd(pk_&type._&name._id, "A3822", "A0000");
pk_&type._&name._id = tranwrd(pk_&type._&name._id, "A3632", "A0000");
pk_&type._&name._id = tranwrd(pk_&type._&name._id, "A1052", "A0000");
src_pk_&type._&name._id = tranwrd(src_pk_&type._&name._id, "A3822", "A0000");
src_pk_&type._&name._id = tranwrd(src_pk_&type._&name._id, "A3632", "A0000");
src_pk_&type._&name._id = tranwrd(src_pk_&type._&name._id, "A1052", "A0000");
%if &name ne hdr %then %do;
	fk_&type._hdr_id = tranwrd(fk_&type._hdr_id, "A3822", "A0000");
	fk_&type._hdr_id = tranwrd(fk_&type._hdr_id, "A3632", "A0000");
	fk_&type._hdr_id = tranwrd(fk_&type._hdr_id, "A1052", "A0000");
%end;
%if &name.=hcc %then %do;
fk_assgn_summ_id = tranwrd(fk_assgn_summ_id, "A3822", "A0000");
fk_assgn_summ_id = tranwrd(fk_assgn_summ_id, "A3632", "A0000");
fk_assgn_summ_id = tranwrd(fk_assgn_summ_id, "A1052", "A0000");
%end;

%end;
run;
%mend;

%rename_aco(cclf_1_pt_a_clm_hdr);
%rename_aco(cclf_2_pt_a_clm_rev_ctr_det);
%rename_aco(cclf_3_pt_a_proc_cd);
%rename_aco(cclf_4_pt_a_diag_cd);
%rename_aco(cclf_5_pt_b_phys);
%rename_aco(cclf_6_pt_b_dme);
%rename_aco(cclf_7_pt_d);
%rename_aco(cclf_8_bene_demo);
%rename_aco(cclf_9_bene_xref);

%rename_aco(cclf_assgn_0_header,assgn,hdr);
%rename_aco(cclf_assgn_1_hcc,assgn,hcc);
%rename_aco(cclf_assgn_1_summ,assgn,summ);
%rename_aco(cclf_assgn_2_tin,assgn,tin);
%rename_aco(cclf_assgn_3_ccn,assgn,ccn);
%rename_aco(cclf_assgn_4_tin_npi,assgn,tin_npi);
%rename_aco(cclf_assgn_5_turnover,assgn,turnover);
%rename_aco(cclf_assgn_6_assgnbl,assgn,assgnbl);

%rename_aco(cclf_expu_0_header,expu,hdr);
%rename_aco(cclf_expu_1_detail,expu,detail);
%rename_aco(cclf_expu_2_regional,expu,regional);
%rename_aco(cclf_expu_3_snf,expu,snf);

data demo.cclf_benchmark_1_detail;
set demo.cclf_benchmark_1_detail;
org_id="A0000";
fk_aco_id="A0000";
pk_benchmark_1_id=tranwrd(pk_benchmark_1_id, "A3822", "A0000");
fk_benchmark_hdr_id=tranwrd(fk_benchmark_hdr_id, "A3822", "A0000");
src_pk_benchmark_1_id=tranwrd(src_pk_benchmark_1_id, "A3822", "A0000");
run;
