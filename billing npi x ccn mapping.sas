libname ccm "D:\SASData\SAS_Shared_Data\LDS_CCM";
libname out "D:\SASData\dua_052882\Users\Jun_Wang\ad hoc";
libname meta2 "D:\SASData\dua_052882\Users\Jun_Wang\LDS Test Stand\meta2";

*******************************************************;
*** create billing NPI - CCN mapping using FFS data ***;
*******************************************************;

/*Pull FFS CCM yearly facility data*/

data ffs_fac;
set ccm.medical_2016 ccm.medical_2017;
where clm_type not in ("PHYS" "DME");
if clm_type="OP" and setting="IP" then do;
	if substr(ccn,3,1) in ('0') then setting='IPA';
	else if substr(ccn,3,2) in ('13') then setting='CAH';
	else if substr(ccn,3,2) in ('40','41','42','43','44') or substr(ccn,3,1) in ('M','S') then setting='IPF';
	else if substr(ccn,3,2) in ('20','21','22') then setting='LTCH';
	else if "3025" le substr(ccn,3,4) le "3099" or substr(ccn,3,1) in ('R','T') then setting='IRF';
	else setting='IPO';
end;
if clm_type="OP" and setting="ER" then setting="OP";
run;

proc sql;
create table bill_npi_ccn as
select bill_npi, ccn, clm_type, setting, substr(ccn,1,2)||"X"||substr(ccn,4,3) as hos, count(*) as clm_cnt
from ffs_fac
group by bill_npi, ccn, clm_type, setting
order by bill_npi, ccn, clm_type, setting;
quit;


/*fix individual NPI as billing NPI*/

proc sql;
create table bill_npi_ccn_nppes as
select a.*, b.entity_code
from bill_npi_ccn a
left join meta2.npi_spec_x_new b
on a.bill_npi=b.npi;
quit;

proc sql;
create table fix_npi as
select *
from bill_npi_ccn_nppes
where ccn in (select distinct ccn from bill_npi_ccn_nppes where entity_code="1")
order by ccn, entity_code, clm_cnt desc;
quit;
proc sort data=fix_npi nodupkey; by ccn entity_code; run;

proc sql;
create table out.fix_npi as /* if a ccn has billed with both individual and org NPI, use org NPI as the fixed NPI */
select a.bill_npi as individual_npi, b.bill_npi as fixed_org_npi
from fix_npi(where=(entity_code="1")) a
inner join fix_npi(where=(entity_code="2")) b
on a.ccn=b.ccn;
quit;

/* fix billing NPI in FFS */
proc sql;
create table bill_npi_new_ccn as
select a.*, coalescec(b.fixed_org_npi, a.bill_npi) as bill_npi_new
from bill_npi_ccn a
left join out.fix_npi b
on a.bill_npi=b.individual_npi;
quit;

proc sql;
create table bill_npi_new_ccn as
select *, count(distinct ccn) as ccn_cnt, count(distinct hos) as hos_cnt, max(clm_cnt)=clm_cnt as max_flag
from bill_npi_new_ccn
group by bill_npi_new, clm_type
order by bill_npi_new, clm_type, max_flag desc, ccn;
quit;

proc freq data=bill_npi_new_ccn;
tables clm_type*ccn_cnt*hos_cnt/list missing;
run;

data bill_npi_ccn_s bill_npi_ccn_m bill_npi_ccn_f; 
set bill_npi_new_ccn; 
if ccn_cnt=1 then output bill_npi_ccn_s; /* 1-to-1 CCN match */
if ccn_cnt>1 and hos_cnt=1 then output bill_npi_ccn_m; /* 1-to-multiple CCN (single hospital) match */
if hos_cnt>1 then output bill_npi_ccn_f; /* 1-to-multiple hospital match */
run; 


/* 1-to-1 CCN match */
proc sort data=bill_npi_ccn_s; by bill_npi_new clm_type ccn descending max_flag; run;
proc sort data=bill_npi_ccn_s out=out.bill_npi_ccn_s nodupkey; by bill_npi_new clm_type ccn; run;


/* 1-to-m CCN match */

proc sort data=bill_npi_ccn_m; by bill_npi_new clm_type setting descending clm_cnt; run;
proc sort data=bill_npi_ccn_m nodupkey; by bill_npi_new clm_type setting; run;
proc transpose data=bill_npi_ccn_m out=bill_npi_ccn_m1;
by bill_npi_new clm_type;
var ccn;
id setting;
run;

proc sort data=bill_npi_new_ccn out=max_ccn nodupkey; where max_flag=1; by bill_npi_new clm_type; run;

proc sql;
create table out.bill_npi_ccn_m as
select distinct b.*, c.ccn as max_ccn
from bill_npi_ccn_m1 b, max_ccn c
where c.clm_type=b.clm_type and c.bill_npi_new=b.bill_npi_new
order by bill_npi_new, clm_type;
quit;

/* 1-to-multiple hospital match */
/* Identify date range for each CCN */

proc sql;
create table ffs_fac_npi_new as
select a.*, coalescec(b.fixed_org_npi, a.bill_npi) as bill_npi_new
from ffs_fac a
left join out.fix_npi b
on a.bill_npi=b.individual_npi;
quit;

proc sql;
create table out.fac_ccn_dt as
select distinct a.bill_npi_new, a.clm_type, a.ccn, count(*) as clm_cnt, min(thru_dt) format=mmddyy10. as first_dt, max(thru_dt) format=mmddyy10. as last_dt
from ffs_fac_npi_new a, bill_npi_ccn_f c 
where a.bill_npi_new=c.bill_npi_new and a.clm_type=c.clm_type and a.ccn=c.ccn
group by a.bill_npi_new, a.clm_type, a.ccn
order by bill_npi_new, clm_type, first_dt desc, last_dt;
quit;



*********************************************************;
*** Applying FFS billing NPI - CCN mapping to MA data ***;
*********************************************************;

data ma_fac;
set ccm.medical_2016 ccm.medical_2017;
where clm_type not in ("PHYS" "DME");
run;

proc sql;
create table ma_fac_npi_new as
select distinct a.*, coalescec(b.fixed_org_npi, a.bill_npi) as bill_npi_new
from ma_fac a
left join out.fix_npi b on a.bill_npi=b.individual_npi;
quit;

/* 1-to-m CCN match */
/* assign ccn based on MS-DRG*/
proc sql;
create table ma_fac_ccn_m as
select distinct a.bene_id, a.clm_id, a.syskey, a.ms_drg, a.setting, /*a.ccn,*/ a.bill_npi_new, c.*
from ma_fac_npi_new a, out.bill_npi_ccn_m c 
where a.clm_type=c.clm_type and a.bill_npi_new=c.bill_npi_new;
quit;

data ma_fac_ccn_m;
set ma_fac_ccn_m;
if clm_type="IP" then do;
	if ipa^="" then do;
		if irf^="" and ms_drg in ("056" "057" "559" "560" "561" "945" "946" "949" "950") then ccn_new=irf; 
		else if ipf^="" and "876"<=ms_drg<="887" then ccn_new=ipf; 
		else ccn_new=ipa; 
	end;
	if ipa="" and ipf^="" then do;
		if irf^="" and ms_drg in ("056" "057" "559" "560" "561" "945" "946" "949" "950") then ccn_new=irf; 
		else ccn_new=ipf; 
	end;
end;
if clm_type="OP" then ccn_new=max_ccn;

run;


/* 1-to-multiple hospital match */
/* Map possible CCN to claim by date*/
proc sql;
create table ma_fac_ccn_dt as
select a.bene_id, a.clm_id, a.syskey, a.thru_dt, a.ms_drg, a.clm_type, a.setting, b.*, b.ccn as ccn_new
from ma_fac_npi_new a, out.fac_ccn_dt b
where a.bill_npi_new=b.bill_npi_new and a.clm_type=b.clm_type
order by bene_id, clm_id, first_dt, last_dt;
quit;

data ma_fac_ccn_f;
set ma_fac_ccn_dt;
by bene_id clm_id;
retain first last;

first=first_dt; last=last_dt;
if first_dt>last then first=last; 
if last_dt>last then last=last_dt;

if first_dt<=thru_dt<=last_dt then flag=1;
if first.clm_id and thru_dt<first_dt then flag=1;
if last=last_dt and thru_dt>last then flag=1;
run;

/* For CCNs with matched date range, choose the high volume CCN */

proc sort data=ma_fac_ccn_f(where=(flag=1)); by bene_id clm_id descending clm_cnt; run;
proc sort data=ma_fac_ccn_f nodupkey; by bene_id clm_id; run;


**********************;
*** final mapping ***;

proc sql;
create table ma_fac_ccn_mapping as
select a.*, coalescec(b.ccn_new, c.ccn_new, d.ccn) as ccn_new
from ma_fac_npi_new a
left join ma_fac_ccn_m b on a.bene_id=b.bene_id and a.clm_id=b.clm_id
left join ma_fac_ccn_f c on a.bene_id=c.bene_id and a.clm_id=c.clm_id
left join out.bill_npi_ccn_s d on a.bill_npi=d.bill_npi and a.clm_type=d.clm_type
;
quit;

proc sql;
select clm_type, sum(ccn=ccn_new)/count(*) as ccn_pct
from ma_fac_ccn_mapping
group by clm_type;
quit;





