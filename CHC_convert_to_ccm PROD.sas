/*********************************************************************
SAS Program Name: CHC_convert_to_ccm PROD.sas
Author: 	Jun Wang
Date 		5/25/2023
Description: Convert donwloaded CHC data files to final CCM
*********************************************************************/

options dlcreatedir;

%include "D:\SASData\dua_052882\prod\utils\sas_init.sas";

libname chc "%sysfunc(pathname(SH052882,L))/Change" compress=char;
libname ref "D:\SASData\SAS_Shared_Data\shared\ref";
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";
libname meta "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta v10";
libname drg "D:\SASData\dua_052882\Sndbx\Chris F\ad_hoc\builder_meta";

libname ccm "%sysfunc(pathname(SH052882,L))/CCM/PROD/chc" compress=char;
libname elig "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\elig";

options mprint mlogic;
proc sort data=ref.na_bill_npi_x_ccn out=na_bill_npi_x_ccn; by bill_npi year descending spend; run;
proc sort data=na_bill_npi_x_ccn nodupkey; by bill_npi year; run;

%macro update(yr);
*** update serviceline table ***;

data serviceline_&yr.;

length clm_type $4.; format clm_type $4.;
set chc.serviceline_&yr.;

if clm_type="HOS" then clm_type="HSP";
if clm_type="PR" then clm_type="PHYS";
apc_cd="";
line_dx_cd="";
charge_amt=src_line_charge/100;
allowed_amt=charge_amt;
paid_amt=charge_amt;
line_dnl_flag=0;

rename 
rev=rev_cd 
pos=line_pos_cd
svc_units=svc_unit
;
drop member_id claim_id from_dt thru_dt claim_837: pk_claim_837: src_line_charge;
run;

data ccm.serviceline_&yr.;
set serviceline_&yr.;
length line_setting $4.;

if clm_type="PHYS" then do;

if line_pos_cd='01' then line_setting='PHRM';
if line_pos_cd='02' then line_setting='TELE';
if line_pos_cd='11' then line_setting='OFFC';
if line_pos_cd in ('12' '14') then line_setting='HOME';
if line_pos_cd in ('13' '33') then line_setting='ASLF';
if line_pos_cd='15' then line_setting='MBLE';
if line_pos_cd in ('19' '22') then line_setting='OP';
if line_pos_cd='20' then line_setting='UCF';
if line_pos_cd='21' then line_setting='IP';
if line_pos_cd='23' then line_setting='ER';
if line_pos_cd='24' then line_setting='ASC';
if line_pos_cd='25' then line_setting='BRTH';
if line_pos_cd in ('31' '32') then line_setting='SNF';
if line_pos_cd='34' then line_setting='HSP';
if line_pos_cd in ('41' '42') then line_setting='AMBL';
if line_pos_cd='49' then line_setting='CLNC';
if line_pos_cd='50' then line_setting='FQHC';
if line_pos_cd in ('51' '52') then line_setting='IPF';
if line_pos_cd='53' then line_setting='CMHC';
if line_pos_cd='54' then line_setting='ICF';
if line_pos_cd in ('60' '71') then line_setting='PHC';
if line_pos_cd='61' then line_setting='IRF';
if line_pos_cd='62' then line_setting='ORF';
if line_pos_cd='65' then line_setting='ESRD';
if line_pos_cd='72' then line_setting='RHC';
if line_pos_cd='81' then line_setting='LAB';
if line_setting="" then line_setting='OTHR';

end;

run;

*** flag ER and ICU ***;

proc sql;
create table er as
select distinct bene_id, clm_id, 1 as er_flag
from ccm.serviceline_&yr.
where clm_type in ('IP' 'OP') and rev_cd='0981' or substr(rev_cd,1,3)="045";
quit;

proc sql;
create table icu as
select distinct bene_id, clm_id, 1 as icu_flag
from ccm.serviceline_&yr.
where clm_type='IP' and rev_cd in ('0200','0201','0202','0203','0204','0207','0208','0209');
quit;


*** roll up line pos ***;

proc sql;
create table lr_svc_&yr. as
select bene_id, clm_id, line_pos_cd as pos_cd, line_setting as setting, count(*) as line_cnt, sum(allowed_amt) as line_allowed
from ccm.serviceline_&yr.
group by bene_id, clm_id, line_pos_cd, line_setting
order by bene_id, clm_id, line_cnt desc, line_allowed desc;
quit;

proc sort data=lr_svc_&yr. nodupkey;
by bene_id clm_id;
run;

*** map provider spec and ccn ***;

proc sql;
create table medical_&yr. as
select a.*, b.er_flag, c.icu_flag, d.pos_cd, d.setting, e.spec_cd as prf_spec, f.ccn, g.final_drg, g.final_drg_avg_paid
from chc.general_&yr. a
left join er b on a.bene_id=b.bene_id and a.clm_id=b.clm_id
left join icu c on a.bene_id=c.bene_id and a.clm_id=c.clm_id
left join lr_svc_&yr. d on a.clm_type="PR" and a.bene_id=d.bene_id and a.clm_id=d.clm_id
left join %if &yr.=2018 %then ref.npi_x_spec_2019; %else ref.npi_x_spec_&yr.; e on a.clm_type="PR" and e.npi^="" and coalescec(at_npi, bill_npi)=e.npi
left join na_bill_npi_x_ccn f on a.clm_type^="PR" and a.bill_npi^="" and a.bill_npi=f.bill_npi and f.year=&yr.
left join chc.msdrg_&yr. g on a.bene_id=g.bene_id and a.clm_id=g.clm_id
order by bene_id, thru_dt, clm_id;
quit;

*** update medical table ***;

data ccm.medical_&yr.;
length clm_type $4.; format clm_type $4.;
set medical_&yr.;

syskey=(year(thru_dt)-2000)*1000000000000+clm_id;

if clm_type="HOS" then clm_type="HSP";
if clm_type="PR" then clm_type="PHYS";
freq_cd=substr(bill_type,3,1);
bill_type=substr(bill_type,1,2);
paid_dt=thru_dt+30;

if clm_type in ('IP' 'OP' 'SNF' 'HHA' 'HSP') then do;
	adm_dt=coalesce(adm_dt, from_dt);
	dis_dt=thru_dt;
end;

if ppx_cd^="" then ppx_dt=from_dt;

ms_drg=coalescec(final_drg,ms_drg);
allowed_amt=coalesce(final_drg_avg_paid,claim_paid_new);
charge_amt=claim_charge_amt/100;

paid_amt=claim_paid_new;
length prf_npi $10. op_npi $10.;
prf_npi=coalescec(at_npi, bill_npi);
op_npi=at_npi;

length tin $9.;
tin=strip(put(bill_tin, $9.));
if length(tin)=7 then tin="00"||tin;
if length(tin)=8 then tin="0"||tin;

bene_ssa_cnty_cd="";
bene_ssa_st_cd="";
prvdr_zip_cd=substr(bill_zip,1,5);

prpay_amt=0;
coin_amt=0;
ded_amt=0;
bene_rspn_amt=0;
dnl_flag=0;

setting=coalescec(setting,clm_type);

if clm_type="IP" then do;	
	if substr(ccn,3,1) in ('0') then setting='IPA';
	else if substr(ccn,3,2) in ('13') then setting='CAH';
	else if substr(ccn,3,2) in ('40','41','42','43','44') or substr(ccn,3,1) in ('M','S') then setting='IPF';
	else if substr(ccn,3,2) in ('20','21','22') then setting='LTCH';
	else if "3025" le substr(ccn,3,4) le "3099" or substr(ccn,3,1) in ('R','T') then setting='IRF';
	else setting='IPO';
end;

if clm_type="OP" then do;	
	if er_flag=1 then setting='ER';
	else if bill_type in ('13') then setting='OP';
	else if bill_type in ('85') then setting='CAH';
	else if bill_type in ('72') then setting='ESRD';
	else if bill_type in ('73','77') then setting='FQHC';
	else if bill_type in ('71') then setting='RHC';
	else if bill_type in ('12') then setting='IP';
	else if bill_type in ('22','23') then setting='SNF';
	else if bill_type in ('74','75') then setting='ORF';
	else if bill_type in ('76') then setting='CMHC';
	else if bill_type in ('34') then setting='HHA';
	else if bill_type in ('14') then setting='LAB';
	else if bill_type in ('83') then setting='ASC';
	else if bill_type in ('84') then setting='BRTH';
	else setting='OPO';
end;

rename 
claim_type_cd=clm_type_cd
member_zip=bene_zip_cd
adm_dx=adx_cd
ref_npi=rfr_npi
;

if clm_type="" then delete;

drop member_id record_type claim_id at_tin ref_tin fac_st bill_st bill_zip claim_837_visit_nk los drg_avg_paid 
	claim_allowed_amt std_paid: claim_paid: final_drg: claim_charge_amt;
run;


data ccm.diagnosis_&yr.;
set chc.diagnosis_&yr.;
if clm_type^="PR" then poa="Y";
rename dx=dx_cd;
drop member_id claim_id clm_type from_dt thru_dt;
run;

data ccm.procedure_&yr.;
set chc.procedure_&yr.;
px_dt=from_dt;
rename px=px_cd;
drop member_id claim_id clm_type from_dt thru_dt;
run;

proc datasets lib=work nolist memtype=data nodetails ;
save na_bill_npi_x_ccn;
quit;
%mend;

%update(2018);
%update(2019);
%update(2020);
%update(2021);

%macro add_case_id(yr);

%let next_yr=%sysevalf(&yr.+1);

data fac;
set ccm.medical_&yr. %if &yr.^=2021 %then ccm.medical_&next_yr.;;
where clm_type in ('IP' 'SNF' 'HHA' 'HSP' 'OP');

length dis $6.;
if dis_status in ('01' '08' '71' '72') then dis='HOME';
if dis_status='02' then dis='TRNSF';
if dis_status in ('03' '61' '64') then dis='SNF';
if dis_status='06' then dis='HHA';
if dis_status='07' then dis='LAMA';
if dis_status='30' then dis='STL_PT';
if dis_status in ('20' '40' '41' '42') then dis='DIED';
if dis_status in ('50' '51') then dis='HSP';
if dis_status='62' then dis='IRF';
if dis_status='63' then dis='LTCH';
if dis_status='65' then dis='IPF';

if dis='' then dis='OTHR';
drop case_id trnsfd_flag stay_dis_dt discharge;
run;


*** flag transferred claims ***;
proc sort data=fac; by bene_id adm_dt from_dt thru_dt dis_dt; run;

data transfer;
set fac; 
where clm_type='IP' and setting in ('IPA' 'CAH');
by bene_id;
retain temp_dt;

if first.bene_id then temp_dt=0;	
if temp_dt<=adm_dt<=temp_dt+1 then transferred=1; 
if dis_status='02' then temp_dt=dis_dt;

run;

proc sql;
create table fac2 as
select a.*, b.transferred=1 as trnsfd_flag
from fac a
left join transfer b
on a.syskey=b.syskey
order by bene_id, clm_type, ccn, adm_dt, dis_dt, thru_dt, from_dt;
quit;

*** add stay_id ***;

data stay_id;
set fac2(where=(clm_type^='OP')); 
by bene_id clm_type ccn adm_dt;
retain stay_id;
if first.adm_dt then stay_id=syskey;	
run;

proc sql;
create table stay_id2 as
select *, max(coalesce(dis_dt, thru_dt)) format=mmddyy10. as stay_dis_dt
from stay_id
group by bene_id, clm_type, ccn, stay_id
order by bene_id, clm_type, ccn, adm_dt, dis_dt, thru_dt, from_dt;
quit;

proc sql;
create table fac3 as
select a.*, coalesce(b.stay_dis_dt, a.dis_dt, a.thru_dt) format=mmddyy10. as stay_dis_dt, 
	coalesce(b.stay_id, a.syskey) as stay_id
from fac2 a
left join stay_id2 b
on a.syskey=b.syskey
order by bene_id, clm_type, ccn, adm_dt, dis_dt, thru_dt, from_dt;
quit;

*** fix discharge ***;
proc sql;
create table dis as
select a.*, b.setting as first_setting, b.clm_type as first_clm_type, b.clm_id as first_clm_id, b.trnsfd_flag,
	b.adm_dt as first_dt, b.dis_status as first_dis, 
	case when b.adm_dt>=a.stay_dis_dt then b.adm_dt-a.stay_dis_dt else 99 end as gap
from (select bene_id, clm_type, setting, clm_id, syskey, adm_dt, stay_dis_dt, dis_status, dis 
	  from fac3 where coalesce(dis_dt, thru_dt)=stay_dis_dt) a 
left join fac3(where=(clm_type^='OP')) b
on a.bene_id=b.bene_id and a.clm_id^=b.clm_id 
	and ((b.clm_type='HHA' and a.stay_dis_dt<=b.adm_dt<=a.stay_dis_dt+30) 
		 or (b.clm_type^='HHA' and a.stay_dis_dt<=b.adm_dt<=a.stay_dis_dt+7)
		 or b.adm_dt<a.stay_dis_dt<b.stay_dis_dt)
order by bene_id, adm_dt, clm_id, gap;
quit;

proc sort data=dis out=dis2 nodupkey;
by bene_id adm_dt clm_id;
run;

data dis3;
set dis2;

discharge=dis;
if dis='TRNSF' then do;
	if clm_type^='IP' or setting not in ('IPA' 'CAH') or trnsfd_flag^=1 then discharge=first_setting;
	if first_setting="" then discharge='HOME';
end;

else if dis not in ('LAMA' 'DIED' 'STL_PT') then do;
	if dis='OTHR' and first_setting="" then discharge='OTHR';
	else if first_setting="" then discharge='HOME';
	else if first_setting not in ('IPA' 'CAH') then discharge=first_setting;
end;

if discharge='CAH' then discharge='IPA';
if discharge='IPO' then discharge='OTHR';

run;

proc sql;
create table fac4 as
select a.*, coalescec(b.discharge, a.dis) length=6 as discharge
from fac3 a
left join dis3 b
on a.syskey=b.syskey;
quit;


*** add case_id ***;

*** map op fac claims to ip/snf ***;
proc sql;
create table case_id_op as
select distinct a.bene_id, a.clm_id, a.stay_id, a.from_dt, a.thru_dt, a.adm_dt, a.dis_dt, a.stay_dis_dt, a.clm_type, a.setting, a.bill_type,
	a.ccn, a.at_npi, a.op_npi, a.pdx_cd, a.er_flag, b.syskey as op_syskey, b.ccn as op_ccn, b.discharge, b.clm_id as op_clm_id, b.from_dt as op_from, b.thru_dt as op_thru, 
	b.setting as op_setting, b.pdx_cd as op_pdx
from fac4(where=(clm_type in ('IP' 'SNF'))) a
left join fac4(where=(clm_type='OP')) b
on a.bene_id=b.bene_id and a.adm_dt^=. and b.thru_dt between a.adm_dt and a.stay_dis_dt
order by bene_id, adm_dt, dis_dt;
quit;

data case_id_op2;
set case_id_op;
where op_clm_id^=.;
if adm_dt<op_thru<dis_dt then do;
	if ccn=op_ccn or op_setting=clm_type or op_setting='LAB' then rank=1;
end;
else do;
	if ccn=op_ccn and op_setting=clm_type then rank=1;
end;
run;

proc sort data=case_id_op2 nodupkey;
where rank^=.;
by op_syskey;
run;

proc sql;
create table fac5 as
select a.*, b.stay_id as op_stay_id
from fac4 a
left join case_id_op2 b
on a.syskey=b.op_syskey;
quit;
***;

data phys;
set ccm.medical_&yr. %if &yr.^=2021 %then ccm.medical_&next_yr.;;
where clm_type='PHYS';
keep bene_id clm_id clm_type syskey from_dt thru_dt setting pos_cd prf_npi prf_spec pdx_cd;
run;

proc sql;
create table case_id_fac as
select distinct a.bene_id, a.clm_id, a.stay_id, a.op_stay_id, a.from_dt, a.thru_dt, a.adm_dt, a.dis_dt, a.stay_dis_dt, a.clm_type, a.setting, a.bill_type,
	a.at_npi, a.op_npi, a.pdx_cd, a.er_flag, b.syskey as phys_syskey, b.from_dt as phys_from, b.thru_dt as phys_thru, 
	b.setting as phys_setting, b.pos_cd, b.prf_npi, b.pdx_cd as phys_pdx
from fac5 a
left join phys b
on a.bene_id=b.bene_id and a.adm_dt^=. and b.thru_dt between a.adm_dt-1 and a.stay_dis_dt+1;
quit;

proc sql;
create table case_id_asc as
select distinct a.bene_id, a.clm_id, a.clm_type, a.syskey as stay_id, a.from_dt, a.thru_dt, a.setting,
	a.pdx_cd, b.syskey as phys_syskey, b.from_dt as phys_from, b.thru_dt as phys_thru, 
	b.setting as phys_setting, b.pos_cd, b.prf_npi, b.pdx_cd as phys_pdx
from phys(where=(setting='ASC' and prf_spec='49')) a
left join phys(where=(prf_spec^='49')) b
on a.bene_id=b.bene_id and a.thru_dt^=. and b.thru_dt between a.from_dt-1 and a.thru_dt+1;
quit;

data case_id2;
set case_id_fac case_id_asc;

if clm_type='IP' then do;
	if adm_dt <= phys_thru <= dis_dt then do;
		if setting=phys_setting or phys_setting='IP' then rank=1;
		else if er_flag=1 and phys_setting='ER' then rank=1;
		else if at_npi=prf_npi or op_npi=prf_npi then rank=2;
	end;
	else do;
		if setting=phys_setting or phys_setting='IP' then rank=3;
		else if er_flag=1 and phys_setting='ER' and phys_thru<=adm_dt then rank=3;
		else if (at_npi=prf_npi or op_npi=prf_npi) and substr(pdx_cd,1,3)=substr(phys_pdx,1,3) then rank=4;
	end;
end;

else if clm_type='OP' then do;
	if adm_dt <= phys_thru <= dis_dt then do;
		if setting=phys_setting or phys_setting='OP' then rank=1;
		else if setting='OP' and phys_setting='IP' then rank=2;
		else if at_npi=prf_npi or op_npi=prf_npi then rank=2;
	end;
	else do;
		if setting=phys_setting or phys_setting='OP' then rank=3;
		else if setting='OP' and phys_setting='IP' then rank=4;
		else if (at_npi=prf_npi or op_npi=prf_npi) and substr(pdx_cd,1,3)=substr(phys_pdx,1,3) then rank=4;
	end;
end;

else if clm_type='SNF' then do;
	if adm_dt <= phys_thru <= dis_dt then do;
		if setting=phys_setting or (bill_type='18' and phys_setting='IP') then rank=1;
		else if at_npi=prf_npi or op_npi=prf_npi then rank=2;
	end;
	else do;
		if setting=phys_setting or (bill_type='18' and phys_setting='IP') then rank=3;
	end;
end;

else if clm_type in ('HSP' 'HHA') then do;
	if adm_dt <= phys_thru <= dis_dt and setting=phys_setting then rank=1;
	else if setting=phys_setting then rank=2;
end;

if phys_setting='AMBL' then do;
	if from_dt <= phys_thru <= thru_dt then do;
		if er_flag=1 then rank=1;
		else if clm_type='IP' then rank=2;
		else if clm_type='OP' then rank=3;
		else rank=4;
	end;
	else do;
		if er_flag=1 then rank=5;
		else if clm_type='IP' then rank=6;
		else if clm_type='OP' then rank=7;
		else rank=8;
	end;
end;

*** revise ASC case id logic ***;
if clm_type="PHYS" and setting='ASC' then do;
	if from_dt <= phys_thru <= thru_dt then do;
		if phys_setting='ASC' then rank=1;
		else if phys_setting='OP' then rank=2;
	end;
	else if phys_setting='ASC' and substr(pdx_cd,1,3)=substr(phys_pdx,1,3) then rank=3;
	else rank=.;
end;
***;

case_id=coalesce(op_stay_id,stay_id);

run;

proc sort data=case_id2 out=case_id3;
where rank^=.;
by phys_syskey rank;
run;
proc sort data=case_id3 nodupkey;
by phys_syskey;
run;

proc sql;
create table ccm.medical_&yr. as
select a.*, b.trnsfd_flag, b.stay_dis_dt, b.discharge, coalesce(b.op_stay_id, b.stay_id, c.case_id, a.syskey) as case_id
from ccm.medical_&yr. a
left join fac5 b
on a.syskey=b.syskey
left join case_id3 c
on a.syskey=c.phys_syskey
order by bene_id, clm_id;
quit;

proc datasets lib=work kill nolist; quit;
%mend;

%add_case_id(2018);
%add_case_id(2019);
%add_case_id(2020);
%add_case_id(2021);

%macro add_key(table);
%if &table.=serviceline %then %do;
proc sql;
create table &table._&yr. as
select a.*, b.syskey, b.from_dt, b.thru_dt, b.clm_type, b.setting, b.case_id, b.dnl_flag, 
	b.tin as line_tin, b.prf_npi as line_prf_npi, b.prf_spec as line_prf_spec
from ccm.&table._&yr.(drop=clm_type) a, ccm.medical_&yr. b
where a.bene_id=b.bene_id and a.clm_id=b.clm_id
order by syskey;
quit;

data ccm.&table._&yr.;
set &table._&yr.;
if dnl_flag=. then dnl_flag=0;
run;
%end;
%else %do;
proc sql;
create table ccm.&table._&yr. as
select a.*, b.syskey, b.from_dt, b.thru_dt, b.clm_type, b.setting, b.case_id, b.dnl_flag
from ccm.&table._&yr. a, ccm.medical_&yr. b
where a.bene_id=b.bene_id and a.clm_id=b.clm_id
order by syskey;
quit;
%end;
%mend;

%macro add_index(table);
proc datasets lib=ccm nolist;
	modify &table._&yr.;
	index create bene_id;
	index create clm_id;
	run;
quit;
%mend;

%macro add_key_index(yr);
%add_key(serviceline);
%add_key(diagnosis);
%add_key(procedure);

%add_index(medical);
%add_index(serviceline);
%add_index(diagnosis);
%add_index(procedure);
%mend;
%add_key_index(2018);
%add_key_index(2019);
%add_key_index(2020);
%add_key_index(2021);



*** update enrollment file ***;

data elig.chc_enrollment;
set chc.chc_elig;

enroll_type="COMM";
bene_dob_dt=mdy(1,1,pt_yob);
bene_dod_dt=.;
bene_race="";
format bene_dob_dt bene_dod_dt mmddyy10.;

if pt_gender='M' then bene_gender='1';
else if pt_gender='F' then bene_gender='2';
else bene_gender='0';

rename 
enroll_start=start_dt
enroll_end=end_dt
;
drop member_id pt_gender;
run;

data elig.chc_mdcr_status;
set elig.chc_enrollment;
mdcr_status_code="10";
keep bene_id mdcr_status_code start_dt end_dt;
run;

data elig.chc_dual;
    format bene_id best12. ;
	format enroll_type $1.;
	format start_dt mmddyy10. ;
	format end_dt mmddyy10. ;
run;

%macro add_index_enroll(table);
proc datasets lib=elig nolist;
	modify &table.;
	index create bene_id;
	run;
quit;
%mend;
%add_index_enroll(chc_enrollment);
%add_index_enroll(chc_mdcr_status);
%add_index_enroll(chc_dual);

*** create a blank table for rx ***;
%macro rx(yr);
    data ccm.pharmacy_&yr.    ;
        format bene_id best12. ;
        format clm_id best12. ;
        format clm_type_cd $1. ;
        format clm_type $4. ;
        format fill_dt mmddyy10. ;
        format month best12. ;
        format paid_dt mmddyy10. ;
        format ndc $11. ;
        format svc_type $1. ;
        format qty_dspnsd best12. ;
        format strength $1. ;
        format days_suply best12. ;
        format brand_name $1. ;
        format generic_name $1. ;
        format bill_npi $1. ;
        format ncpdp_id $1. ;
        format prs_npi $1. ;
        format charge_amt best32. ;
        format allowed_amt best32. ;
        format paid_amt best32. ;
        format ncvrd_plan_pd_amt best32. ;
        format cvrd_d_plan_pd_amt best32. ;
        format bene_rspn_amt best32. ;
        format othr_troop_amt best32. ;
        format ptnt_pay_amt best32. ;
        format lics_amt best32. ;
        format plro_amt best32. ;
        format gdc_blw_oopt_amt best32. ;
        format gdc_abv_oopt_amt best32. ;
        format syskey best32. ;
        format case_id best32. ;
     run;
%mend;

%rx(2018);
%rx(2019);
%rx(2020);
%rx(2021);

