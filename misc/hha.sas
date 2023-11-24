libname lds "D:\SASData\SAS_Shared_Data\LDS_CCM";
libname rif17 "D:\SASData\SAS_Shared_Data\LDS_rif\2017";
libname rif16 "D:\SASData\SAS_Shared_Data\LDS_rif\2016";

data rif_hha;
set rif16.hha_claims_01-rif16.hha_claims_12 rif17.hha_claims_01-rif17.hha_claims_12;
clm_id2=strip(put(clm_id,15.))||put(year(clm_thru_dt),4.)||"HHA";
run;
data ccm_hha;
set lds.medical_2016 lds.medical_2017;
where clm_type="HHA";
run;

proc sql;
create table hha as
select a.*, b.clm_hha_rfrl_cd as hha_rfr_type
from ccm_hha a
left join rif_hha b on a.bene_id=b.bene_id and a.clm_id=b.clm_id2
order by bene_id, adm_dt, case_id, from_dt, thru_dt;
quit;

data non_hha;
set lds.medical_2016 lds.medical_2017;
where clm_type^="HHA";
run;

proc sql;
create table hha_adm as
select distinct bene_id, case_id as hha_case_id, adm_dt format=mmddyy10. as hha_adm_dt, stay_dis_dt format=mmddyy10. as hha_dis_dt, thru_dt, hha_rfr_type
from hha
where adm_dt>=mdy(3,1,2016)
order by bene_id, hha_adm_dt, hha_dis_dt, hha_case_id, thru_dt;
quit;
proc sort data=hha_adm nodupkey; by bene_id hha_adm_dt hha_dis_dt hha_case_id; run;

*** HHA started after IP/SNF/HSP discharge ***;
proc sql;
create table ins_dis_hha as
select distinct a.*, b.clm_type, b.setting, b.from_dt, b.thru_dt, b.adm_dt, b.dis_dt, b.stay_dis_dt, b.dis_status, b.discharge, 
	a.hha_adm_dt-coalesce(b.stay_dis_dt, b.thru_dt) as dis_gap
from hha_adm a, non_hha b
where a.bene_id=b.bene_id and .<b.stay_dis_dt<=a.hha_adm_dt and b.clm_type in ("IP" "SNF" "HSP") and b.discharge="HHA" and a.hha_adm_dt-coalesce(b.stay_dis_dt, b.thru_dt)<=30
order by bene_id, hha_adm_dt, dis_gap, from_dt, thru_dt;
quit;
proc sort data=ins_dis_hha out=ins_dis_hha2 nodupkey; by bene_id hha_case_id; run;

proc sql;
create table hha_adm2 as
select a.*, b.clm_type as hha_dis_type, b.stay_dis_dt as ins_dis_dt
from hha_adm a
left join ins_dis_hha2 b on a.bene_id=b.bene_id and a.hha_case_id=b.hha_case_id
order by bene_id, hha_adm_dt, hha_dis_dt;
quit;

proc freq data=hha_adm2;
tables hha_dis_type hha_dis_type*hha_rfr_type/list missing;
run;

*** HHA started after OP ***;
proc sql;
create table comm_ref_hha as
select distinct a.*, b.clm_type, b.setting, b.from_dt, b.thru_dt, b.adm_dt, b.dis_dt, b.stay_dis_dt, b.dis_status, b.discharge, 
	a.hha_adm_dt-b.thru_dt as ref_gap, b.prf_spec, b.prf_npi, b.at_npi
from hha_adm2 a, non_hha b
where a.hha_dis_type="" and a.bene_id=b.bene_id and .<b.thru_dt<=a.hha_adm_dt and b.clm_type not in ("IP" "SNF" "HSP") and a.hha_adm_dt-b.thru_dt<=30
order by bene_id, hha_adm_dt, ref_gap, from_dt, thru_dt;
quit;
proc sort data=ins_dis_hha out=ins_dis_hha2 nodupkey; by bene_id hha_case_id; run;


