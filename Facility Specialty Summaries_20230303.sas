%LET _CLIENTTASKLABEL='Facility Specialty Summaries_20230303';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='';
%LET _CLIENTPROJECTPATHHOST='';
%LET _CLIENTPROJECTNAME='';
%LET _SASPROGRAMFILE='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Juns Queries/Facility Specialty Summaries_20230303.sas';
%LET _SASPROGRAMFILEHOST='apcw10sg05.ap.r53.ccwdata.org';

GOPTIONS ACCESSIBLE;

%include "&myfiles_root/dua_052882/prod/utils/CCM_Macros.sas";
%gbl_ccm_libnames(PROD);


data medical_2021;
set ccmffs.medical_2021_01 - ccmffs.medical_2021_12;
run;

*** identify fac specialty ***;
proc sql;
create table fac_type as
select distinct coalescec(ccn,bill_npi) as fac_id, clm_type, setting, count(*) as cnt
from medical_2021
where clm_type not in ('PHYS' 'DME' 'OP') or (clm_type='PHYS' and setting='ASC' and prf_spec='49')
group by coalescec(ccn,bill_npi), clm_type, setting
order by fac_id, cnt desc;
quit;
proc sort data=fac_type nodupkey; by fac_id; run;
proc sql;
create table fac_op as
select distinct ccn as fac_id, clm_type, setting, count(*) as cnt
from medical_2021
where clm_type='OP' and ccn not in (select distinct fac_id from fac_type)
group by ccn, clm_type, setting
order by fac_id, cnt desc;
quit;
proc sort data=fac_op nodupkey; by fac_id; run;
data fac_all;
set fac_type fac_op;
fac_type=setting;
run;
*** rollup prf spec ***;
proc sql;
create table fac_phys as
select a.fac_id, a.fac_type, b.bene_id, b.case_id, c.prf_spec, c.tot_amt
from fac_all a
left join (select distinct coalescec(ccn,bill_npi) as fac_id, bene_id, case_id from medical_2021) b
on a.fac_id=b.fac_id
left join (select bene_id, case_id, prf_spec, sum(allowed_amt) as tot_amt from medical_2021
				where clm_type='PHYS' and prf_spec^='49' group by bene_id, case_id, prf_spec) c
on b.bene_id=c.bene_id and b.case_id=c.case_id
order by fac_id, fac_type, bene_id, case_id;
quit;
proc sql;
create table fac_spec as
select distinct fac_type, fac_id, prf_spec, sum(tot_amt) as tot_amt
from fac_phys
group by fac_id, fac_type, prf_spec
order by fac_id, fac_type, prf_spec;
quit;
*** rollup by disease category ***;
proc sql;
create table fac_dx as
select distinct a.fac_type, a.fac_id, substr(pdx_cd,1,4) as dx4, sum(allowed_amt) as tot_amt
from fac_all a, medical_2021 b
where a.fac_id=coalescec(b.ccn, b.bill_npi)
group by fac_id, substr(pdx_cd,1,4)
order by fac_type, fac_id, dx4;
quit;
*** pg by spec and disease category ***;
proc sql;
create table pg_spec as
select bill_npi, prf_spec, sum(allowed_amt) as allowed_amt
from medical_2021
where clm_type='PHYS' and prf_spec^='49'
group by bill_npi, prf_spec;
quit;
proc sql;
create table pg_dx as
select bill_npi, substr(pdx_cd,1,4) as dx4, sum(allowed_amt) as allowed_amt
from medical_2021
where clm_type='PHYS' and prf_spec^='49'
group by 1, 2;
quit;



/* add where statements to exclude $0 payments */

data fac_spec_export;
set fac_spec;
where fac_id ~= '' and tot_amt ~= 0;
run;

data fac_dx_export;
set fac_dx;
where fac_id ~= '' and tot_amt ~= 0;
run;

data pg_spec_export;
set pg_spec;
where bill_npi ~= '' and allowed_amt ~= 0;
run;

data pg_dx_export;
set pg_dx;
where bill_npi ~= '' and allowed_amt ~= 0;
run;









GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;
