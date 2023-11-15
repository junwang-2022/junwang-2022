%LET _CLIENTTASKLABEL='Summary of Provider Specialty & Place of Service';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='';
%LET _CLIENTPROJECTPATHHOST='';
%LET _CLIENTPROJECTNAME='';
%LET _SASPROGRAMFILE='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Summary of Provider Specialty & Place of Service.sas';
%LET _SASPROGRAMFILEHOST='apcw10sg07.ap.r53.ccwdata.org';

GOPTIONS ACCESSIBLE;

%include "&myfiles_root/dua_052882/prod/utils/CCM_Macros.sas";
%gbl_ccm_libnames(PROD);


data medical_2021;
set ccmffs.medical_2021_01 - ccmffs.medical_2021_12;
run;

proc sql;
create table npi_spec as
select prf_npi, prf_spec, setting, count(*) as cnt
from medical_2021
where clm_type='PHYS'
group by prf_npi, prf_spec, setting;
quit;
proc sql;
create table npi_spec as
select prf_npi, prf_spec, setting, sum(cnt) as tot_clm_cnt, cnt/sum(cnt) as pct
from npi_spec
group by prf_npi, prf_spec
order by prf_npi, prf_spec, pct desc;
quit;


/*Please download the table after replacing <11 count with blank in tot_clm_cnt.*/

proc sql;
create table npi_spec01 as
select prf_npi, prf_spec, setting, case when tot_clm_cnt < 11 then . else tot_clm_cnt end as tot_clm_count, pct
from npi_spec
where prf_npi ~= ''
;
quit;



/********************************** SECOND ROUND OF ANALYSIS 03/03 *************************************************/

data medical_2021;
set ccmffs.medical_2021_01 - ccmffs.medical_2021_12;
run;

proc sql;
create table npi_spec as
select prf_npi, prf_spec, count(*) as cnt
from medical_2021
where clm_type='PHYS'
group by prf_npi, prf_spec
order by prf_npi, cnt desc, prf_spec desc;
quit;

proc sort data=npi_spec nodupkey; by prf_npi; run;

proc sql;
create table npi_zip as
select prf_npi, prvdr_zip_cd, count(*) as cnt
from medical_2021
where clm_type='PHYS'
group by prf_npi, prvdr_zip_cd
order by prf_npi, cnt desc;
quit;

proc sort data=npi_zip nodupkey; by prf_npi; run;

proc sql;
create table npi_setting as
select prf_npi, setting, count(*) as clm_cnt, sum(allowed_amt) as allowed_amt
from medical_2021
where clm_type='PHYS'
group by prf_npi, setting;
quit;

proc sql;
create table npi_setting2 as
select prf_npi, setting, clm_cnt, sum(clm_cnt) as tot_clm_cnt, clm_cnt/sum(clm_cnt) as clm_pct,
	allowed_amt, sum(allowed_amt) as tot_amt, allowed_amt/sum(allowed_amt) as amt_pct
from npi_setting
group by prf_npi;
quit;

proc sql;
create table npi_setting3 as
select a.*, b.prf_spec, c.prvdr_zip_cd
from npi_setting2 a
left join npi_spec b on a.prf_npi=b.prf_npi
left join npi_zip c on a.prf_npi=c.prf_npi;
quit;


data npi_setting4;
set npi_setting3;
where PRF_NPI ~= '';

if clm_cnt < 11 then do;
	clm_cnt = .;
	clm_pct = .;
end;

if tot_clm_cnt < 11 then tot_clm_cnt = .;
else tot_clm_cnt = tot_clm_cnt;

run;

/* merge list back on know where there is one blank */

proc freq data = npi_setting4 noprint;
where clm_cnt = .;
tables PRF_NPI / missing out = check01 (drop = PERCENT);
run;


proc sql;
create table npi_setting5 as
select a.prf_npi,
		a.setting,
		a.clm_cnt,
		case when b.count = 1 then . else tot_clm_cnt end as tot_clm_cnt,
		case when b.count = 1 then . else clm_pct end as clm_pct,
		a.allowed_amt,
		a.tot_amt,
		a.amt_pct,
		a.prf_spec,
		a.PRVDR_ZIP_CD
from npi_setting4 a
left join check01 b
on a.PRF_NPI = b.PRF_NPI
;
quit;





GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

