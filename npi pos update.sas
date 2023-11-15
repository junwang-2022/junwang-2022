libname a "C:\Users\jun.wang\PPI3.0\taxonomy\npi pos";
libname ccmffs "D:\sas\ccm";

********************************;
*** identify physician's pos ***;
********************************;
%macro prvdr_pos(prvdr);

%if &prvdr.=prf_npi %then %do;
	proc sql;
	create table npi_spec as
	select prf_npi, prf_spec as npi_spec, count(*) as cnt
	from medical_&yr.
	where clm_type='PHYS'
	group by prf_npi, prf_spec
	order by prf_npi, cnt desc, npi_spec desc;
	quit;
proc sort data=npi_spec nodupkey; by &prvdr._npi; run;
%end;
%if &prvdr.=bill_npi %then %do;
	data npi_spec;
	bill_npi=""; npi_spec="";
	run;
%end;
proc sql;
create table npi_zip as
select &prvdr., prvdr_zip_cd, count(*) as cnt
from medical_&yr.
where clm_type='PHYS'
group by &prvdr., prvdr_zip_cd
order by &prvdr., cnt desc;
quit;

proc sort data=npi_zip nodupkey; by &prvdr.; run;

proc sql;
create table npi_setting as
select &prvdr., setting, count(*) as clm_cnt, sum(allowed_amt) as allowed_amt
from medical_&yr.
where clm_type='PHYS'
group by &prvdr., setting;
quit;

proc sql;
create table npi_setting2 as
select &prvdr., setting, clm_cnt, sum(clm_cnt) as tot_clm_cnt, clm_cnt/sum(clm_cnt) as clm_pct,
	allowed_amt, sum(allowed_amt) as tot_amt, allowed_amt/sum(allowed_amt) as amt_pct
from npi_setting
group by &prvdr.;
quit;

proc sql;
create table npi_setting3 as
select a.*, b.npi_spec, c.prvdr_zip_cd
from npi_setting2 a
left join npi_spec b on a.&prvdr.=b.&prvdr.
left join npi_zip c on a.&prvdr.=c.&prvdr.;
quit;


data npi_setting4;
set npi_setting3;
where &prvdr. ~= '';

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
tables &prvdr. / missing out = check01 (drop = PERCENT);
run;

proc sql;
create table &prvdr._pos_&yr. as
select a.&prvdr.,
		a.setting,
		a.clm_cnt,
		case when b.count = 1 then . else tot_clm_cnt end as tot_clm_cnt,
		case when b.count = 1 then . else clm_pct end as clm_pct,
		a.allowed_amt,
		a.tot_amt,
		a.amt_pct,
		a.npi_spec,
		a.PRVDR_ZIP_CD
from npi_setting4 a
left join check01 b
on a.&prvdr. = b.&prvdr.;
quit;

%mend;

%macro pos_year(yr);
data medical_&yr.;
set ccmffs.medical_&yr._01 - ccmffs.medical_&yr._12;
run;
%prvdr_pos(prf_npi);
%prvdr_pos(bill_npi);
%mend;

%pos_year(2021);
%pos_year(2022);

****** after vrdc ******;
libname meta2 "C:\Users\jun.wang\Episode Builder\meta2";
libname vrdc "C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\bill_npi_prf_npi_pos-20230613T141101Z-001\bill_npi_prf_npi_pos";

%macro prvdr_pos(prvdr, yr);
proc transpose data=vrdc._&prvdr._pos_&yr. out=pct1;
where clm_pct>=0.05;
by &prvdr. npi_spec tot_clm_cnt prvdr_zip_cd;
var clm_pct;
id setting;
run;
proc transpose data=vrdc._&prvdr._pos_&yr. out=pct2;
where amt_pct>=0.05;
by &prvdr. npi_spec tot_amt prvdr_zip_cd;
var amt_pct;
id setting;
run;

data npi_pos;
set pct1(rename=(tot_clm_cnt=total)) pct2(rename=(tot_amt=total));
year=&yr.;
length provider_type $8.;
provider_type="&prvdr.";
if _name_="clm_pct" then type="Total claim count   ";
if _name_="amt_pct" then type="Total allowed amount";
drop _name_;
run;

proc sql;
create table a.&prvdr._pos_&yr. as 
select distinct a.&prvdr., strip(strip(b.first_name)||" "||strip(b.last_name)||strip(b.providerorganization)) as npi_name, 
	coalescec(a.npi_spec, b.cms_specialty_code) as npi_spec_cd, c.spec_desc, d.cbsa_desc as cbsa, d.county_desc as county, d.state, a.type, a.total, a.*
from npi_pos a 
left join meta2.npi_spec_x_new b on a.&prvdr.=b.npi
left join meta2.cms_spec_cd c on coalescec(a.npi_spec, b.cms_specialty_code)=c.spec_cd
left join a.zip_cbsa d on substr(a.prvdr_zip_cd,1,5)=d.zip_cd 
order by &prvdr., type desc;
quit;

proc export data=a.&prvdr._pos_&yr. outfile="C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\&prvdr._pos_&yr..csv" replace; run;

%mend;

%prvdr_pos(prf_npi, 2021);
%prvdr_pos(prf_npi, 2022);
%prvdr_pos(bill_npi, 2021);
%prvdr_pos(bill_npi, 2022);



