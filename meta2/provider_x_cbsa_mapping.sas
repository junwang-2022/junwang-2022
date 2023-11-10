libname ccm "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\chc" compress=char;
libname ref "D:\SASData\SAS_Shared_Data\shared\ref";
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";
libname meta "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta v10";


proc sql;
create table nppes_cbsa_spec as 
select a.npi, a.cms_specialty_code as spec_cd, b.cbsa_desc as cbsa, b.state, b.county_desc as county
from meta.npi_spec_x_new a
left join meta2.ref_cj_na_geography b on substr(a.prvd_bz_addresszip,1,5)=substr(b.zip_cd,1,5);
quit;
proc sort data=nppes_cbsa_spec nodupkey; by npi; run;

*** map spec ***;

%macro map_spec(year);
data npi_spec;
set meta2.npi_x_spec_&year.(in=a) nppes_cbsa_spec(in=b);
if a then flag=1;
if b then flag=2;
year=&year.;
keep npi spec_cd year flag;
run;
proc sort data=npi_spec; where npi^=""; by npi flag; run;
proc sort data=npi_spec out=ref.npi_x_spec_&year.(drop=flag) nodupkey; by npi; run;
%mend;

%map_spec(2019);
%map_spec(2020);
%map_spec(2021);
%map_spec(2022);

*** map cbsa ***;


%macro prvdr_zip(prvdr, type);
proc sql;
create table zip_x_&prvdr._&year. as 
select 	 distinct &year. as year
		,"&type." as provider_type length=10
		,"&prvdr." as npi_type length=10
		,&prvdr. as provider_id length=11
		,prvdr_zip_cd as provider_zip
		,sum(allowed_amt) as provider_total
from ccm.medical_&year.
where &prvdr.^=""
group by &prvdr., prvdr_zip_cd;
quit;

proc sql;
create table cbsa_x_&prvdr._&year. as 
select *, sum(provider_total) as cbsa_total
from (select a.*, b.cbsa_desc as cbsa, b.state, b.county_desc as county
		from zip_x_&prvdr._&year. a
		left join meta2.ref_cj_na_geography b on a.provider_zip=substr(b.zip_cd,1,5))
group by provider_type, npi_type, provider_id, cbsa
order by provider_type, provider_id, cbsa_total desc, provider_total desc;
quit;

proc sort data=cbsa_x_&prvdr._&year. nodupkey; by provider_type npi_type provider_id; run;

%mend;
	
%macro map_cbsa(year);

/*%prvdr_zip(at_npi, phys_npi);*/
/*%prvdr_zip(bill_npi, bill_npi);*/
/*%prvdr_zip(tin, tin);*/
/*%prvdr_zip(ccn, ccn);*/

data provider_x_cbsa_&year.;
set cbsa_x_at_npi_&year. cbsa_x_bill_npi_&year. cbsa_x_tin_&year. cbsa_x_ccn_&year.;
run;

proc sort data=provider_x_cbsa_&year.;
by provider_id provider_type descending npi_type;
run;
proc sort data=provider_x_cbsa_&year. nodupkey;
by provider_id provider_type;
run;

proc sql;
create table ref.provider_x_cbsa_&year. as
select distinct a.year, a.provider_type, a.provider_id, 
	coalescec(b.cbsa, b1.cbsa, c.cbsa, a.cbsa) as cbsa,
	coalescec(b.state, b1.state, c.state, a.state) as state,
	coalescec(b.county, b1.county, c.county, a.county) as county
from provider_x_cbsa_&year.(where=(provider_id^="")) a
left join meta2.provider_x_cbsa_&year.(where=(provider_id^="")) b on a.provider_type=b.provider_type and a.provider_id=b.provider_id
left join meta2.provider_x_cbsa_&year.(where=(provider_id^="")) b1 on a.provider_id=b1.provider_id
left join nppes_cbsa_spec c on index(a.provider_type,"npi")>0 and a.provider_id=c.npi
order by provider_type, provider_id;
quit;
proc sort data=ref.provider_x_cbsa_&year. nodupkey; by provider_type provider_id; run;
%mend;
%map_cbsa(2019);
%map_cbsa(2020);
%map_cbsa(2021);


%macro npi_x_tin(year);
proc sql;
	create table npi_x_tin_&year. as
	select distinct &year. as year, prf_npi, tin, sum(allowed_amt) as prf_tin_total
	from ccm.medical_&year.
	where prf_npi^="" and tin^=""
	group by prf_npi, tin
	order by prf_npi, prf_tin_total desc;
	quit;
proc sort data=npi_x_tin_&year. out=ref.npi_x_tin_&year. nodupkey; by prf_npi; run;

%mend;
%npi_x_tin(2019);
%npi_x_tin(2020);
%npi_x_tin(2021);

%macro npi_x_bill(year);
proc sql;
	create table npi_x_bill_&year. as
	select distinct &year. as year, prf_npi, bill_npi, sum(allowed_amt) as prf_bill_total
	from ccm.medical_&year.
	where prf_npi^="" and bill_npi^=""
	group by prf_npi, bill_npi
	order by prf_npi, prf_bill_total desc;
	quit;
proc sort data=npi_x_bill_&year. out=ref.npi_x_bill_&year. nodupkey; by prf_npi; run;

%mend;
%npi_x_bill(2019);
%npi_x_bill(2020);
%npi_x_bill(2021);


libname hhs1 "D:\SASData\SAS_Shared_Data\shared\Edge";
libname hhs2 "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS-HCC";

data meta.hhs_cpt_hcc;
set 
hhs1.hhs_hcc_table2(where=(cy2019="yes") rename=(hcpcs_cd=proc_code) in=x)
hhs1.hhs_hcc_table2(where=(cy2019="yes") rename=(hcpcs_cd=proc_code) in=a)
hhs1.hhs_hcc_table2(where=(cy2020="yes") rename=(hcpcs_cd=proc_code) in=b)
hhs2.cpt_crosswalk(where=(include_cy2021="yes") rename=(code=proc_code) in=c)
hhs2.cpt_crosswalk(where=(include_cy2022="yes") rename=(code=proc_code) in=d)
;
if x then year=2018;
if a then year=2019;
if b then year=2020;
if c then year=2021;
if d then year=2022;

keep proc_code year;
run;
