
%let year=2017;

%macro prvdr_zip(prvdr, type);
proc sql;
create table zip_x_&prvdr._&year. as 
select 	 distinct &year. as year
		,"&type." as provider_type length=10
		,"&prvdr." as npi_type length=10
		,&prvdr. as provider_id length=11
		,substr(prvdr_zip_cd,1,5) as provider_zip
		,sum(allowed_amt) as provider_total
from ccm.medical_&year.
where &prvdr.^=""
group by &prvdr., substr(prvdr_zip_cd,1,5);
quit;

proc sql;
create table cbsa_x_&prvdr._&year. as 
select *, sum(provider_total) as cbsa_total
from (select a.*, b.cbsa_desc as cbsa, b.state, b.county_desc as county
		from zip_x_&prvdr._&year. a
		left join meta2.zip_cbsa b on a.provider_zip=substr(b.zip_cd,1,5))
group by provider_type, npi_type, provider_id, cbsa
order by provider_type, provider_id, cbsa_total desc, provider_total desc;
quit;

proc sort data=cbsa_x_&prvdr._&year. nodupkey; by provider_type npi_type provider_id; run;

%mend;

%macro provider_x_cbsa;			

%prvdr_zip(prf_npi, phys_npi);
%prvdr_zip(at_npi, phys_npi);
%prvdr_zip(op_npi, phys_npi);
%prvdr_zip(bill_npi, bill_npi);
%prvdr_zip(tin, tin);
%prvdr_zip(ccn, ccn);

data provider_x_cbsa_&year.;
set cbsa_x_:;
run;

proc sort data=provider_x_cbsa_&year.;
by provider_id provider_type descending npi_type;
run;
proc sort data=provider_x_cbsa_&year. out=meta2.provider_x_cbsa_&year.(drop=npi_type) nodupkey;
by provider_id provider_type;
run;
proc datasets lib=work noprint; delete cbsa_x_:; run;
%mend;

%macro npi_x_spec;
proc sql;
	create table prf_npi_x_spec_&year. as
	select distinct &year. as year, prf_npi as npi, prf_spec as spec_cd, sum(allowed_amt) as npi_spec_total
	from ccm.medical_&year.
	where prf_spec^=""
	group by npi, spec_cd
	order by npi, npi_spec_total desc;
	quit;
proc sort data=prf_npi_x_spec_&year. nodupkey; by npi; run;

proc sql;
	create table atop_npi_x_spec_&year. as
	select distinct &year. as year, a.npi, b.cms_specialty_code as spec_cd
	from
		(select distinct &year. as year, at_npi as npi from ccm.medical_&year. where at_npi^=""
		union corr
		select distinct &year. as year, op_npi as npi from ccm.medical_&year. where op_npi^="") a
	left join meta2.npi_spec_x_new b
	on a.npi=b.npi
	order by npi;
	quit;

data npi_x_spec_&year.;
set prf_npi_x_spec_&year. atop_npi_x_spec_&year.;
run;
proc sort data=npi_x_spec_&year. out=meta2.npi_x_spec_&year. nodupkey; by npi; run;

%mend;

%macro npi_x_tin;
proc sql;
	create table npi_x_tin_&year. as
	select distinct &year. as year, prf_npi, tin, sum(allowed_amt) as prf_tin_total
	from ccm.medical_&year.
	where prf_npi^="" and tin^=""
	group by prf_npi, tin
	order by prf_npi, prf_tin_total desc;
	quit;
proc sort data=npi_x_tin_&year. out=meta2.npi_x_tin_&year. nodupkey; by prf_npi; run;

%mend;

%macro npi_x_bill;
proc sql;
	create table npi_x_bill_&year. as
	select distinct &year. as year, prf_npi, bill_npi, sum(allowed_amt) as prf_bill_total
	from ccm.medical_&year.
	where prf_npi^="" and bill_npi^=""
	group by prf_npi, bill_npi
	order by prf_npi, prf_bill_total desc;
	quit;
proc sort data=npi_x_bill_&year. out=meta2.npi_x_bill_&year. nodupkey; by prf_npi; run;

%mend;


%provider_x_cbsa;
%npi_x_spec;
%npi_x_tin;
%npi_x_bill;

