libname hv "D:\SASData\dua_052882\Sndbx\Chris F\inovalon\raw_data";

*** source_id_crosswalk ***;
proc sql; select count(*), count(distinct hvid) from hv.source_id_crosswalk; quit;
proc sql;
select cnt, count(*) as hvid_cnt
from (select hvid, count(*) as cnt from hv.source_id_crosswalk group by hvid)
group by cnt;
quit;
proc print data=hv.source_id_crosswalk; where hvid="c559e080f4ff5316b309d39bb7fb5b93"; run;

proc sql;
create table a as 
select sourceid, count(*) as cnt
from (select distinct sourceid, hvid from hv.source_id_crosswalk)
group by sourceid
order by cnt desc;
quit;
proc sql;
select distinct sourceid, cnt, cnt/sum(cnt) as pct
from a
order by cnt desc;
quit;

proc sql;
create table effc_date as
select year(start_dt) as year, month(start_dt) as month, count(*) as cnt
from
	(select hvid, effectivedate as start_dt format=mmddyy10., max(terminationdate) as end_dt format=mmddyy10.
	from hv.source_id_crosswalk
	group by hvid, effectivedate)
group by year(start_dt), month(start_dt)
order by year, month;
quit;

*** enrollment_x ***;

data enrollment;
set 
hv.enrollment_1(in=a)
hv.enrollment_2(in=b)
hv.enrollment_3(in=c)
hv.enrollment_4(in=d)
hv.enrollment_5(in=e)
;
if a then flag="a";
if b then flag="b";
if c then flag="c";
if d then flag="d";
if e then flag="e";

run;
proc sql; select count(distinct hvid) from enrollment; quit;
proc sql; select distinct year(calendar_date) as year, month(calendar_date) as month, count(distinct hvid) 
from enrollment 
group by year(calendar_date), month(calendar_date)
order by year, month; quit;

proc sql;
select cnt, count(*) as hvid_cnt
from (select hvid, count(*) as cnt from enrollment group by hvid)
group by cnt;
quit;

data a;
set enrollment;
where hvid="c559e080f4ff5316b309d39bb7fb5b93";
run;
proc sort data=a; by calendar_date; run;


*** pharmacy ***;

proc sql;
select count(distinct hvid)
from enrollment
where hvid not in (select distinct hvid from hv.pharmacy);
quit;

proc sql; select distinct year(date_service) as year, month(date_service) as month, count(distinct hvid) 
from hv.pharmacy 
group by year(date_service), month(date_service)
order by year, month; quit;

proc sql;
select distinct 
sum(date_service=.)/count(*) as date_service_missing,
sum(ndc_code=.)/count(*) as ndc_missing,
sum(pharmacy_npi=.)/count(*) as pharmacy_npi_missing,
sum(dispensed_quantity=.)/count(*) as dispensed_quantity_missing,
sum(days_supply=.)/count(*) as days_supply_missing,
sum(prov_prescribing_npi=.)/count(*) as prescribing_npi_missing,
sum(submitted_gross_due=.)/count(*) as submitted_gross_missing,
sum(paid_gross_due=.)/count(*) as paid_gross_missing,
sum(copay_coinsurance=.)/count(*) as copay_coinsurance_missing
from hv.pharmacy;
quit;

*** medical ***;

proc sql;
select count(distinct hvid)
from enrollment
where hvid not in (select distinct hvid from hv.medical);
quit;

proc sql; select distinct year(date_service) as year, month(date_service) as month, count(distinct hvid) 
from hv.medical 
group by year(date_service), month(date_service)
order by year, month; quit;

proc sql;
select distinct claim_type,
sum(date_service=.)/count(*) as date_service_missing,
sum(claim_id="")/count(*) as claim_id_missing,
sum(service_line_id=.)/count(*) as service_line_id_missing,
sum(inst_type_of_bill_std_id="")/count(*) as bill_type_missing,
sum(inst_discharge_status_std_id=.)/count(*) as discharge_missing,
sum(place_of_service_std_id=.)/count(*) as pos_missing,
sum(line_charge=.)/count(*) as line_charge_missing,
sum(line_allowed=.)/count(*) as line_allowed_missing,
sum(total_charge=.)/count(*) as total_charge_missing,
sum(total_allowed="")/count(*) as total_allowed_missing,
sum(prov_rendering_npi=.)/count(*) as rendering_npi_missing,
sum(prov_billing_npi=.)/count(*) as billing_npi_missing,
sum(prov_referring_npi="")/count(*) as referring_npi_missing
from hv.medical
group by claim_type;
quit;
proc sql;
select distinct 
sum(drg^=.)/count(*) as msdrg_missing
from (select service_line_id, max(inst_drg_std_id) as drg from hv.medical
		where substr(inst_type_of_bill_std_id,1,2)="11" group by service_line_id);
quit;

proc sort data=hv.medical out=medical;
by hvid date_service service_line_id;
run;

proc freq data=medical;
where diagnosis_code^="";
tables diagnosis_code_qual diagnosis_priority/list missing;
run;

proc sort data=medical out=proc;
where procedure_code^="" and procedure_code_qual="HC";
by hvid service_line_id date_service line_allowed procedure_code;
run;
proc transpose data=proc out=proc2;
by hvid service_line_id date_service line_allowed;
var procedure_code;
run;

proc print data=hv.medical;
where service_line_id in (885325641568766976);
run;

proc print data=hv.medical;
where service_line_id=885325641500174592;
run;
data a;
set hv.medical;
where hvid="005347ad1db48d57d9c4db934086accd";
run;
proc sort; by date_service service_line_id; run;

data p01850;
set medical;
where hvid="0185008033a8a5b2142b35e5252a2992";
run;
proc sort; by date_service service_line_id; run;


proc sql;
create table line_header as
select distinct hvid, service_line_id, claim_type, inst_type_of_bill_std_id, place_of_service_std_id, inst_discharge_status_std_id, date_service, date_service_end, line_charge, line_allowed, total_charge, total_allowed, 
	prov_rendering_npi, prov_billing_npi, prov_rendering_vendor_id, prov_billing_vendor_id, logical_delete_reason
from hv.medical
order by hvid, date_service, date_service_end, prov_billing_npi, prov_billing_vendor_id, prov_rendering_npi, prov_rendering_vendor_id ;
quit;

proc sql;
create table medical_header as
select distinct hvid, claim_type, inst_type_of_bill_std_id, place_of_service_std_id, inst_discharge_status_std_id, date_service, date_service_end, 
	prov_rendering_npi, prov_billing_npi, prov_rendering_vendor_id, prov_billing_vendor_id, logical_delete_reason,
	sum(line_charge) as charge_amt, sum(line_allowed) as allowed_amt
from line_header
group by hvid, claim_type, inst_type_of_bill_std_id, place_of_service_std_id, inst_discharge_status_std_id, date_service, date_service_end,
	prov_rendering_npi, prov_billing_npi, prov_rendering_vendor_id, prov_billing_vendor_id, logical_delete_reason
order by hvid, date_service, date_service_end, prov_billing_npi, prov_billing_vendor_id, prov_rendering_npi, prov_rendering_vendor_id ;
quit;

proc sort data=hv.medical out=hcpcs;
where length(procedure_code)=5 and procedure_code^="";
by hvid claim_type service_line_id date_service line_allowed procedure_code;
run;
proc transpose data=hcpcs out=hcpcs2;
by hvid claim_type service_line_id date_service line_allowed;
var procedure_code;
run;
proc freq data=hcpcs2;
tables claim_type*(col2 col3)/list missing;
run;

proc sort data=hv.medical out=proc;
where length(procedure_code)=7 and procedure_code^="";
by hvid claim_type service_line_id date_service line_allowed line_charge total_charge total_allowed procedure_code;
run;
proc transpose data=proc out=proc2;
by hvid claim_type service_line_id date_service line_allowed line_charge total_charge total_allowed;
var procedure_code;
run;
