/*This script will create the DIAG data set, which is input 2 of 4 into the HHS-HCC
	program. Components are:

	IDVAR
	DIAG
	DIAGNOSIS_SERVICE_DATE

	For OP and Professional diags, needs to apply HCPCS filter.

	Created by: Michelle Vergara
	Last Updated: June 1, 2023
*/

LIBNAME ccmelig "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\elig";
LIBNAME ccmmed "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\chc";

%macro diagnosis(year);

/*Define file paths*/
LIBNAME ref "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS_HCC_&year.";
LIBNAME data  "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS-HCC Inputs\&year.";


*Import the HCPCS list to filter professional and outpatient claims;

%let file_pre = D:\SASData\dua_052882\Sndbx\Michelle_V\HHS_HCC_&year.;
%let filename = &file_pre.\cpt_crosswalk.csv;

proc import datafile = "&filename."
		out=ref.cpt_crosswalk
		dbms=csv
		replace;
		guessingrows=max;
run;

*Import the diagnosis list for the final filter;

%let file_pre = D:\SASData\dua_052882\Sndbx\Michelle_V\HHS_HCC_&year.;
%let filename = &file_pre.\diag_filter.csv;

proc import datafile = "&filename."
		out=ref.diag_filter
		dbms=csv
		replace;
		guessingrows=max;
run;

*inpatient diagnoses;
	*filter for IP claims that have bill types 111 and 117. Grab relevant diagnoses from the diag file;

proc sql;
create table inp_diags as
select 
	diag.bene_id,
	diag.clm_id,
	diag.dx_cd,
	input(put(diag.thru_dt, yymmddn8.), 8.) as diagnosis_service_date
from (select * from ccmmed.diagnosis_&year. where clm_type = 'IP') diag
inner join (select bene_id from data.person) bene
	on bene.bene_id = diag.bene_id
order by 1,2
;
quit;

proc sql;
create table inp_claims_filter as
select clm.bene_id, clm.clm_id 
from (select *, cats(bill_type, freq_cd) as type_of_bill from ccmmed.medical_&year.
	where clm_type = 'IP' 
	and cats(bill_type, freq_cd) in ('111', '117')) clm
inner join (select bene_id from data.person) bene
	on bene.bene_id = clm.bene_id
order by bene_id, clm_id
;
quit;

data inp_base(drop=clm_id);
merge inp_diags (in=x) inp_claims_filter (in=y);
	by bene_id clm_id;
	if x and y;
run;


*prof diagnoses;
	*filter for prof claims that have eligible CPTs. Grab relevant diagnoses from the diag file;

proc sql;
create table prof_claims_filter as
select clm.bene_id, clm.clm_id 
from (select * from ccmmed.serviceline_&year. where clm_type = 'PHYS') clm
inner join (select code from ref.cpt_crosswalk where include_CY&year. = 'yes') ref
	on clm.cpt = ref.code
inner join (select bene_id from data.person) bene
	on bene.bene_id = clm.bene_id
order by bene_id, clm_id
;
quit;

proc sql;
create table prof_diags as
select 
	diag.bene_id,
	diag.clm_id,
	diag.dx_cd,
	input(put(diag.thru_dt, yymmddn8.), 8.) as diagnosis_service_date
from (select * from ccmmed.diagnosis_&year. where clm_type = 'PHYS') diag
inner join (select bene_id from data.person) bene
	on bene.bene_id = diag.bene_id
order by 1,2
;
quit;

data prof_base(drop=clm_id);
merge prof_diags (in=x) prof_claims_filter (in=y);
	by bene_id clm_id;
	if x and y;
run;


*outpatient diagnoses;
/*	filter for OP claims that have bill types:*/
/*		i. 131 (hospital outpatient admit through discharge); or*/
/*		ii. 137 (hospital outpatient replacement of prior claim); or*/
/*		iii. 711 (rural health clinic admit through discharge); or*/
/*		iv. 717 (rural health clinic replacement of prior claim); or*/
/*		v. 731 (clinic – freestanding admit through discharge); or*/
/*		vi. 737 (clinic – freestanding replacement of prior claim); or*/
/*		vii. 761 (community mental health center admit through discharge); or*/
/*		viii. 767 (community mental health center replacement of prior claim); or*/
/*		ix. 771 (federally qualified health center admit through discharge); or*/
/*		x. 777 (federally qualified health center replacement of prior claim).*/
/*		xi. 851 (critical access hospital admit through discharge); or*/
/*		xii. 857 (critical access hospital replacement of prior claim); or*/
/*		xiii. 871 (freestanding non-residential opioid treatment programs [OTPs]); or*/
/*		xiv. 877 (OTPs replacement of prior claim).*/
/**/
/*		filter for prof claims that have eligible CPTs*/
/**/
/*		Grab relevant diagnoses from the diag file*/

proc sql;
create table op_diags as
select 
	diag.bene_id,
	diag.clm_id,
	diag.dx_cd,
	input(put(diag.thru_dt, yymmddn8.), 8.) as diagnosis_service_date
from (select * from ccmmed.diagnosis_&year. where clm_type = 'OP') diag
inner join (select bene_id from data.person) bene
	on bene.bene_id = diag.bene_id
order by 1,2
;
quit;


proc sql;
create table op_hcpcs_filter as
select clm.bene_id, clm.clm_id 
from (select * from ccmmed.serviceline_&year.
	where clm_type = 'OP') clm
inner join (select code from ref.cpt_crosswalk where include_CY&year. = 'yes') ref
	on clm.cpt = ref.code
inner join (select bene_id from data.person) bene
	on bene.bene_id = clm.bene_id
order by bene_id, clm_id
;
quit;

proc sql;
create table op_claims_filter as
select clm.bene_id, clm.clm_id 
from (select *, cats(bill_type, freq_cd) as type_of_bill from ccmmed.medical_&year.
	where clm_type = 'OP' 
	and cats(bill_type, freq_cd) in 
		('131', '137','711','717','731','737','761','767','771','777','851','857','871','877')) clm
inner join op_hcpcs_filter cpt
	on cpt.bene_id = clm.bene_id and cpt.clm_id = clm.clm_id
order by bene_id, clm_id
;
quit;

data op_base(drop=clm_id);
merge op_diags (in=x) op_claims_filter (in=y);
	by bene_id clm_id;
	if x and y;
run;


*stack and remove duplicates;

data diagnosis_temp;
set op_base prof_base inp_base;
run;

proc sort data = diagnosis_temp nodupkey out=diagnosis_temp2(rename=(dx_cd=diag));
by bene_id dx_cd diagnosis_service_date;
run;

*apply the final filter for only eligible diagnosis codes;

proc sql;
create table data.diagnosis as
select 
	a.*
from diagnosis_temp2 a
inner join ref.diag_filter b
	on a.diag = b.diag
;
quit;

%mend;


