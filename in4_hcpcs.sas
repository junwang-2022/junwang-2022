/*This script will create the HCPCS data set, which is input 4 of 4 into the HHS-HCC
	program. Components are:

	IDVAR
	HCPCS

	Only for adult enrollees. The Healthcare Common Procedure Coding System (HCPCS) input
SAS® data set must include HCPCS codes used for risk adjustment RXCs, listed in Table 10b
RXC to HCPCS Crosswalk. Inpatient, outpatient, and professional medical claims are acceptable
sources for HCPCS codes. Inpatient and outpatient claims should be restricted to the same
facility bill type codes used for the diagnosis data set (see Section III. 2a and 3a)


	Created by: Michelle Vergara
	Last Updated: June 1, 2023
*/

LIBNAME ccmelig "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\elig";
LIBNAME ccmmed "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\chc";

%macro hcpcs(year);

/*Define file paths*/
LIBNAME ref "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS_HCC_&year.";
LIBNAME  data  "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS-HCC Inputs\&year.";


*Import the HCPCS list;

%let file_pre = D:\SASData\dua_052882\Sndbx\Michelle_V\HHS_HCC_&year.;
%let filename = &file_pre.\cpt_rxc_crosswalk.csv;

proc import datafile = "&filename."
		out=ref.cpt_rxc_crosswalk
		dbms=csv
		replace;
		guessingrows=max;
run;

*Filter the serviceline dataset to only IP, OP, and PROF claims with HCPCS-RXC codes;

proc sql;
create table hcpcs_claim_filter_temp as
select 
	bene_id,
	cpt,
	clm_id
from ccmmed.serviceline_&year.
where cpt in (select HCPCS from ref.cpt_rxc_crosswalk)
	and clm_type in ('IP', 'OP','PROF')
order by 1,2
;
quit;

*remove duplicates;
proc sort data=hcpcs_claim_filter_temp nodupkey out=hcpcs_claim_filter;
by bene_id cpt clm_id;
run;

*filter for claims in the IP and OP list;
proc sql;
create table hcpcs_base_temp as
select 
	a.*
from hcpcs_claim_filter a
inner join 
	(select bene_id, clm_id, cats(bill_type, freq_cd) as bill_type from ccmmed.medical_&year. 
	where cats(bill_type, freq_cd) in ('111','117','131','137','711','717','731','737','761','767','771','777','851','857','871','877')
			or clm_type = 'PHYS') clm
on a.bene_id = clm.bene_id and a.clm_id = clm.clm_id
;
quit;


*final filter for adult enrollees;

proc sql;
create table hcpcs as
select a.bene_id,
	a.cpt
from hcpcs_base_temp a
inner join (select bene_id from data.person where age_last >= 18) b
	on a.bene_id = b.bene_id
;
quit;


*final dedupe;

proc sort data=hcpcs nodupkey out=data.hcpcs(rename=(cpt=hcpcs));
by bene_id cpt;
run;

%mend;

