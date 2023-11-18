/*This script will create the PERSON data set, which is input 1 of 4 into the HHS-HCC
	program. Components are:

	IDVAR
	SEX
	DOB
	AGE_LAST
	METAL
	CSR_Indicator
	ENROLDURATION

	Created by: Michelle Vergara
	Last Updated: June 1, 2023
*/

LIBNAME ccmelig "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\elig";
LIBNAME ccmmed "D:\SASData\SAS_Shared_Data\shared\CCM\PROD\chc";

%macro person(year);

/*Define file paths*/

LIBNAME data "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS-HCC Inputs\&year.";

%let last_day = %sysfunc(intnx(year, %sysfunc(mdy(12,31, &year.)),0,end));

proc sql;
create table data.person as	
select 
	bene_id,
	bene_gender as sex,
	input(put(bene_dob_dt, yymmddn8.), 8.) as dob,
	round((&last_day. - bene_dob_dt)/365) as age_last,
	"S" as metal,
	0 as csr_indicator,
	12 as enrolduration
from ccmelig.chc_enrollment
where year(start_dt) <= &year. <= year(end_dt)
;
quit;

%mend;


