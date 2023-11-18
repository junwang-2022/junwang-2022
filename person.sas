/*LIBNAME ECI00001 'D:\SASData\SAS_Shared_Data\shared\Edge';*/

/*proc sql;*/
/*create table rarecale_2020_agg as*/
/*select sysid, max(age) as age, max(gender) as gender,*/
/*	sum(enroll_days) as enroll_days, max(metal) as metal,*/
/*	max(csr) as csr, max(market) as market*/
/*from ECI00001.rarecale_2020*/
/*group by 1;*/


proc sql;
create table eci00001.person as
select sysid, gender as sex,
put(intnx('year', '31DEC2020'd, -1*age),yymmdd10.) as dob,
age as age_last,
metal,
case
	when csr = '00' then 0
	when csr = '01' then 2
	when csr = '04' then 3
	when csr = '05' then 2
	when csr = '06' then 1
	when csr = '11' then 1
	else 0 end
as csr_indicator,
case when enroll_days <= 31 then 1
	when enroll_days between 32 and 62 then 2
	when enroll_days between 63 and 92 then 3
	when enroll_days between 93 and 123 then 4
	when enroll_days between 124 and 153 then 5
	when enroll_days between 154 and 184 then 6
	when enroll_days between 185 and 214 then 7
	when enroll_days between 215 and 245 then 8
	when enroll_days between 246 and 275 then 9
	when enroll_days between 276 and 306 then 10
	when enroll_days between 307 and 335 then 11
	when enroll_days >= 336 and 366 then 12
end as ENROLDURATION
from rarecale_2020_agg;



data eci00001.person_20230516;
set eci00001.person(rename=(dob=dob1));
dob = input(dob1, yymmdd10.);
format dob yymmdd8.;
keep sysid sex dob age_last metal csr_indicator ENROLDURATION;
run;
/**/
/*NOTE: There were 28639876 observations read from the data set ECI00001.PERSON.*/
/*NOTE: The data set ECI00001.PERSON_20230516 has 28639876 observations and 7 variables.*/
/*NOTE: Compressing data set ECI00001.PERSON_20230516 decreased size by 44.14 percent. */
/*      Compressed is 70478 pages; un-compressed would require 126167 pages.*/
/*NOTE: DATA statement used (Total process time):*/
/*      real time           1:27.84*/
/*      cpu time            26.84 seconds*/
