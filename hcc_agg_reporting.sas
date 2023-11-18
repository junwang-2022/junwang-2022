LIBNAME ECI00001 'D:\SASData\SAS_Shared_Data\shared\Edge';
options fullstimer;

data person_cc_20230516;
set eci00001.person_cc_20230516_00
	eci00001.person_cc_20230516_01
	eci00001.person_cc_20230516_03;
run;

/*avg(score_adult), avg(score_child), avg(score_infant)*/
/*0.980	0.354	2.163*/

proc sql;
create table x as
select avg(score_adult), avg(score_child), avg(score_infant), count(*)
from eci00001.person_cc_20230516_0;
run;


proc sql;
create table x as
select avg(score_adult), avg(score_child), avg(score_infant), count(*),
avg(score_adult_bronze),
avg(score_adult_gold),
avg(score_adult_platinum),
avg(score_adult_silver),
avg(score_adult_catastrophic),
avg(score_child_bronze),
avg(score_child_gold),
avg(score_child_platinum),
avG(score_child_silver),
avg(score_child_catastrophic),
avg(score_infant_bronze),
avg(score_infant_gold),
avg(score_infant_platinum),
avg(score_infant_silver),
avg(score_infant_catastrophic)
from person_cc_20230516
/*group by 1 order by 1;*/
run;
/**/
proc sql;	
create table x as
select
	metal,
/*	sex,*/
/*	case when age_last <= 1 then '0-1'*/
/*		when age_last between 2 and 13  then '2-13'*/
/*		when age_last between 14 and 20  then '14-20'*/
/*		when age_last between 21 and 25  then '21-25'*/
/*		when age_last between 26 and 34  then '26-34'*/
/*		when age_last between 35 and 44  then '35-44'*/
/*		when age_last between 45 and 54  then '45-54'*/
/*		when age_last between 55 and 64  then '55-64'*/
/*		when age_last >= 65 and 64  then '65+'*/
/*		else 'Oth' end as age,*/
	avg(score_adult/0.980) as score_adult,
	avg(score_child/0.354) as score_child,
	avg(score_infant/2.163) as score_infant,
	avg(case
	when metal='B' then score_adult_bronze
	when metal='G' then score_adult_gold
	when metal='P' then score_adult_platinum
	when metal='S' then score_adult_silver
	when metal='C' then score_adult_catastrophic
	end),
	avg(case
	when metal='B' then score_child_bronze
	when metal='G' then score_child_gold
	when metal='P' then score_child_platinum
	when metal='S' then score_child_silver
	when metal='C' then score_child_catastrophic
	end),
	avg(case
	when metal='B' then score_infant_bronze
	when metal='G' then score_infant_gold
	when metal='P' then score_infant_platinum
	when metal='S' then score_infant_silver
	when metal='C' then score_infant_catastrophic
	end),
	count(*)
from eci00001.person_cc_20230516_0
group by 1 order by 1;
run;


proc sql;
create table eci00001.y2 as
select c.metal, h.cc, count(distinct c.sysid) as benes, count(*) as c
from eci00001.person_cc_20230516_0 c
inner join eci00001.diagnosis_20230516_DD d
on c.sysid = d.sysid
inner join eci00001.hhs_hcc_table3 h
on d.diag = h.icd10
where h.fy2020 ='Y'
/*and substr(d.sysid,1,2) in ('00', '01', '03')*/
group by 1,2;
run;


