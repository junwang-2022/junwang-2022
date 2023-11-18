* This program creates the input table for the MS-DRG Grouper;

*Created by: Michelle Vergara;
*Created on: 10/31/2023;

libname ip "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\250K sample QC";
libname data "D:\SASData\dua_052882\Sndbx\Michelle_V\MSDRG\Data";

%let source = input_data_for_msdrg_group;
%let label = sample;

*create temp table with calculated and dummy fields;

proc sql;
create table ip_base_layout_&label. as
select
	'' as patient_name,
	left(put(ip.bene_id, 13.)) as mrn,
	left(put(key.surr_key, 17.)) as acct_num,
	ip.admissiondate as admit_date,
	ip.dischargedate as discharge_date,
	coalesce(ip.discharge_status , '00') as discharge_status,
	'07' as primary_payer,
	put(intck('day',ip.admissiondate, ip.dischargedate), z5.) as los,
	ip.dob as dob,
	put(floor(YRDIF(ip.dob, ip.admissiondate,"AGE")), z3.) as age,
	case when sex = 'F' then '2'
		when sex = 'M' then '1' 
		else '0'
		end as sex,
	principal_dx as admit_dx length=7,
	principal_dx as principal_dx length=8,
	dx2 length =8,
	dx3 length =8,
	dx4 length =8,
	dx5 length =8,
	dx6 length =8,
	dx7 length =8,
	dx8 length =8,
	dx9 length =8,
	dx10 length =8,
	dx11 length =8,
	dx12 length =8,
	dx13 length =8,
	dx14 length =8,
	dx15 length =8,
	dx16 length =8,
	dx17 length =8,
	dx18 length =8,
	dx19 length =8,
	dx20 length =8,
	dx21 length =8,
	dx22 length =8,
	dx23 length =8,
	dx24 length =8,
	dx25 length =8,
	principal_px as principal_px length =7,
	proc2 length =7,
	proc3 length =7,
	proc4 length =7,
	proc5 length =7,
	proc6 length =7,
	proc7 length =7,
	proc8 length =7,
	proc9 length =7,
	proc10 length =7,
	proc11 length =7,
	proc12 length =7,
	proc13 length =7,
	proc14 length =7,
	proc15 length =7,
	proc16 length =7,
	proc17 length =7,
	proc18 length =7,
	proc19 length =7,
	proc20 length =7,
	proc21 length =7,
	proc22 length =7,
	proc23 length =7,
	proc24 length =7,
	'' as proc_date,
	'X' as HAC,
	'' as unused,
	'' as optional,
	'' as filler
from ip.&source. ip
left join data.claim_keys key
	on ip.clm_id = key.clm_id
;
quit;



proc sql;
create table ip_export_&label. as
select 
	repeat(' ', 31-1) as pat_name,
    case when mrn is null then repeat(' ',13-1)
        else mrn
        end ||
    case when acct_num is null then repeat(' ',17-1)
        else acct_num
        end ||
    put(admit_date, mmddyy10.) ||
    put(discharge_date, mmddyy10.) ||
    discharge_status ||
    primary_payer ||
    case when los = '00000' then '00001'
        else los
        end ||
    put(dob, mmddyy10.) ||
	case when age is null then '000'
        else age
        end ||
	sex ||
	admit_dx ||
	principal_dx ||
	dx2 ||
	dx3 ||
	dx4 ||
	dx5 ||
	dx6 ||
	dx7 ||
	dx8 ||
	dx9 ||
	dx10 ||
	dx11 ||
	dx12 ||
	dx13 ||
	dx14 ||
	dx15 ||
	dx16 ||
	dx17 ||
	dx18 ||
	dx19 ||
	dx20 ||
	dx21 ||
	dx22 ||
	dx23 ||
	dx24 ||
	dx25 ||
	principal_px ||
	proc2 ||
	proc3 ||
	proc4 ||
	proc5 ||
	proc6 ||
	proc7 ||
	proc8 ||
	proc9 ||
	proc10 ||
	proc11 ||
	proc12 ||
	proc13 ||
	proc14 ||
	proc15 ||
	proc16 ||
	proc17 ||
	proc18 ||
	proc19 ||
	proc20 ||
	proc21 ||
	proc22 ||
	proc23 ||
	proc24 as test_col,
	repeat(' ', 7-1) as proc_cd_filler,
	repeat(' ', (5*25)-1) as proc_dt_filler1,
	repeat(' ', (5*25)-1) as proc_dt_filler2,
	HAC,
   repeat(' ', 1+72+25-1) as end_filler
from ip_base_layout_&label.
;
quit;

data _null_;
set ip_export_sample;
file "D:\SASData\dua_052882\Users\Michelle_Vergara\ip_export_sample.txt" 
	lrecl=837;
put @1 pat_name
	@32 test_col
	@480 proc_cd_filler
	@487 proc_dt_filler1
	@612 proc_dt_filler2
	@737 HAC
	@738 end_filler $98.
	;
run;


 
