* This program creates pre-requisite tables for the MS-DRG Grouper;

*Created by: Michelle Vergara;
*Created on: 10/31/2023;

libname jun "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\250K sample QC";
libname data "D:\SASData\dua_052882\Sndbx\Michelle_V\MSDRG\Data";

*create surrogate claim ID table;
proc sql;
create table data.claim_keys as
select clm_id, monotonic() as surr_key
from (select distinct clm_id 
	 from jun.input_data_for_msdrg_group)
;
quit;
