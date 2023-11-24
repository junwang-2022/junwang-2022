libname in "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11"; ** builder output **;
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";

/*keep included episodes only*/
/*map trigger ccn wage index*/
/*add 7-day follow up and acp flag*/

proc sql;
create table bpcia_trigger_wi as
select a.*, b.ccn as trigger_ccn, c.wi as trigger_wi, d1.def_id is not null as fu7_flag, d2.def_id is not null as acp_flag
from in.bpcia_trigger(where=(exclusion^=1)) a
left join in.bpcia_episode_medical b on a.bene_id=b.bene_id and a.def_sub=b.def_sub and a.def_id=b.def_id and a.syskey=b.syskey
left join meta2.wi c on b.ccn=c.ccn and year(a.index_dt)=c.year
left join (select distinct bene_id, def_sub, def_id from in.bpcia_rf where risk_in_rank="RISK_IN_01") d1 on a.bene_id=d1.bene_id and a.def_sub=d1.def_sub and a.def_id=d1.def_id 
left join (select distinct bene_id, def_sub, def_id from in.bpcia_rf where risk_in_rank="RISK_IN_02") d2 on a.bene_id=d2.bene_id and a.def_sub=d2.def_sub and a.def_id=d2.def_id 
having trigger_wi^=.;
quit;


/*keep included claims for included episodes*/

proc sql;
create table bpcia_epi_clm as
select a.*, b.trigger_wi 
from in.bpcia_episode_medical(where=(episode_in_06^=1 and episode_in_07^=1)) a, bpcia_trigger_wi b 
where a.bene_id=b.bene_id and a.def_sub=b.def_sub and a.def_id=b.def_id
order by bene_id, def_sub, def_id, from_dt, thru_dt;
quit;


/*calculate BPCIA-relevant payment*/

data bpcia_epi_clm;
set bpcia_epi_clm;
bpcia_amt=allowed_amt-max(0,sum(of cptl_ime_amt, cptl_dsh_amt, ime_op_amt, dsh_op_amt, ucc_amt, excl_amt));
bpcia_std_amt=bpcia_amt/(trigger_wi*0.7+0.3);
run;



