libname in "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\250K sample QC";
libname out "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\250K sample QC";

data ip;
set out.medical4;
where clm_type="IP";
run;

%macro dx_px(type, prefix);
proc sql;
create table &type. as
select a.*
from out.&type. a, ip b
where a.memberuid=b.memberuid and a.clm_id_new=b.clm_id_new
order by memberuid, clm_id_new, seq_num;
quit;

proc transpose data=&type. out=&type.2 prefix=&prefix.;
by memberuid clm_id_new;
var &type._cd;
id seq_num;
run;
%mend;
%dx_px(dx, dx);
%dx_px(px, proc);

proc sql;
create table ip2 as
select a.*, b.*, c.*, d.*
from ip a
left join in.member b on a.memberuid=b.memberuid
left join dx2(drop=dx1 _name_) c on a.memberuid=b.memberuid and a.clm_id_new=c.clm_id_new
left join px2(drop=proc1 _name_) d on a.memberuid=d.memberuid and a.clm_id_new=d.clm_id_new
order by memberuid, admissiondate, dischargedate;
quit;

data out.input_data_for_msdrg_group;
set ip2;
format dob mmddyy10.;
dob=mdy(1,1,birthyear);
rename 
ubpatientdischargestatuscode=discharge_status
gendercode=sex
dx_0=principal_dx
px_0=principal_px
memberuid=bene_id
clm_id_new=clm_id
;

keep memberuid dob gendercode clm_id_new admissiondate dischargedate dx_0 dx2-dx25 px_0 proc2-proc25 ubpatientdischargestatuscode;
run;

