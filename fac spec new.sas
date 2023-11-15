libname a "C:\Users\jun.wang\PPI3.0\taxonomy\npi pos";
libname ccm "D:\sas\ccm";
libname meta2 "D:\sas\builder\meta2";
libname meta "C:\Users\jun.wang\Episode Builder\Episode builder v9\meta v9";

options mprint mlogic;

data a.ccn_name    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\CCN_INFORMATION_FULL.csv' delimiter = ','
 MISSOVER DSD lrecl=32767 firstobs=2 ;
        informat Federal_Provider_Number $6. ;
        informat Type $6. ;
        informat Provider_City $30. ;
        informat Provider_Name $100. ;
        informat Provider_Address $50. ;
        informat Provider_State $2. ;
        informat Provider_Zip_Code $5. ;
        format Federal_Provider_Number $6. ;
        format Type $6. ;
        format Provider_City $30. ;
        format Provider_Name $100. ;
        format Provider_Address $50. ;
        format Provider_State $2. ;
        format Provider_Zip_Code $5. ;
     input
                 Federal_Provider_Number $
                 Type  $
                 Provider_City  $
                 Provider_Name  $
                 Provider_Address  $
                 Provider_State  $
                 Provider_Zip_Code $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

******************************;
*** identify fac specialty ***;
******************************;

%macro vrdc;
*** VRDC code ***;

proc sql;
create table fac_type as
select distinct coalescec(ccn,bill_npi) as fac_id, clm_type, setting, count(*) as cnt
from medical_2021
where clm_type not in ('PHYS' 'DME' 'OP') or (clm_type='PHYS' and setting='ASC' and prf_spec='49')
group by coalescec(ccn,bill_npi), clm_type, setting
order by fac_id, cnt desc;
quit;
proc sort data=fac_type nodupkey; by fac_id; run;
proc sql;
create table fac_op as
select distinct ccn as fac_id, clm_type, setting, count(*) as cnt
from medical_2021
where clm_type='OP' and ccn not in (select distinct fac_id from fac_type)
group by ccn, clm_type, setting
order by fac_id, cnt desc;
quit;
proc sort data=fac_op nodupkey; by fac_id; run;
data fac_all;
set fac_type fac_op;
fac_type=setting;
run;
*** rollup prf spec ***;
proc sql;
create table fac_phys as
select a.fac_id, a.fac_type, b.bene_id, b.case_id, c.prf_spec, c.tot_amt
from fac_all a
left join (select distinct coalescec(ccn,bill_npi) as fac_id, bene_id, case_id from medical_2021) b
on a.fac_id=b.fac_id
left join (select bene_id, case_id, prf_spec, sum(allowed_amt) as tot_amt from medical_2021
				where clm_type='PHYS' and prf_spec^='49' group by bene_id, case_id, prf_spec) c
on b.bene_id=c.bene_id and b.case_id=c.case_id
order by fac_id, fac_type, bene_id, case_id;
quit;
proc sql;
create table fac_spec as
select distinct fac_type, fac_id, prf_spec, sum(tot_amt) as tot_amt
from fac_phys
group by fac_id, fac_type, prf_spec
order by fac_id, fac_type, prf_spec;
quit;
*** rollup by disease category ***;
proc sql;
create table fac_dx as
select distinct a.fac_type, a.fac_id, substr(pdx_cd,1,4) as dx4, sum(allowed_amt) as tot_amt
from fac_all a, medical_2021 b
where a.fac_id=coalescec(b.ccn, b.bill_npi)
group by fac_id, substr(pdx_cd,1,4)
order by fac_type, fac_id, dx4;
quit;
*** pg by spec and disease category ***;
proc sql;
create table pg_spec as
select bill_npi, prf_spec, sum(allowed_amt) as allowed_amt
from medical_2021
where clm_type='PHYS' and prf_spec^='49'
group by bill_npi, prf_spec;
quit;
proc sql;
create table pg_dx as
select bill_npi, substr(pdx_cd,1,4) as dx4, sum(allowed_amt) as allowed_amt
from medical_2021
where clm_type='PHYS' and prf_spec^='49'
group by 1, 2;
quit;

/* add where statements to exclude $0 payments */

data fac_spec_export;
set fac_spec;
where fac_id ~= '' and tot_amt ~= 0;
run;

data fac_dx_export;
set fac_dx;
where fac_id ~= '' and tot_amt ~= 0;
run;

data pg_spec_export;
set pg_spec;
where bill_npi ~= '' and allowed_amt ~= 0;
run;

data pg_dx_export;
set pg_dx;
where bill_npi ~= '' and allowed_amt ~= 0;
run;

*** by fac billing npi ***;

proc sql;
create table fac_bill as
select distinct bill_npi as fac_id, clm_type, setting, count(*) as cnt
from medical_2021
where clm_type not in ('PHYS' 'DME' 'OP')
group by bill_npi, clm_type, setting
order by fac_id, cnt desc;
quit;
proc sort data=fac_bill nodupkey; by fac_id; run;
proc sql;
create table fac_bill_op as
select distinct bill_npi as fac_id, clm_type, setting, count(*) as cnt
from medical_2021
where clm_type='OP' and bill_npi not in (select distinct fac_id from fac_bill)
group by bill_npi, clm_type, setting
order by fac_id, cnt desc;
quit;
proc sort data=fac_bill_op nodupkey; by fac_id; run;
data fac_bill_all;
set fac_bill fac_bill_op;
fac_type=setting;
run;
*** rollup prf spec ***;
proc sql;
create table fac_bill_phys as
select a.fac_id, a.fac_type, b.bene_id, b.case_id, c.prf_spec, c.tot_amt
from fac_bill_all a
left join (select distinct bill_npi as fac_id, bene_id, case_id from medical_2021) b
on a.fac_id=b.fac_id
left join (select bene_id, case_id, prf_spec, sum(allowed_amt) as tot_amt from medical_2021
				where clm_type='PHYS' and prf_spec^='49' group by bene_id, case_id, prf_spec) c
on b.bene_id=c.bene_id and b.case_id=c.case_id
order by fac_id, fac_type, bene_id, case_id;
quit;
proc sql;
create table fac_bill_spec as
select distinct fac_type, fac_id, prf_spec, sum(tot_amt) as tot_amt
from fac_bill_phys
group by fac_id, fac_type, prf_spec
order by fac_id, fac_type, prf_spec;
quit;
*** rollup by disease category ***;
proc sql;
create table fac_bill_dx as
select distinct a.fac_type, a.fac_id, substr(pdx_cd,1,4) as dx4, sum(allowed_amt) as tot_amt
from fac_bill_all a, medical_2021 b
where a.fac_id=b.bill_npi
group by fac_id, substr(pdx_cd,1,4)
order by fac_type, fac_id, dx4;
quit;

data fac_bill_spec_export;
set fac_bill_spec;
where fac_id ~= '' and tot_amt ~= 0;
run;

data fac_bill_dx_export;
set fac_bill_dx;
where fac_id ~= '' and tot_amt ~= 0;
run;

     data a.fac_bill_dx_export   ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\facility_rollups\FAC_BILL_DX_EXPORT.csv'
 delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
        informat fac_type $4. ;
        informat fac_id $10. ;
        informat dx4 $4. ;
        informat tot_amt best32. ;
        format fac_type $3. ;
        format fac_id $10. ;
        format dx4 $4. ;
        format tot_amt best12. ;
     input
                 fac_type  $
                 fac_id $
                 dx4  $
                 tot_amt
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

	      data a.fac_bill_spec_export    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\facility_rollups\FAC_BILL_SPEC_EXPORT.csv'
  delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
        informat fac_type $4. ;
        informat fac_id $10. ;
        informat PRF_SPEC $2. ;
        informat tot_amt best32. ;
        format fac_type $4. ;
        format fac_id $10. ;
        format PRF_SPEC $2. ;
        format tot_amt best12. ;
     input
                 fac_type  $
                 fac_id	$
                 PRF_SPEC $
                 tot_amt
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;


%mend;
**********************;
*** final analysis ***;

*** rollup by disease category ***;

%macro dx(type);
data &type._dx;
set a.&type._dx_export;
%if &type.=pg %then %do;
pg_type="PG";
rename bill_npi=pg_id allowed_amt=tot_amt;
%end;
%if &type.=fac_bill %then %do;
rename fac_type=fac_bill_type fac_id=fac_bill_id;
%end;
run;

proc sql;
create table &type._dx1 as
select distinct a.*, case when a.dx4 in ("Z510" "Z511") then "02.Neoplasms" else b.dx1_desc end as dx1_desc, 
	b.dx2_desc, b.dx3, b.dx3_desc, b.dx4_desc, substr(calculated dx1_desc,1,2) in ("18" "20" "21" "22" "") as dx1_excl
from &type._dx a, (select distinct dx1_desc, dx2_desc, dx3, dx3_desc, dx4, dx4_desc from meta.icd_10_dx) b
where a.dx4=b.dx4;
quit;

data &type._dx1;
set &type._dx1;

if dx4="Z510" then do;
dx2_desc="Radiation therapy";
dx3_desc="Radiation therapy";
dx4_desc="Radiation therapy";
end;
if dx4="Z511" then do;
dx2_desc="Chemotherapy";
dx3_desc="Chemotherapy";
dx4_desc="Chemotherapy";
end;
run;

%macro rollup(dx);
proc sql;
create table &type._sum_&dx. as
select *, tot_amt/sum(tot_amt) as pct
from (select "&dx." as dx_type, &type._type, &type._id, &dx._desc as dx_desc, sum(tot_amt) as tot_amt 
		from &type._dx1 where dx1_excl=0
		group by &type._type, &type._id, &dx._desc)
group by &type._type, &type._id
having pct>=0.3
order by &type._type, &type._id, pct desc;
quit;

proc transpose data=&type._sum_&dx. out=&type._sum_desc_&dx.; by dx_type &type._type &type._id; var dx_desc; run;
proc transpose data=&type._sum_&dx. out=&type._sum_pct_&dx.; by dx_type &type._type &type._id; var pct; run;

data &type._sum2_&dx.;
merge &type._sum_desc_&dx.(in=a) &type._sum_pct_&dx.(rename=(col1=pct1 col2=pct2 col3=pct3));
by &type._type &type._id;
if a;

length clin_1 $100. clin_2 $100. clin_3 $100.;
if col1^= "" then clin_1=strip(col1)||' ('||strip(put(round(pct1*100,1),3.))||'%)';
if col2^= "" then clin_2=strip(col2)||' ('||strip(put(round(pct2*100,1),3.))||'%)';
if col3^= "" then clin_3=strip(col3)||' ('||strip(put(round(pct3*100,1),3.))||'%)';

drop col: pct: _name_;
run;
%mend;

%rollup(dx1);
%rollup(dx2);
%rollup(dx3);

data &type._sum2_all;
set &type._sum2_dx:;
run;
proc sort data=&type._sum2_all; by &type._type &type._id descending dx_type; run;
proc sort data=&type._sum2_all nodupkey ; by &type._type &type._id; run;

proc sql;
create table &type._list as 
select &type._type, &type._id, sum(tot_amt) as &type._payment
from &type._dx
group by &type._type, &type._id;
quit;

data a.&type._dx_sum;
merge &type._list(in=a) &type._sum2_all;
by &type._type &type._id;
if a;

if dx_type="dx1" then do;
if clin_1^="" then clin_1=substr(clin_1,4,length(clin_1)-3);
if clin_2^="" then clin_2=substr(clin_2,4,length(clin_2)-3);
if clin_3^="" then clin_3=substr(clin_3,4,length(clin_3)-3);
end;
rename clin_1=primary_clinical_category clin_2=secondary_clinical_category clin_3=other_clinical_category;
drop dx_type;
run;

%mend;

%dx(fac);
%dx(pg);
%dx(fac_bill);

*** rollup by physician spec ***;

%macro spec(type);

data &type._spec;
set a.&type._spec_export;
prf_spec2=prf_spec;
if prf_spec in ('05' '32' '43' '09' '72') then prf_spec2='72';
if prf_spec in ('62' '68' '26' '80' '86') then prf_spec2='26';
if prf_spec in ('12' '25' '65' '67') then prf_spec2='25';
if prf_spec in ('10' '28') then prf_spec2='10';
if prf_spec in ('18' '41') then prf_spec2='18';
if prf_spec in ('13' '14') then prf_spec2='13';
if prf_spec in ('06' '21' '76' '77' '78' 'C3' 'C7' 'D8') then prf_spec2='06';
if prf_spec in ('20' '40' '23') then prf_spec2='20';
if prf_spec in ('82' '83' '90' '91' '92') then prf_spec2='90';
if prf_spec in ('16' '42' '98') then prf_spec2='16';
if prf_spec in ('07' 'D7') then prf_spec2='07';
if prf_spec in ('79' 'D5') then prf_spec2='79';
if prf_spec in ('30' '63') then prf_spec2='30';
if prf_spec in ('01' '08' '11' '38' '81' '84' '93' '70') then prf_spec2='01';

%if &type=pg %then %do;
pg_type="PG";
rename bill_npi=pg_id allowed_amt=tot_amt;
%end;
%if &type.=fac_bill %then %do;
fac_bill_type="FAC_BILL";
rename fac_id=fac_bill_id;
%end;
run;

proc sql;
create table &type._spec1 as
select a.*, b.tot_amt as &type._phys_payment, a.tot_amt/b.tot_amt as prf_spec_pct
from &type._spec a,
	(select &type._type, &type._id, sum(tot_amt) as tot_amt from &type._spec group by &type._type, &type._id) b
where a.&type._type=b.&type._type and a.&type._id=b.&type._id
order by &type._type, &type._id, prf_spec_pct desc;
quit;
proc sort data=&type._spec1 out=&type._spec_highest nodupkey; by &type._type &type._id; run;

proc sql;
create table &type._spec2 as
select &type._type, &type._id, prf_spec2, sum(tot_amt) as tot_amt, 
	case when prf_spec2 in ('72' '22' '30' '47' '50' '59' '69' '89' '97' 'C6' '') then 0 else 1 end as excl_spec
from &type._spec1 a
group by &type._type, &type._id, prf_spec2;
quit;

proc sql;
create table &type._spec3 as
select *, sum(tot_amt) as &type._spec_total, tot_amt/sum(tot_amt) as pct_all, tot_amt*excl_spec/sum(tot_amt*excl_spec) as pct_excl
from &type._spec2
group by &type._type, &type._id
having prf_spec2^=""
order by &type._type, &type._id, pct_all desc;
quit;

data &type._spec4;
set &type._spec3;
by &type._type &type._id;
retain seq_all;
if first.&type._id then seq_all=0;
seq_all+1;
run;
proc sort data=&type._spec4; by &type._type &type._id descending pct_excl; run;
data &type._spec5;
set &type._spec4;
by &type._type &type._id;
retain seq_excl;
if first.&type._id then seq_excl=0;
seq_excl+1;
run;

%macro par(spec,cd);
proc sql;
create table &type._par_&spec. as
select *, "&spec." as spec_grp length=20, seq_all as seq_num, pct_all as pct
from &type._spec5
where &type._id in (select distinct &type._id from &type._spec5 where pct_all>0.5 and prf_spec2="&cd.")
having pct_all>=0.3
order by &type._type, &type._id, pct_all desc;
quit;
%mend;

%par(Anesthesiology,72);
%par(Pathology,22);
%par(Radiology,30);

data &type._grp_par;
set &type._par_:;
run;

proc sql;
create table &type._grp_multi as
select *, "Multi-specialty" as spec_grp length=20, seq_excl as seq_num, pct_excl as pct
from &type._spec5 
where &type._id not in (select distinct &type._id from &type._par_anesthesiology) 
	and seq_num=1 and (pct_excl<0.3 or prf_spec2='01')
order by &type._type, &type._id, pct_all desc;
quit;

proc sql;
create table &type._grp_oth as
select *, seq_excl as seq_num, pct_excl as pct
from &type._spec5 
where &type._id not in (select distinct &type._id from &type._par_anesthesiology)
	and &type._id not in (select distinct &type._id from &type._grp_multi)
having pct_excl>=0.3 or seq_num=1
order by &type._type, &type._id, pct_excl desc;
quit;

data &type._grp_all;
set &type._par_anesthesiology(in=a) &type._grp_multi(in=b) &type._grp_oth(in=c);
run;

proc sql;
create table &type._grp_spec as
select distinct a.*, a1.prf_spec as &type._hi_spec, a1.prf_spec_pct as &type._hi_pct, a3.spec_desc as &type._hi_spec_desc, 
	a2.spec_desc, b1.spec_desc as nppes_spec
from &type._grp_all a 
left join &type._spec_highest a1 on a.&type._type=a1.&type._type and a.&type._id=a1.&type._id
left join meta2.cms_spec_cd a2 on a.prf_spec2=a2.spec_cd
left join meta2.cms_spec_cd a3 on a1.prf_spec=a3.spec_cd
left join npi.npi_spec_x_new b on a.&type._id=b.npi 
left join meta2.cms_spec_cd b1 on b.cms_specialty_code=b1.spec_cd
order by &type._type, &type._id, spec_grp, pct desc;
quit;

data &type._grp_spec;
set &type._grp_spec;
if prf_spec2='26' then spec_desc="Psychiatry/Psychology";
if prf_spec2='25' then spec_desc="Physical therapy and rehabilitation";
if prf_spec2='18' then spec_desc="Ophthalmology/Optometry";
if prf_spec2='20' then spec_desc="Orthopedic surgery/Sports medicine";
if prf_spec2='90' then spec_desc="Oncology";
if prf_spec2='01' then spec_desc="General medicine";
run;

proc transpose data=&type._grp_spec out=a.&type._spec_sum;
by &type._type &type._id spec_grp &type._spec_total &type._hi_pct &type._hi_spec_desc nppes_spec;
var spec_desc;
run;

data a.&type._spec_sum;
set a.&type._spec_sum;
rename col1=primary_taxonomy col2=secondary_taxonomy col3=other_taxonomy;
run;

%mend;

%spec(fac);
%spec(pg);
%spec(fac_bill);
%macro spec_dx(type);

proc sql;
create table a.&type._taxonomy as
select distinct a.&type._type, a.&type._id, a.&type._payment, b.&type._spec_total, 
	b.primary_taxonomy, b.secondary_taxonomy, b.other_taxonomy, b.spec_grp, b.nppes_spec, b.&type._hi_spec_desc,
	a.primary_clinical_category, a.secondary_clinical_category, a.other_clinical_category, 
	coalescec(c.providerorganization, d.provider_name, strip(c.first_name)||" "||strip(c.last_name)) as &type._name,
	substr(coalescec(c.prvd_bz_addresszip, d.provider_zip_code),1,5) as fac_zip, 
	coalescec(c.prvd_bz_addressstate, d.provider_state) as fac_state,
	e.county_desc as county, e.cbsa_desc as cbsa
from a.&type._dx_sum a
left join a.&type._spec_sum b on a.&type._type=b.&type._type and a.&type._id=b.&type._id
left join meta2.npi_spec_x_new c on a.&type._id=c.npi 
left join meta2.ccn_name d on a.&type._id=d.federal_provider_number
left join meta2.zip_cbsa e on substr(coalescec(c.prvd_bz_addresszip, d.provider_zip_code),1,5)=e.zip_cd
order by &type._type, &type._id;
quit;
%mend;

%spec_dx(fac);
%spec_dx(pg);
%spec_dx(fac_bill);

data a.fac_taxonomy2;
set a.fac_taxonomy;

if spec_grp="Multi-specialty" then primary_taxonomy="Multi-specialty";
if fac_spec_total<10000 or fac_type in ("RHC" "FQHC" "HSP" "HHA") then do;
	primary_taxonomy="NA - low/incomplete physician payment";
	secondary_taxonomy="";
	other_taxonomy="";
end;

if primary_clinical_category=" " then primary_clinical_category="Multiple clinical categories";
if fac_payment<30000 then do;
	primary_clinical_category="NA - low facility payment";
	secondary_clinical_category="";
	other_clinical_category="";
end;
drop spec_grp nppes_spec fac_hi_spec_desc;
run;

data a.pg_taxonomy2;
set a.pg_taxonomy;

primary_taxonomy=coalescec(nppes_spec, pg_hi_spec_desc); 
if primary_clinical_category=" " then primary_clinical_category="Multiple clinical categories";

drop spec_grp nppes_spec secondary_taxonomy other_taxonomy pg_spec_total pg_hi:;
run;


proc export data=a.fac_taxonomy2 outfile="C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\fac_taxonomy2.csv" replace; run;
proc export data=a.pg_taxonomy2 outfile="C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\pg_taxonomy2.csv" replace; run;



