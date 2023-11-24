libname ccm "D:\sas\ccm";

libname npi "D:\sas\ccm\npi";

%let py=2019;



***;

%let ay1_beg=mdy(7,1,%sysevalf(&py.)-3);

%let ay1_end=mdy(6,30,%sysevalf(&py.)-2);

%let ay2_beg=mdy(7,1,%sysevalf(&py.)-2);

%let ay2_end=mdy(6,30,%sysevalf(&py.)-1);



%let em_1='96160' '96161' '99201' '99202' '99203' '99204' '99205' '99211' '99212' '99213' '99214' '99215'

			'99324' '99325' '99326' '99327' '99328' '99334' '99335' '99336' '99337'

			'99339' '99340' '99341' '99342' '99343' '99344' '99345' '99347' '99348' '99349' '99350' '99421'

			'99422' '99423' '99441' '99442' '99443' '99483' '99484' '99492' '99493' '99494' 'G2214' 



			'G0076' 'G0077' 'G0078' 'G0079' 'G0080' 'G0081' 'G0082' 'G0083' 'G0084' 'G0085' 'G0086' 'G0087' 

			'99437' '99424' '99425' '99426' '99427' '99439' '99487' '99489' '99490' '99491' 'G2064' 'G2065' 

			'G0506' 'G0402' 'G0438' 'G0439' '99495' '99496' 'G0442' 'G0443' 'G0444' 'G0463' '99354' '99355'

			'G2212' 'G2010' 'G2012' 'G2252' ; 



%let em_2='99304' '99305' '99306' '99307' '99308' '99309' '99310' '99311' '99312' '99313' '99314' '99315'

			'99316' '99317' '99318'; /* SNF E&M */



%let em_3='99497' '99498'; /* ACP E&M */



%let pcs='01' '08' '11' '37' '38' '50' '89' '97';

%let non_pcs='06' '10' '12' '13' '16' '17' '23' '25' '26' '27' '29' '39' '44' '46' '66' 

				'70' '79' '82' '83' '84' '90' '98' '86';



/*Identify all FFS beneficiaries that are eligible for DCE alignment for PY2019*/

data bene;

set ccm.enrollment;

where enroll_type="FFS_AB" and .<start_dt<=&ay2_end. and end_dt>=&ay1_beg. 

	and (bene_dod_dt=. or bene_dod_dt>=mdy(1,1,&py.));

run;



/*pull pqem services from physician and FQHC/RHC/CAH2 claims*/

data medical(keep=syskey bene_id clm_id thru_dt bill_type clm_type at_npi ccn);

set ccm.medical_2016 ccm.medical_2017;

where &ay1_beg.<=thru_dt<=&ay2_end. and paid_dt<=intnx("MONTH", &ay2_end., 3, 'S');

run;

data serviceline(keep=syskey bene_id clm_id thru_dt clm_type cpt rev_cd 

						line_num line_tin line_prf_npi line_prf_spec line_pos_cd allowed_amt);

set ccm.serviceline_2016 ccm.serviceline_2017;

where &ay1_beg.<=thru_dt<=&ay2_end.;

run;



proc sql;

create table pqem as

select distinct a.bene_id, b.*, c.*

from bene a, medical b, serviceline c

where a.bene_id=b.bene_id and b.thru_dt between a.start_dt and a.end_dt and b.syskey=c.syskey

	and (b.bill_type in ("71" "77") or 

		(c.cpt in (&em_1. &em_2. &em_3.) and (c.clm_type="PHYS" or (b.bill_type="85" and substr(c.rev_cd,1,3) in ("096" "097" "098")))));

quit;



proc sql;

create table pqem2 as

select distinct a.*, b.cms_specialty_code, a.syskey=c.syskey as snf_flag

from pqem a

left join npi.npi_x_spec b

on a.at_npi=b.npi

left join (select distinct syskey from pqem where bill_type in ("71" "77") and rev_cd in ('0524' '0525')) c

on a.syskey=c.syskey;

quit;



/*keep pqem claims that meet both CPT and specialty criteria*/

data pqem3;

set pqem2;

length dce $11. spec_type $7.;



dce=coalescec(line_tin, ccn);

npi=coalescec(line_prf_npi, at_npi);

npi_spec=coalescec(line_prf_spec, cms_specialty_code);



em=1;

if bill_type in ("71" "77") then spec_type="pcs";



if clm_type="PHYS" or bill_type="85" then do;

	if cpt in (&em_2.) and line_pos_cd='31' then em=0;

	if cpt in (&em_3.) and line_pos_cd='21' then em=0;

	if npi_spec in (&pcs.) then spec_type="pcs";

	if npi_spec in (&non_pcs.) then spec_type="non_pcs";

end;



if &ay1_beg.<=thru_dt<=&ay1_end. then ay=1;

if &ay2_beg.<=thru_dt<=&ay2_end. then ay=2;



em2=em;

if snf_flag=1 then em2=0;



if em=1 and spec_type^="" and ay^=.;

run;



/* rollup pqem allowed to bene level */



%macro align(type);

proc sql;

create table pt_pqem as 

select bene_id, spec_type, dce, npi, ay, sum(allowed_amt) as pqem, max(thru_dt) format mmddyy10. as last_visit

from pqem3

%if &type.=em2 %then %do; where em2=1 %end;

group by bene_id, spec_type, dce, npi, ay

order by bene_id, spec_type, dce, npi, ay;

quit;



proc sql;

create table pt_pqem2 as

select distinct bene_id, spec_type, dce, npi, max(last_visit) format mmddyy10. as npi_last_visit, 

sum(pqem) as npi_pqem, sum(pqem*ay/3) as npi_wt_pqem

from pt_pqem

group by bene_id, spec_type, dce, npi;

quit;



proc sql;

create table pt_pqem3 as

select distinct *, sum(npi_wt_pqem) as dce_wt_pqem, max(npi_last_visit) format mmddyy10. as dce_last_visit

from pt_pqem2

group by bene_id, spec_type, dce;

quit;



proc sql;

create table pt_pqem4 as

select *, sum(npi_wt_pqem*(spec_type="pcs"))/sum(npi_wt_pqem) as pcs_pct, 

case when (calculated pcs_pct)>=0.1 then "pcs" else "non_pcs" end as align_type

from pt_pqem3

group by bene_id

order by bene_id, spec_type, dce_wt_pqem desc, dce_last_visit desc, npi_wt_pqem desc, npi_last_visit desc;

quit;



proc sort data=pt_pqem4 out=pt_align nodupkey;

where spec_type=align_type;

by bene_id;

run;



proc sql;

create table dce_bene_alignment_py_&py._&type. as

select distinct a.*, b.dce as aligned_dce, b.npi as aligned_npi

from bene(drop=start_dt end_dt) a

left join pt_align b

on a.bene_id=b.bene_id

order by bene_id;

quit;

%mend align;



%align(em);

%align(em2);



proc sql;

select count(*) as bene_cnt, sum(a.aligned_dce=b.aligned_dce) as match

from dce_bene_alignment_py_2019_em a, dce_bene_alignment_py_2019_em2 b

where a.bene_id=b.bene_id;

quit;



proc sql;

create table a as

select a.*, b.aligned_dce as dce2

from dce_bene_alignment_py_2019_em a, dce_bene_alignment_py_2019_em2 b

where a.bene_id=b.bene_id and a.aligned_dce^=b.aligned_dce;

quit;

