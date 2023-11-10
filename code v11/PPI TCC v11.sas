libname ccm "D:\SASData\SAS_Shared_Data\LDS_CCM";
libname out "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\tcc";
libname in "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\test v11\tcc";
libname meta "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11";
libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";


%let py=2017;
%let year=2017;
%let db=LDS;
%let epi=PPI_TCC;
%let enroll_type=FFS_AB;

options mprint mlogic;

***;

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

/*Identify all FFS non-ESRD beneficiaries that are eligible for PY*/

proc sort data=ccm.enrollment out=bene(keep=bene_id bene_dod_dt bene_dob_dt bene_gender bene_race start_dt end_dt);  
where enroll_type="&enroll_type." and (bene_dod_dt=. or bene_dod_dt>mdy(1,1,&py.))
	and .<start_dt<=mdy(1,1,&py.) and end_dt>mdy(1,1,&py.);
by bene_id start_dt end_dt;
run;
proc sort data=bene nodupkey; by bene_id; run;

data bene;
set bene;
format ana_beg ana_end mmddyy10.;

ana_beg=mdy(1,1,&py.);
if bene_dod_dt=. then ana_end=min(mdy(12,31,&py.), end_dt);
else ana_end=min(mdy(12,31,&py.), end_dt, bene_dod_dt);
if bene_dod_dt^=. and bene_dod_dt<=ana_end then death=1; else death=0;

month=month(ana_end);

age=floor((ana_beg-bene_dob_dt)/365.25);

%if &enroll_type.=COMM %then %do;
if age<18 then age_grp="<18  ";
else if 18<=age<=34 then age_grp="18-34";
else if 35<=age<=54 then age_grp="35-54";
else if 55<=age<=64 then age_grp="55-64";
else if age>=65 then age_grp="65+  ";
%end;
%else %do;
if age<65 then age_grp="<65  ";
else if 65<=age<=74 then age_grp="65-74";
else if 75<=age<=84 then age_grp="75-84";
else if age>=85 then age_grp="85+  ";
%end;

/*clin_cat="TCC";*/
def_sub="PPI_TCC";
def_id=bene_id;

run;

proc sql;
create table bene as
select a.*, y.bene_id is not null as dual
from bene a
inner join ccm.mdcr_status c on a.bene_id=c.bene_id and c.start_dt<=a.ana_beg and c.end_dt>=a.ana_end and c.mdcr_status_code in ('10' '20') 
left join ccm.dual y on a.bene_id=y.bene_id and ana_beg between y.start_dt and y.end_dt
order by bene_id, ana_beg, ana_end;
quit;
proc sort data=bene nodupkey; by bene_id; run;


*******************************;
*** retrospective alignment ***;
*******************************;

/*pull relevant claims*/

proc sql;
create table clm_medical as
select a.*, b.def_sub, b.def_id, b.month
from ccm.medical_&py. a, bene b
where a.dnl_flag^=1 and a.paid_dt<=intnx("MONTH", mdy(12,31,&py.), 3, 'S') 
	and a.bene_id=b.bene_id and a.thru_dt between b.ana_beg and b.ana_end;
quit;

proc sql;
create table out.&epi._serviceline as
select a.*, b.bill_type, b.at_npi, b.op_npi, b.ccn, b.tin, b.bill_npi, b.def_sub, b.def_id
from ccm.serviceline_&py.(keep=syskey bene_id clm_id thru_dt clm_type cpt rev_cd 
			line_num line_tin line_prf_npi line_prf_spec line_pos_cd allowed_amt) a,
	clm_medical b 
where a.bene_id=b.bene_id and a.syskey=b.syskey;
quit;

/*pull pqem services from physician and FQHC/RHC/CAH2 claims*/

data pqem;
set out.&epi._serviceline;
where bill_type in ("71" "77")
	or (cpt in (&em_1. &em_2. &em_3.) 
		and (clm_type="PHYS" or (bill_type="85" and substr(rev_cd,1,3) in ("096" "097" "098"))));
run;

proc sql;
create table pqem2 as
select distinct a.*, b.spec_cd
from pqem a
left join meta2.npi_x_spec_&py. b
on a.at_npi^="" and a.at_npi=b.npi;
quit;

/*keep pqem claims that meet both CPT and specialty criteria*/
data pqem3;
set pqem2;
length tin $11. spec_type $7.;

tin=coalescec(line_tin, ccn);
npi=coalescec(line_prf_npi, at_npi);
npi_spec=coalescec(line_prf_spec, spec_cd);

em=1;
if bill_type in ("71" "77") then spec_type="pcs";

if clm_type="PHYS" or bill_type="85" then do;
	if cpt in (&em_2.) and line_pos_cd='31' then em=0;
	if cpt in (&em_3.) and line_pos_cd='21' then em=0;
	if npi_spec in (&pcs.) then spec_type="pcs";
	if npi_spec in (&non_pcs.) then spec_type="non_pcs";
end;

if em=1 and spec_type^="";
run;

/* rollup pqem allowed to bene level */

%macro pqem_rollup(type);

proc sql;
create table pt_pqem_&type. as 
select bene_id, spec_type, &type., npi, sum(allowed_amt) as pqem, max(thru_dt) format mmddyy10. as last_visit
from pqem3
group by bene_id, spec_type, &type., npi
order by bene_id, spec_type, &type., npi;
quit;

proc sql;
create table pt_pqem2_&type. as
select distinct bene_id, spec_type, &type., npi, max(last_visit) format mmddyy10. as npi_last_visit, 
sum(pqem) as npi_pqem, sum(pqem) as npi_wt_pqem
from pt_pqem_&type.
group by bene_id, spec_type, &type., npi;
quit;

proc sql;
create table pt_pqem3_&type. as
select distinct *, sum(npi_wt_pqem) as &type._wt_pqem, max(npi_last_visit) format mmddyy10. as &type._last_visit
from pt_pqem2_&type.
group by bene_id, spec_type, &type.;
quit;

proc sql;
create table pt_pqem4_&type. as
select *, sum(npi_wt_pqem*(spec_type="pcs"))/sum(npi_wt_pqem) as pcs_pct, 
case when (calculated pcs_pct)>=0.1 then "pcs" else "non_pcs" end as align_type
from pt_pqem3_&type.
group by bene_id
order by bene_id, spec_type, &type._wt_pqem desc, &type._last_visit desc, npi_wt_pqem desc, npi_last_visit desc;
quit;

proc sort data=pt_pqem4_&type. out=pt_align_&type. nodupkey;
where spec_type=align_type;
by bene_id;
run;

proc datasets lib=work noprint; delete pt_pqem:; run;

%mend;

%pqem_rollup(tin);
%pqem_rollup(bill_npi);

proc sql;
create table prpay as
select bene_id, sum(prpay_amt) as prpay_total,
%if &enroll_type.=COMM %then %do; sum(allowed_amt) as ppi_total %end; 
%else %do; sum(allowed_amt)-max(0,sum(cptl_ime_amt+cptl_dsh_amt+ime_op_amt+dsh_op_amt+ucc_amt)) as ppi_total %end; 
from clm_medical
group by bene_id;
quit;

proc sql;
create table out.bene_&epi. as
select distinct a.*, b1.tin as attr_tin, b1.npi as attr_npi, b2.bill_npi as attr_p_bill, 
	b3.cbsa, b3.state, b3.county, b4.spec_cd, b5.spec_type
from bene(drop=start_dt end_dt) a
left join pt_align_tin b1 on a.bene_id=b1.bene_id
left join pt_align_bill_npi b2 on a.bene_id=b2.bene_id
left join meta2.provider_x_cbsa_&py. b3 on b3.provider_type="phys_npi" and b1.npi=b3.provider_id
left join meta2.npi_x_spec_&py. b4 on b1.npi^="" and b1.npi=b4.npi
left join meta2.cms_spec_cd b5 on b4.spec_cd=b5.spec_cd
left join prpay c on a.bene_id=c.bene_id
having c.prpay_total<=0 and c.ppi_total>0
order by bene_id;
quit;


**************************;
*** Calculate TCC Cost ***;
**************************;

%let planned_readm="001" "002" "005" "006" "007" "008" "009" "010" "011" "012" "013" "014" "015" "016" "017" "018" "019" 
					"650" "651" "652" "837" "838" "839" "846" "847" "848" "849" "945" "946" "949" "950";
proc sql;
create table clm_medical2 as
select a.*, c.dx3, c.pac_rf, d.ms_drg_type
from clm_medical a
inner join out.bene_ppi_tcc b on a.bene_id=b.bene_id
left join meta.icd_10_dx c on a.pdx_cd=c.dx7
left join meta.ms_drg d on a.ms_drg=d.ms_drg;
quit;

proc sql;
create table er as
select distinct bene_id, case_id, dx3 from clm_medical2 
where clm_type="OP" and setting="ER";
quit;
proc sql;
create table readm as
select distinct bene_id, case_id, ms_drg from clm_medical2 
where clm_type="IP" and setting in ("IPA" "IPF" "CAH") and (ms_drg_type="M" or adm_type="1") and ms_drg not in (&planned_readm.);
quit;

proc sql;
create table out.epi_clm_&epi. as
select distinct a.*, b.dx3 as er_dx3, b.case_id is not null as er, c.ms_drg as readm_drg, c.case_id is not null as readm
from clm_medical2 a
left join er b on a.bene_id=b.bene_id and a.case_id=b.case_id
left join readm c on a.bene_id=c.bene_id and a.case_id=c.case_id
order by bene_id, syskey, readm_drg;
quit;
proc sort data=out.epi_clm_&epi. nodupkey; by bene_id syskey; run;

data out.epi_clm_&epi.;
set out.epi_clm_&epi.;
%if &enroll_type.=COMM %then %do;
ppi_amt=allowed_amt;
%end;
%else %do;
ppi_amt=allowed_amt-max(0,sum(of cptl_ime_amt, cptl_dsh_amt, ime_op_amt, dsh_op_amt, ucc_amt));
%end;
if ppi_amt<0 then ppi_amt=0;
phase="POST";
episode_in_rank="";
%if &db.=LDS %then %do;
if adm_dt^=. and dis_dt^=. then los=dis_dt-adm_dt;
else los=thru_dt-from_dt;
%end;
los=thru_dt-from_dt;
run;

proc sql;
create table epi_summary_&epi. as
select distinct def_sub, def_id, month, sum(ppi_amt) as epi_pay, 
	sum(ppi_amt*(clm_type="IP")) as ip_pay, sum(ppi_amt*(clm_type="OP")) as op_pay,
	sum(ppi_amt*(clm_type="SNF")) as snf_pay, sum(ppi_amt*(clm_type="HHA")) as hha_pay,
	sum(ppi_amt*(clm_type="PHYS")) as phys_pay, sum(ppi_amt*(clm_type="DME")) as dme_pay,
	sum(ppi_amt*(clm_type="HSP")) as hsp_pay, 
	max(0,sum(los*(clm_type in ("IP" "SNF") and setting in ("IRF" "LTCH" "SNF")))) as ins_los,
	max(0,sum(ppi_amt*(clm_type in ("IP" "SNF") and setting in ("IRF" "LTCH" "SNF")))) as ins_cost,
	max(0,sum(readm*(clm_type="IP"))) as readm_cnt, max(0,sum(er*(clm_type="OP"))) as er_cnt,
	sum(ppi_amt*readm) as readm_cost, sum(ppi_amt*er) as er_cost
from out.epi_clm_&epi.
group by def_sub, def_id
order by def_sub, def_id;
quit;

data epi_summary_&epi.;
set epi_summary_&epi.;
array x epi_pay ip_pay op_pay snf_pay hha_pay phys_pay dme_pay hsp_pay ins_los ins_cost readm_cnt er_cnt readm_cost er_cost;
	do over x;
	x=x/month*12;
	end;
pc_pay=readm_cost+er_cost;
run;

proc sql; /* use bene zip code with the highest ppi_amt as the episode bene zip code */
create table bene_zip as
select def_id, bene_zip_cd, sum(ppi_amt) as tot_amt
from out.epi_clm_&epi.
group by def_id, bene_zip_cd
order by def_id, tot_amt desc;
quit;
proc sort data=bene_zip out=bene_zip nodupkey; where bene_zip_cd not in ("" "."); by def_id; run;

proc sql;
create table out.bene_cost_&epi. as
select distinct a.*, b.*, c.bene_zip_cd
from out.bene_&epi. a
left join epi_summary_&epi. b on a.bene_id=b.def_id
left join bene_zip c on a.bene_id=c.def_id
order by bene_id;
quit;

%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI Year Agg MACROs v11.sas";

%cost_bd_by_specialty;
%pc_report(readm, readm_drg, ms_drg,);
%pc_report(er, er_dx3, dx3, dx3_desc);
data out.epi_pc_&epi.; set readm_&epi. er_&epi.; year=&py.; run;
proc datasets lib=work noprint; delete readm_&epi. er_&epi. ; run;

**** steps above this can be run with rsubmit ****;
**** steps after this must have all benes combined ****;

%if &enroll_type.=COMM %then %do;
proc sql;
create table out.bene_cost_&epi. as
select distinct a.*, b.final_score as hcc_score, "PPI_TCC" as frailty_cat
from out.bene_cost_&epi. a
left join hcc.master_&py. b on a.bene_id=b.bene_id /*link to the HCC score output from the production run */
order by bene_id;
quit;
%end;

%else %do;
proc sql;
create table out.bene_cost_&epi. as
select distinct a.*, b.hcc_score as hcc_score, c.frailty as frailty_cat
from out.bene_cost_&epi. a
left join hcc_score_&py. b on a.bene_id=b.bene_id /*link to the HCC score output from the production run */
left join &benedir..ref_bene_x_frailty c on a.bene_id=c.bene_id and c.year=&py. /*link to the frailty output from the production run */
order by bene_id;
quit;
%end;

data out.bene_cost_&epi.;
set out.bene_cost_&epi.;

if spec_type not in ('PCP' 'MPCP') then spec_type="SCP";

%if &db.=LDS %then %do;
	hcc_score=epi_pay/5000+0.5*rand('uniform');
	if hcc_score<0.7 then frailty_cat="Level_1";
	else if hcc_score<1 then frailty_cat="Level_2";
	else if hcc_score<1.3 then frailty_cat="Level_3";
	else frailty_cat="Level_4";

	if attr_npi^="" then attr_npi=strip(spec_cd)||substr(attr_npi,2,1)||"XXXXXX";
	if attr_tin^="" then attr_tin=strip(spec_cd)||substr(attr_tin,2,1)||"XXXXXX";
	if attr_p_bill^="" then attr_p_bill=strip(spec_cd)||substr(attr_p_bill,2,1)||"XXXXXX";
	if substr(attr_npi,3,1) in ("0" "1" "2" "3" "4") then cbsa="1XXXXX"; else cbsa="2XXXXX";
	state="VA";
%end;

run;

proc univariate data=out.bene_cost_&epi. noprint;
var epi_pay pc_pay;
output out=outlier n=cnt pctlpre=epi_p pc_p pctlpts=1,2.5,97.5,99;
run;

proc sql;
create table out.bene_cost_&epi. as
select a.*, min(max(a.epi_pay,epi_p1),epi_p99) as t_epi_pay, min(max(a.pc_pay,pc_p1),pc_p99) as t_pc_pay
from out.bene_cost_&epi. a, outlier b
order by def_id;
quit;

***********************;
*** risk adjustment ***;
***********************;

proc sql;
create table out.bene_p_&epi. as
select *, 
mean(t_epi_pay)/mean(hcc_score)*hcc_score as p_t_epi_pay,
mean(t_pc_pay)/mean(hcc_score)*hcc_score as p_t_pc_pay,
mean(readm_cnt)/mean(hcc_score)*hcc_score as p_readm_cnt,
mean(er_cnt)/mean(hcc_score)*hcc_score as p_er_cnt,
mean(ins_los)/mean(hcc_score)*hcc_score as p_ins_los,
mean(death)/mean(hcc_score)*hcc_score as p_death
from out.bene_cost_&epi.
group by cbsa, spec_type
order by cbsa, bene_id;
quit;

data out.bene_p_&epi.; 
set out.bene_p_&epi.;
array x(6) p_t_epi_pay p_t_pc_pay p_readm_cnt p_er_cnt p_ins_los p_death;
do i=1 to 6;
	if x(i)<=0 then x(i)=0.00000001; 
end;

index_cm="";
index_er=.;
index_pay=.;
year=&py.;

drop i;
run;

%macro renorm(y);

*** winsorization ***;
proc sort data=out.bene_p_&epi.; by def_sub cbsa spec_type frailty_cat; run;
proc univariate data=out.bene_p_&epi. noprint;
	by cbsa spec_type frailty_cat;
	var p_&y.;
	output out=p_o_&y._&epi. n=cnt pctlpre=&y._p pctlpts=1,99;
run;

proc sql;
	create table out.bene_p_&epi. as
	select a.*, min(max(a.p_&y.,b.&y._p1),b.&y._p99) as t_p_&y.
	from out.bene_p_&epi. a, p_o_&y._&epi. b
	where a.cbsa=b.cbsa and a.spec_type=b.spec_type and a.frailty_cat=b.frailty_cat
	order by def_id;
quit;

*** renormalization by cbsa and specialty type ***;
proc sql;
create table out.bene_p_&epi. as
select *, t_p_&y.*(sum(&y.)/sum(t_p_&y.)) as n_p_&y.
from out.bene_p_&epi.
group by cbsa, spec_type, frailty_cat;
quit;

%mend;

%renorm(t_epi_pay);
%renorm(t_pc_pay);
%renorm(readm_cnt);
%renorm(er_cnt);
%renorm(ins_los);
%renorm(death);

* delete records with missing or wrong 2021 specialty ;

%if &db.=LDS %then %do;
proc sql;
create table out.bene_p_&epi. as
select a.*, substr(a.attr_npi,1,2) as spec_cd, c.spec_desc, c.spec_type
from out.bene_p_&epi.(drop=spec_cd spec_type) a
left join meta2.cms_spec_cd c on substr(a.attr_npi,1,2)=c.spec_cd
having spec_cd in (&pcs. &non_pcs.);
quit;
%end;
%else %do;
proc sql;
create table out.bene_p_&epi. as
select a.*, b.spec_cd, c.spec_desc, c.spec_type
from out.bene_p_&epi.(drop=spec_cd spec_type) a
left join meta.npi_x_spec_2021 b on a.attr_npi=b.npi
left join meta2.cms_spec_cd c on b.spec_cd=c.spec_cd
having spec_cd in (&pcs. &non_pcs.);
quit;
%end;

data bene_p_&epi.;
set out.bene_p_&epi.;
if spec_type not in ('PCP' 'MPCP') then spec_type="SCP";
run;

%include "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\code v11\PPI PY Score MACROs v11.sas";

%epi_var;

%macro score(prvdr);
%map_cbsa;
%rank(def_sub, frailty_cat, 1);
%rank(frailty_cat, frailty_cat, 2);

data out.r_sc_&prvdr._&epi.;
set out.r_sc1_&prvdr._&epi. out.r_sc2_&prvdr._&epi.;
run;
proc sort data=out.r_sc_&prvdr._&epi.; by provider_type provider_id measure lvl_1_value; run;

%add_benchmark(pc, frailty_cat, type, type, description, description);
%add_benchmark(cbd, frailty_cat, phase, clm_type, svc_cat, svc_sub);

%mend;

%score(attr_npi);
%score(attr_tin);
%score(attr_p_bill);


