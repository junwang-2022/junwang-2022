
%macro readm_wt;
proc sql;
create table readm_&epi. as 
select bene_id, def_sub, def_id, clm_id, a.ms_drg, coalesce(b.weights,1) as weight
from in.epi_clm_&epi.(where=(readm=1 and clm_type="IP")) a
left join meta2.drg_weight b
on a.ms_drg=b.ms_drg and ((month(dis_dt)<=9 and year(dis_dt)=b.fy) or (month(dis_dt)>9 and year(dis_dt)+1=b.fy));
quit;

proc sql;
create table readm_wt_&epi. as
select bene_id, def_sub, def_id, sum(weight) as readm_wt
from readm_&epi.
group by bene_id, def_sub, def_id
order by def_sub, def_id;
quit;

proc sql;
create table ins_&epi. as
select a.*, b.case_id is not null as ins
from in.epi_clm_&epi. a
left join (select distinct def_id, case_id, dx3_desc from in.epi_clm_&epi. 
				where phase^="INDEX" and clm_type in ("IP" "SNF") and setting in ("IRF" "LTCH" "SNF")) b
on a.def_id=b.def_id and a.case_id=b.case_id;
quit;

proc sql;
create table ins_cost_&epi. as
select bene_id, def_sub, def_id, sum(ppi_amt*ins) as ins_cost
from ins_&epi.
group by bene_id, def_sub, def_id
order by def_sub, def_id;
quit;

proc sql;
create table out.bene_&epi. as
select a.*, coalesce(b.readm_wt, 0) as readm_wt, c.ins_cost
from in.bene_&epi. a
left join readm_wt_&epi. b on a.bene_id=b.bene_id and a.def_sub=b.def_sub and a.def_id=b.def_id
left join ins_cost_&epi. c on a.bene_id=c.bene_id and a.def_sub=c.def_sub and a.def_id=c.def_id;
quit;
%mend;

%macro model(y);
proc hpgenselect data=sample_&sub. noprint maxtime=10;
	class /*cbsa*/ spec_type index_setting index_cm age_grp cnt_hcc_grp ;
	model &y.=/*cbsa*/ spec_type index_setting index_cm age_grp dual index_er cnt_hcc_grp rf_: &hcc_list. 
		%if &y.= t_epi_pay %then %do; /include=7 dist=gamma link=log; %end;
		%if &y.= t_pc_pay or &y.= readm_wt %then %do; /include=7 dist=tweedie; %end;
		%if &y.= readm_cnt or &y.= er_cnt or &y.= ins_los %then %do; 
				/include=7 dist=zinb; 
				zeromodel /*cbsa*/ spec_type index_setting index_cm age_grp dual index_er cnt_hcc_grp rf_: &hcc_list. ;
		%end;
		%if &y.= death %then %do; /include=7 dist=binary link=logit; %end;
	selection method=forward(sle=0.15);
code file="&model.\sc_&sub._&y..sas";
run;
quit;

data mo_&y._&sub.;
set model_&sub.;
%include "&model.\sc_&sub._&y..sas";
run;
%mend;

%macro re_norm(y);
data p_&y._&epi.; 
set out.p_&y._&epi.;
	if p_&y.=. then p_&y.=mean_&y.; 
	if p_&y.=0 then p_&y.=0.00000001; 
run;

*** winsorization ***;
proc sort data=p_&y._&epi.; by def_sub /*cbsa*/ spec_type; run;
proc univariate data=p_&y._&epi. noprint;
	by def_sub /*cbsa*/ spec_type;
	var p_&y.;
	output out=p_o_&y._&epi. n=cnt pctlpre=&y._p pctlpts=1,99;
run;

proc sql;
	create table p_&y._&epi. as
	select a.*, min(max(a.p_&y.,b.&y._p1),b.&y._p99) as t_p_&y.
	from p_&y._&epi. a, p_o_&y._&epi. b
	where a.def_sub=b.def_sub /*and a.cbsa=b.cbsa*/ and a.spec_type=b.spec_type
	order by def_sub, def_id;
quit;

proc datasets lib=work noprint; delete p_o_:; run;

*** renormalization by specialty type ***;
proc sql;
create table out.p_&y._&epi. as
select *, sum(&y.)/sum(t_p_&y.) as oe, t_p_&y.*(calculated oe) as n_p_&y.
from p_&y._&epi.
group by def_sub, /*cbsa,*/ spec_type;
quit;

%mend;

%macro update_bad_model(y);
proc sql;
create table bad_mo_&y._&epi. as
select distinct def_sub, sum(oe>10)/count(*) as high_oe_pct
from
	(select distinct def_sub, attr_npi, count(*) as cnt, sum(&y.)/sum(n_p_&y.) as oe
	from out.p_&y._&epi.
	group by def_sub, attr_npi)
where cnt>10
group by def_sub;
quit;

proc sql noprint;
select distinct "'"||strip(def_sub)||"'" into :sub_list separated by " "
from bad_mo_&y._&epi.
where high_oe_pct>0.02;
quit;

%if &sqlobs.>0 %then %do;
	data out.p_&y._&epi.;
	set out.p_&y._&epi.(drop=t_p_&y. oe n_p_&y.);
	if def_sub in (&sub_list.) then p_&y.=.;
	run;
	%re_norm(&y.);
%end;
%mend;

%macro risk_adj;
proc datasets lib=work noprint; delete hcc: model: pred_: ; run;

proc sql;
create table hcc_&epi. as
	select a.*, b.*
	from out.bene_&epi. a
	left join in.hcc_&epi. b
	on %if &epi.^=PPI_CH_MED and &epi.^=PPI_CHEMO %then %do; a.def_sub=b.def_sub and %end; a.def_id=b.def_id
	order by def_sub, def_id;
quit;

data hcc_&epi.;
set hcc_&epi.;

array x hcc:;
do over x;
if x=. then x=0;
end;

cnt_hcc=sum(of hcc:);
if cnt_hcc=0 then cnt_hcc_grp="0  ";
else if cnt_hcc=1 then cnt_hcc_grp="1  ";
else if 2<=cnt_hcc<=3 then cnt_hcc_grp="2-3";
else if 4<=cnt_hcc<=6 then cnt_hcc_grp="4-6";
else cnt_hcc_grp="7+ ";

%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED %then %do; /* adj for specialty type only for OP acute and chronic episodes */
spec_type="x";
%end;

%if &epi.=PPI_PERINATAL %then %do;
def_sub="PPI_PERINATAL";
%end;

run;

proc sql noprint;
select distinct def_sub into:sublst separated by " " from hcc_&epi. where def_sub not in (select distinct del_epi from py.del_epi); quit;

%do i=1 %to %sysfunc(countw(&sublst.));
	%let sub=%scan(&sublst., &i.);

	proc sql;
		create table model_&sub. as
		select distinct *, mean(readm_wt) as mean_readm_wt,	mean(er_cnt) as mean_er_cnt, mean(ins_los) as mean_ins_los
		from hcc_&epi.
		where def_sub="&sub."
		group by spec_type, index_setting, index_cm, age_grp, dual, index_er;
	quit;

	proc sql noprint; select count(*) into:cnt from model_&sub.; quit;
	%if &cnt.>100000 %then %do;
		proc sort data=model_&sub.; 
		by cbsa spec_type index_setting index_cm age_grp dual index_er;
		run;
		proc surveyselect data=model_&sub. method=srs n=100000 seed=1234 out=sample_&sub.;
	   	strata cbsa spec_type index_setting index_cm age_grp dual index_er/alloc=proportional;
		run;
	%end;
	%else %do;
		data sample_&sub.; set model_&sub.; run;
	%end;

	proc sort data=sample_&sub.; by def_id; run;
	proc transpose data=sample_&sub.(keep=def_id hcc:) out=sample_&sub._trans; by def_id; run;
	proc sql; select count(*) into:cnt from sample_&sub; quit;
	proc sql;
	create table hcc_cnt_&sub. as
	select distinct _name_ as hcc, sum(col1) as hcc_cnt, sum(col1)/&cnt. as hcc_pct
		from sample_&sub._trans
		group by _name_;
	quit;

	proc sql;
	select distinct hcc into:hcc_list separated by " " from hcc_cnt_&sub. where hcc_cnt>=15 and hcc_pct>0.001;quit;

	%model(readm_wt);
	%model(er_cnt);
	%model(ins_los);

%end;
data out.p_readm_wt_&epi.(drop=hcc:);	set mo_readm_wt_:; run;
data out.p_er_cnt_&epi.(drop=hcc:); 	set mo_er_cnt_:; run;
data out.p_ins_los_&epi.(drop=hcc:); 	set mo_ins_los_:; run;

proc datasets lib=work noprint; delete mo_: model: sample: hcc: ; run;

%re_norm(readm_wt); 	%update_bad_model(readm_wt);
%re_norm(er_cnt); 		%update_bad_model(er_cnt);
%re_norm(ins_los); 		%update_bad_model(ins_los);

%mend risk_adj;

%macro bene_output;
%if &epi.=PPI_PERINATAL %then %do;
data out.bene_&epi.;
set out.bene_&epi.;
def_sub="PPI_PERINATAL";
run;
%end;
proc sql;
create table out.bene_p_&epi. as
	select a.*, d.n_p_readm_wt, e.n_p_er_cnt, f.n_p_ins_los
	from out.bene_&epi. a
	left join out.p_readm_wt_&epi. d on a.def_sub=d.def_sub and a.def_id=d.def_id 
	left join out.p_er_cnt_&epi. e on a.def_sub=e.def_sub and a.def_id=e.def_id 
	left join out.p_ins_los_&epi. f on a.def_sub=f.def_sub and a.def_id=f.def_id 
	order by def_sub, def_id ;
quit;

%if &epi.=PPI_CH_MED %then %do; /* convert PMPM to PMPY */
data out.bene_p_&epi.;
set out.bene_p_&epi.;
	readm_wt=readm_wt*12; er_cnt=er_wt*12; ins_los=ins_los*12;
	n_p_readm_wt=n_p_readm_wt*12; n_p_er_cnt=n_p_er_cnt*12; n_p_ins_los=n_p_ins_los*12;
run;
%end;
%mend;


%macro combine_year;
data bene_p_&epi.;
set y1.bene_p_&epi.(in=a) y2.bene_p_&epi.(in=b) y3.bene_p_&epi.(in=c);
if a then year=&py.;
if b then year=%sysevalf(&py.-1);
if c then year=%sysevalf(&py.-2);
run;

%if &epi. ne PPI_CH_MED and &epi. ne PPI_CHEMO and &epi. ne PPI_PERINATAL %then %do; 
	proc sort data=bene_p_&epi.; by bene_id def_sub index_beg; run;
	data bene_p_&epi.; 
		set bene_p_&epi; 
		by bene_id def_sub;
		retain epi_dt;
		if first.def_sub then do; epi_dt=ana_end; overlap=0; end;
		else if index_beg>epi_dt then do; epi_dt=ana_end; overlap=0; end;
		if overlap=0 then output;
	run;
%end;

proc sql;
create table out.bene_p_&epi. as
select a.*, b.spec_cd, c.spec_desc, c.spec_type
from bene_p_&epi.(drop=spec_cd spec_type) a
left join ref.npi_x_spec_&py. b on a.attr_npi=b.npi
inner join meta2.ppi_spec_mapping c on a.def_desc=c.episode_name and %if &epi.^=PPI_PERINATAL %then a.def_sub=c.def_sub and; b.spec_cd=c.spec_cd;
quit;

%mend;

%macro rollup(var, cost);
proc sql;
create table bene_&prvdr._&epi. as
select a.*, b.cbsa, b.state, b.county
from bene_p_&epi.(drop=cbsa state county) a
left join ref.provider_x_cbsa_&py. b
on a.&prvdr.=b.provider_id and 
	%if &prvdr.=attr_npi %then b.provider_type="phys_npi";
	%if %index(&prvdr., bill)>0 %then b.provider_type="bill_npi";
	%if &prvdr.=attr_tin %then %do;
		case when length(attr_tin)=6 then b.provider_type="ccn"
		else b.provider_type="tin" end
	%end;
	%if &prvdr.=attr_ccn %then b.provider_type="ccn";
	;
quit;

data bene_&prvdr._&epi.;
set bene_&prvdr._&epi.;
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_TCC %then %do; /* rank by specialty type only for OP acute and chronic episodes */
spec_type="x";
%end;
run;

proc sql;
create table &prvdr._&var. as
select distinct &py. as py, state, cbsa, spec_type, clin_cat, epi_type, "&epi." as def_name length=30, def_sub, def_desc,
	"&prvdr." as provider_type length=12, &prvdr. as provider_id, spec_desc,
	"&cost." as saving_type length=10, count(*) as epi_cnt, 
	mean(&cost.) as avg_cost,
	%if &var.=er_cnt %then %do; 
		mean(&var.)*1000 as avg_observed, mean(n_p_&var.)*1000 as avg_expected,%end;
	%else %do; mean(&var.) as avg_observed, mean(n_p_&var.) as avg_expected, %end;
	(calculated avg_observed)/(calculated avg_expected) as avg_oe, 
	min(1,sqrt(count(*)/100)) as credibility, (calculated avg_oe)*(calculated credibility)+(1-calculated credibility) as avg_oe_adj
from out.bene_&prvdr._&epi.
group by state, cbsa, spec_type, clin_cat, epi_type, def_sub, &prvdr., spec_desc
having provider_id ^=""
order by state, cbsa, spec_type, clin_cat, epi_type, def_sub, provider_type, provider_id, spec_desc;
quit;
%mend rollup;

%macro saving(prvdr);

proc sql;
create table bene_&prvdr._&epi. as
select a.*, b.cbsa, b.state, b.county
from out.bene_p_&epi.(drop=cbsa state county) a
left join meta2.provider_x_cbsa_&year. b
on a.&prvdr.=b.provider_id and 
	%if &prvdr.=attr_npi %then b.provider_type="phys_npi";
	%if %index(&prvdr., bill)>0 %then b.provider_type="bill_npi";
	%if &prvdr.=attr_tin %then %do;
		case when length(attr_tin)=6 then b.provider_type="ccn"
		else b.provider_type="tin" end
	%end;
	%if &prvdr.=attr_ccn %then b.provider_type="ccn";
	;
quit;

data out.bene_&prvdr._&epi.;
set bene_&prvdr._&epi.;
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_TCC %then %do; /* rank by specialty type only for OP acute and chronic episodes */
spec_type="x";
%end;
run;

%rollup(readm_wt, readm_cost);
%rollup(er_cnt, er_cost);
%rollup(ins_los, ins_cost);

data ru_&prvdr._&epi.; set &prvdr._:; run;
proc datasets lib=work noprint; delete &prvdr._:; run;

*** separate low vs. high vol providers ***;
data high_&prvdr._&epi. low_&prvdr._&epi.;
set ru_&prvdr._&epi.;
if epi_cnt>=11 then output high_&prvdr._&epi.;
else output low_&prvdr._&epi.;
run;

*** winsorize outlier oe ratio ***;
proc sort data=high_&prvdr._&epi.; by saving_type epi_type def_sub; run;
proc univariate data=high_&prvdr._&epi. noprint;
	by saving_type epi_type def_sub;
	var avg_oe;
	output out=ol_&prvdr._&epi. n=cnt pctlpre=oe_p pctlpts=1,99;
run;

proc sql;
	create table high_&prvdr._&epi. as
	select a.*, case when a.avg_oe>. then min(20,min(a.avg_oe,b.oe_p99)) end as w_oe, 
				case when a.avg_oe>. then avg_observed/min(20,min(a.avg_oe,b.oe_p99)) end as w_expected
	from high_&prvdr._&epi. a, ol_&prvdr._&epi. b
	where a.saving_type=b.saving_type and a.epi_type=b.epi_type and a.def_sub=b.def_sub 
	order by saving_type, epi_type, def_sub, avg_oe desc;
quit;

data high_&prvdr._&epi.;
set high_&prvdr._&epi.;
if .<avg_expected<w_expected then do;
	avg_expected=w_expected;
	avg_oe=w_oe;
	avg_oe_adj=avg_oe*credibility+(1-credibility);
end;
drop w_oe w_expected;
run;

*** check saving_types to be deleted ***;
proc sql;
create table del_m_&prvdr._&epi. as
select distinct saving_type, epi_type, def_sub,
	count(*) as phys_cnt, sum(epi_cnt) as epi_cnt, sum(avg_oe_adj=.)/count(*) as missing_oe_pct
from high_&prvdr._&epi.
group by saving_type, epi_type, def_sub
having missing_oe_pct>0.5
order by saving_type, epi_type, def_sub;
quit;

proc sql;
create table out.sav_type_&prvdr._&epi. as
select a.*, b.def_sub is not null as del_flag, max(0,avg_cost*(avg_oe_adj-1)/avg_oe_adj) as avg_saving
from high_&prvdr._&epi. a
left join del_m_&prvdr._&epi. b on a.saving_type=b.saving_type and a.epi_type=b.epi_type and a.def_sub=b.def_sub
having del_flag=0
order by epi_type, def_sub, provider_id, spec_desc, saving_type;
quit;

proc sort data=in.r_sc1_&prvdr._&epi. out=sc_&prvdr._&epi._c(keep=epi_type lvl_1_value provider_id ppi_score_adj);
where measure="t_epi_pay";
by epi_type lvl_1_value provider_id;
run;
proc sort data=in.r_sc1_&prvdr._&epi. out=sc_&prvdr._&epi._q(keep=epi_type lvl_1_value provider_id ppi_score_adj);
where measure="t_pc_pay";
by epi_type lvl_1_value provider_id;
run;

data out.saving_&prvdr._&epi.; 
merge 
out.sav_type_&prvdr._&epi.(where=(saving_type='readm_cost') rename=(avg_cost=readm_cost avg_oe_adj=readm_oe avg_saving=readm_avg_saving))
out.sav_type_&prvdr._&epi.(where=(saving_type='er_cost') rename=(avg_cost=er_cost avg_oe_adj=er_oe avg_saving=er_avg_saving))
out.sav_type_&prvdr._&epi.(where=(saving_type='ins_cost') rename=(avg_cost=ins_cost avg_oe_adj=ins_oe avg_saving=ins_avg_saving))
sc_&prvdr._&epi._c(rename=(lvl_1_value=def_sub ppi_score_adj=ppi_cost_score))
sc_&prvdr._&epi._q(rename=(lvl_1_value=def_sub ppi_score_adj=ppi_quality_score))
;
by epi_type def_sub provider_id;

epi_avg_saving=sum(of readm_avg_saving er_avg_saving ins_avg_saving);
epi_total_saving=epi_avg_saving*epi_cnt;

drop saving_type avg_observed avg_expected avg_oe del_flag credibility;
run;

proc datasets lib=work noprint; delete del_m_: &prvdr._: ru_: sc_:; run; quit;
%mend;
