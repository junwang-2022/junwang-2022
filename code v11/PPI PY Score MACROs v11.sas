%macro combine_year;
data out.bene_p_&epi.;
set y1.bene_p_&epi. 
%if &db. ne LDS %then %do;
%if &epi. ne PPI_CHEMO and &epi. ne PPI_PERINATAL %then y2.bene_p_&epi. y3.bene_p_&epi.;
%end;
;
run;

%if &epi. ne PPI_CH_MED and &epi. ne PPI_CHEMO and &epi. ne PPI_PERINATAL %then %do; 
	proc sort data=out.bene_p_&epi.; by bene_id def_sub index_beg; run;
	data out.bene_p_&epi.; 
		set out.bene_p_&epi; 
		by bene_id def_sub;
		retain epi_dt;
		if first.def_sub then do; epi_dt=ana_end; overlap=0; end;
		else if index_beg>epi_dt then do; epi_dt=ana_end; overlap=0; end;
		if overlap=0 then output;
	run;
%end;

%macro combine_pc_cbd(type);
data epi_&type._&epi.;
set y1.epi_&type._&epi.
%if &db. ne LDS %then %do;
%if &epi. ne PPI_CHEMO and &epi. ne PPI_PERINATAL %then y2.epi_&type._&epi. y3.epi_&type._&epi.;
%end;
;
run;

proc sql;;
create table out.epi_&type._&epi. as
select a.*
from epi_&type._&epi. a, out.bene_p_&epi. b
where a.def_sub=b.def_sub and a.def_id=b.def_id and a.year=b.year;
quit;
%mend;
%combine_pc_cbd(pc);
%combine_pc_cbd(cbd);

%mend;

%macro epi_var;

data epi_all ; 
set out.bene_p_ppi_:;
keep def_sub county state cbsa t_epi_pay n_p_t_epi_pay t_pc_pay index_pay epi_pay phys_pay dme_pay ip_pay op_pay snf_pay hha_pay hsp_pay rx_pay attr_:;
county=strip(state)||", "||strip(county);
run;

%macro geo(grp, geo_type, geo_name);
proc sql;
create table geo_&geo_type. as
select &py. as py, def_sub, "&geo_type." as geo_type length=10, &geo_name. as geo_name, count(*) as epi_cnt, mean(t_epi_pay) as avg_epi_pay,
	std(t_epi_pay/n_p_t_epi_pay) as cv, mean(t_pc_pay)/mean(t_epi_pay) as pc_pct, mean(index_pay)/mean(epi_pay) as trigger_pct, 
	mean(epi_pay) as avg_episode_cost, mean(phys_pay+dme_pay) as avg_partb_cost, mean(ip_pay) as avg_inpatient_cost, mean(op_pay) as avg_outpatient_cost, 
	mean(snf_pay) as avg_snf_cost, mean(hha_pay) as avg_hha_cost, mean(hsp_pay) as avg_hospice_cost %if &epi.=PPI_CHEMO %then %do; , mean(rx_pay) as avg_rx_cost %end;
from epi_all
group by def_sub &grp.;
quit;
%mend;

%geo(%str(, cbsa), CBSA, cbsa);
%geo(%str(, county), County, county);
%geo(%str(, state), State, state);
%geo(%str(), Nation, "Nation");

data out.r_epi_var;
set geo_:;
if epi_cnt<11 then epi_cnt=.;
run;

%macro npi_xwalk(type);
proc sql;
create table npi_x_&type. as
select attr_npi, state, cbsa, "&type." as xwalk_type, def_sub, &type. as xwalk_provider, count(*) as epi_cnt
from epi_all
group by def_sub, state, cbsa, attr_npi, &type.
union corr
select attr_npi, state, cbsa, "&type." as xwalk_type, "All" as def_sub, &type. as xwalk_provider, count(*) as epi_cnt
from epi_all
group by state, cbsa, attr_npi, &type.
order by state, cbsa, attr_npi, xwalk_type, def_sub, epi_cnt desc;
quit;
%mend;
%npi_xwalk(attr_tin);
%npi_xwalk(attr_p_bill);
%npi_xwalk(attr_f_bill);
%npi_xwalk(attr_ccn);

data out.r_npi_xwalk;
set npi_x_:;
if epi_cnt<11 then epi_cnt=.;
run;

%mend;

%macro clean;

* delete 35 def_sub from reporting ;
proc sql;
create table bene_p_&epi. as
select *
from out.bene_p_&epi.
where def_sub not in (select del_epi from out.del_epi);
quit;

* delete records with missing or wrong specialty ;
%if &db. ne LDS %then %do;
proc sql;
create table bene_p_&epi. as
select a.*, b.spec_cd, c.spec_desc, c.spec_type
from bene_p_&epi.(drop=spec_cd spec_type) a
left join meta.npi_x_spec_&py. b on a.attr_npi=b.npi
inner join meta.ppi_spec_mapping c on a.def_sub=c.def_sub and b.spec_cd=c.spec_cd;
quit;
%end;
%else %do;
proc sql;
create table bene_p_&epi. as
select a.*, substr(a.attr_npi,1,2) as spec_cd, c.spec_desc, c.spec_type
from bene_p_&epi.(drop=spec_cd spec_type) a
inner join meta2.ppi_spec_mapping c on a.def_sub=c.def_sub and substr(a.attr_npi,1,2)=c.spec_cd;
quit;
%end;

%mend;

%macro map_cbsa;

proc sql;
create table bene_&prvdr._&epi. as
select a.*, b.cbsa, b.state, b.county
from bene_p_&epi.(drop=cbsa state county) a
left join meta2.provider_x_cbsa_&py. b
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

%if &db. = LDS %then %do;
data bene_&prvdr._&epi.;
set bene_&prvdr._&epi.;
if substr(&prvdr.,3,1) in ("0" "1" "2" "3" "4") then cbsa="1XXXXX"; else cbsa="2XXXXX"; 
state="VA";
run;
%end;

data bene_&prvdr._&epi.;
set bene_&prvdr._&epi.;
%if &epi. ne PPI_AC_MED and &epi. ne PPI_CH_MED and &epi. ne PPI_TCC %then %do; /* rank by specialty type only for OP acute and chronic episodes */
spec_type="x";
%end;
run;
%mend;

%macro rollup(var);
proc sql;
create table &prvdr._&var. as
select distinct &py. as py, state, cbsa, spec_type,
	"&lvl_1." as rpt_lvl_1 length=20, &lvl_1. as lvl_1_value length=100, 
	"&prvdr." as provider_type length=12, &prvdr. as provider_id, spec_desc,
	"&var." as measure length=10, count(*) as epi_cnt, 
	%if &var.=readm_cnt or &var.=er_cnt %then %do; 
		mean(&var.)*1000 as avg_observed, mean(n_p_&var.)*1000 as avg_expected,%end;
	%else %do; mean(&var.) as avg_observed, mean(n_p_&var.) as avg_expected, %end;
	(calculated avg_observed)/(calculated avg_expected) as avg_oe, 
	min(1,sqrt(count(*)/100)) as credibility, (calculated avg_oe)*(calculated credibility)+(1-calculated credibility) as avg_oe_adj
from bene_&prvdr._&epi.
group by state, cbsa, spec_type, &lvl_1., &prvdr., spec_desc
having provider_id ^=""
order by state, cbsa, spec_type, lvl_1_value, provider_type, provider_id, spec_desc;
quit;
%mend rollup;

%macro rerank(type, name);
proc sql;
create table range_&prvdr._&epi. as
select &type., spec_type, lvl_1_value, measure, rank_oe_adj, min(avg_oe_adj) as min_oe, max(avg_oe_adj) as max_oe
from r_&prvdr._&epi._&name.
group by &type., spec_type, lvl_1_value, measure, rank_oe_adj;
quit;

proc sql;
create table rank_&prvdr._&epi. as
select distinct a.&type., a.spec_type, a.lvl_1_value, a.measure 
from range_&prvdr._&epi.(where=(rank_oe_adj in (0,1,2) and min_oe<1)) a
left join range_&prvdr._&epi. b on b.rank_oe_adj=1 and b.min_oe>1 and a.&type.=b.&type. and a.spec_type=b.spec_type and a.lvl_1_value=b.lvl_1_value and a.measure=b.measure
left join range_&prvdr._&epi. c on c.rank_oe_adj=2 and 0.9<c.max_oe<=1 and a.&type.=c.&type. and a.spec_type=c.spec_type and a.lvl_1_value=c.lvl_1_value and a.measure=c.measure
having b.measure is null or c.measure is null;
quit;

proc sql;
create table rank2_&prvdr._&epi. as
select a.*, b.measure is not null as flag
from r_&prvdr._&epi._&name. a
left join rank_&prvdr._&epi. b
on a.&type.=b.&type. and a.spec_type=b.spec_type and a.lvl_1_value=b.lvl_1_value and a.measure=b.measure;
quit;

data r_&prvdr._&epi._&name.;
set rank2_&prvdr._&epi.;

rerank_oe_adj=rank_oe_adj;

if flag=1 then do;
if rerank_oe_adj in (0,1) and avg_oe_adj>1 then change=0;
if rerank_oe_adj in (3,4) and avg_oe_adj<1 then change=0;
if change^=0 then do;
	if 0.9<avg_oe_adj<1.1 then rerank_oe_adj=2;
	else if avg_oe_adj<0.9 and rank_oe_adj^=4 then rerank_oe_adj=3;
end;
end;
run;
proc sort; by cbsa measure descending avg_oe_adj rank_oe_adj; run;
%mend;
%macro rank(lvl_1, lvl_2, flag);


%rollup(t_epi_pay);
%rollup(t_pc_pay);
%rollup(readm_cnt);
%rollup(er_cnt);
%rollup(ins_los);
%rollup(death);

data ru&flag._&prvdr._&epi.; set &prvdr._:; run;
proc datasets lib=work noprint; delete &prvdr._:; run;

*** separate low vs. high vol providers ***;
data high&flag._&prvdr._&epi. low&flag._&prvdr._&epi.;
set ru&flag._&prvdr._&epi.;
if epi_cnt>=11 then output high&flag._&prvdr._&epi.;
else output low&flag._&prvdr._&epi.;
run;

*** winsorize outlier oe ratio ***;
proc sort data=high&flag._&prvdr._&epi. out=high&flag._&prvdr._&epi.; by measure lvl_1_value; run;
proc univariate data=high&flag._&prvdr._&epi. noprint;
	by measure lvl_1_value;
	var avg_oe;
	output out=ol_&prvdr._&epi. n=cnt pctlpre=oe_p pctlpts=1,99;
run;

proc sql;
	create table high&flag._&prvdr._&epi. as
	select a.*, case when a.avg_oe>. then min(20,min(a.avg_oe,b.oe_p99)) end as w_oe, 
				case when a.avg_oe>. then avg_observed/min(20,min(a.avg_oe,b.oe_p99)) end as w_expected
	from high&flag._&prvdr._&epi. a, ol_&prvdr._&epi. b
	where a.measure=b.measure and a.lvl_1_value=b.lvl_1_value 
	order by measure, lvl_1_value, avg_oe desc;
quit;

data high&flag._&prvdr._&epi.;
set high&flag._&prvdr._&epi.;
if .<avg_expected<w_expected then do;
	avg_expected=w_expected;
	avg_oe=w_oe;
	avg_oe_adj=avg_oe*credibility+(1-credibility);
end;
drop w_oe w_expected;
run;

*** check measures to be deleted ***;
proc sql;
create table del_m_&prvdr._&epi. as
select distinct measure, rpt_lvl_1, lvl_1_value,
	count(*) as phys_cnt, sum(epi_cnt) as epi_cnt, sum(avg_oe_adj=.)/count(*) as missing_oe_pct
from high&flag._&prvdr._&epi.
group by measure, rpt_lvl_1, lvl_1_value
having missing_oe_pct>0.5
order by measure, rpt_lvl_1, lvl_1_value;
quit;

proc sql;
create table high&flag._&prvdr._&epi. as
select a.*, b.lvl_1_value is not null as del_flag
from high&flag._&prvdr._&epi. a
left join del_m_&prvdr._&epi. b
on a.measure=b.measure and a.lvl_1_value=b.lvl_1_value
order by measure, rpt_lvl_1, lvl_1_value, provider_id, spec_desc;
quit;

*** check provider count in each comparison group ***;

proc sql;
create table cnt_&prvdr._&epi. as
select distinct *, count(*) as prvdr_cnt
from high&flag._&prvdr._&epi.
where del_flag=0
group by cbsa, spec_type, lvl_1_value, measure
order by cbsa, spec_type, lvl_1_value, measure;
quit;

proc rank data=cnt_&prvdr._&epi. out=r_&prvdr._&epi._cbsa group=5 descending ties=high; /* rank by cbsa */
where prvdr_cnt>=5;
by cbsa spec_type lvl_1_value measure;
var avg_oe avg_oe_adj;
ranks rank_oe rank_oe_adj;
run;
%rerank(cbsa, cbsa);

proc sort data=cnt_&prvdr._&epi.; by state spec_type lvl_1_value measure; run; /* rank by state */
proc rank data=cnt_&prvdr._&epi. out=r_&prvdr._&epi._st group=5 descending ties=high;
where prvdr_cnt<5;
by state spec_type lvl_1_value measure;
var avg_oe avg_oe_adj;
ranks rank_oe rank_oe_adj;
run;
%rerank(state, st);

data r_sc&flag._&prvdr._&epi.; 
set r_&prvdr._&epi._cbsa(in=a) r_&prvdr._&epi._st(in=b);
if a then rank_type="CBSA ";
if b then rank_type="STATE";
ppi_score=rank_oe+1; 
ppi_score_adj=rerank_oe_adj+1; 
drop rank_oe: rerank: prvdr_cnt; 
run;

proc sql;
create table out.r_sc&flag._&prvdr._&epi as
select a.*, b.rank_type, b.ppi_score, b.ppi_score_adj
from high&flag._&prvdr._&epi. a
left join r_sc&flag._&prvdr._&epi. b
on a.cbsa=b.cbsa and a.spec_type=b.spec_type and a.spec_desc=b.spec_desc
	and a.lvl_1_value=b.lvl_1_value and a.measure=b.measure and a.provider_id=b.provider_id
order by cbsa, spec_type, lvl_1_value, measure, rank_type, avg_oe_adj desc; 
quit;

proc datasets lib=work noprint; delete del_m_: cnt_: r_&prvdr.: r_sc: range: rank:; run;
%mend;

%macro rollup2(var);
proc sql;
create table setting_&prvdr._&var. as
select distinct &py.  as py, state, cbsa, "&prvdr." as provider_type length=12, &prvdr. as provider_id, spec_type,  
	def_sub, index_setting, "&var." as measure length=10, count(*) as epi_cnt, 
	%if &var.=readm_cnt or &var.=er_cnt %then %do; 
		mean(&var.)*1000 as avg_observed, mean(n_p_&var.)*1000 as avg_expected,%end;
	%else %do; mean(&var.) as avg_observed, mean(n_p_&var.) as avg_expected, %end;
	(calculated avg_observed)/(calculated avg_expected) as avg_oe, 
	min(1,sqrt(count(*)/100)) as credibility, (calculated avg_oe)*(calculated credibility)+(1-calculated credibility) as avg_oe_adj
from bene_&prvdr._&epi.
group by cbsa, &prvdr., spec_type, def_sub, index_setting
having provider_id ^=""
order by state, cbsa, spec_type, def_sub, provider_type, provider_id, index_setting;
quit;
%mend rollup2;

%macro rollup_setting;
%rollup2(t_epi_pay);
%rollup2(t_pc_pay);
%rollup2(readm_cnt);
%rollup2(er_cnt);
%rollup2(ins_los);
%rollup2(death);

data r_s1_&prvdr._&epi.; set setting_&prvdr._:; run;
proc sql;
	create table out.r_s1_&prvdr._&epi. as
	select distinct a.*
	from r_s1_&prvdr._&epi. a, (select distinct lvl_1_value, provider_id from out.r_sc1_&prvdr._&epi. where measure='t_epi_pay' and ppi_score_adj^=.) b
	where a.def_sub=b.lvl_1_value and a.provider_id=b.provider_id
	order by state, cbsa, def_sub, provider_type, provider_id, index_setting, measure;
quit;
proc datasets lib=work noprint; delete setting_&prvdr._: r_s:; run;
%mend;

%macro add_benchmark(type, cat, var1, var2, var3, var4);
%if &type.=pc %then %do;
proc sql;
create table &type._&epi. as 
select *
from out.epi_pc_&epi.
union corr 
select def_sub, def_id, type, "All" as description, sum(visit_cnt) as visit_cnt, sum(tot_amt) as tot_amt, year
from out.epi_pc_&epi.
group by def_sub, def_id, type, year;
quit;
%end;
%if &type.=cbd %then %do;
proc sql;
create table &type._&epi. as 
select *
from out.epi_cbd_&epi.
union corr 
select def_sub, def_id, "All" as phase, "All" as clm_type, "All" as svc_cat, "All" as svc_sub, sum(tot_amt) as tot_amt, year
from out.epi_cbd_&epi.
where svc_cat="Total"
group by def_sub, def_id, year;
quit;
%end;

proc sql;
create table c_&prvdr._&epi. as
select a.*, %if &epi.=PPI_TCC or &epi.=PPI_CH_MED %then a.tot_amt/b.month*12; %else a.tot_amt; as total_amt, 
	%if &type.=pc %then %do; %if &epi.=PPI_TCC or &epi.=PPI_CH_MED %then round(a.visit_cnt/b.month*12,1); %else a.visit_cnt; as total_visit, %end;
	b.&cat., b.state, b.cbsa, b.&prvdr., b.spec_type, b.index_cm, b.age_grp, b.dual, b.index_er, b.t_epi_pay
from &type._&epi. a, bene_&prvdr._&epi. b
where a.def_sub=b.def_sub and a.def_id=b.def_id and a.year=b.year;
quit;

*** decide what to keep in report ***;

proc sql;
create table c_k_&prvdr._&epi. as
select distinct a.def_sub, &var1., &var2., &var3., &var4., sum(a.total_amt)/b.epi_cnt as avg
from c_&prvdr._&epi. a, (select def_sub, count(*) as epi_cnt from bene_&prvdr._&epi. group by def_sub) b
where a.def_sub=b.def_sub
group by a.def_sub, &var1., &var2., &var3., &var4.
having avg>=20
order by def_sub, &var1., &var2., avg desc;
quit;

proc sql;
create table c_b_&prvdr._&epi. as
select distinct a.cbsa, a.def_sub, a.spec_type, a.&cat., a.index_cm, a.age_grp, a.dual, a.index_er, 
	a.&var1., a.&var2., a.&var3., a.&var4., sum(total_amt)/b.epi_cnt/b.avg as pct, count(*)/b.epi_cnt as exp_svc_pct
from c_&prvdr._&epi. a, 
	(select cbsa, def_sub, spec_type, &cat., index_cm, age_grp, dual, index_er, count(*) as epi_cnt, mean(epi_pay) as avg
	from bene_&prvdr._&epi. group by cbsa, def_sub, spec_type, &cat., index_cm, age_grp, dual, index_er) b
where a.cbsa=b.cbsa and a.def_sub=b.def_sub and a.spec_type=b.spec_type 
	and a.&cat.=b.&cat. and a.index_cm=b.index_cm and a.age_grp=b.age_grp and a.dual=b.dual and a.index_er=b.index_er
group by a.cbsa, a.def_sub, a.spec_type, a.&cat., a.index_cm, a.age_grp, a.dual, a.index_er, a.&var1., a.&var2., a.&var3., a.&var4.;
quit;

proc sql;
create table c_kb_&prvdr._&epi. as
select a.*
from c_b_&prvdr._&epi. a, c_k_&prvdr._&epi. b
where a.def_sub=b.def_sub and a.&var1.=b.&var1. and a.&var2.=b.&var2. and a.&var3.=b.&var3. and a.&var4.=b.&var4.;
quit;

proc sql;
create table c_exp_&prvdr._&epi. as
select distinct a.year, a.def_id, a.&prvdr., b.provider_type, b.provider_id, state, cbsa, def_sub, spec_type, &cat., index_cm, age_grp, 
				dual, index_er, n_p_t_epi_pay as epi_exp 
from bene_&prvdr._&epi. a, 	
		(select distinct lvl_1_value, provider_type, provider_id from %if &epi.=PPI_TCC %then out.r_sc_&prvdr._&epi.; %else out.r_sc1_&prvdr._&epi.; 
		where measure="t_epi_pay" and ppi_score_adj^=.) b
where %if &epi.=PPI_TCC %then a.frailty_cat=b.lvl_1_value; %else a.def_sub=b.lvl_1_value; and a.&prvdr.=b.provider_id;
quit;

proc sql;
create table c_c_&prvdr._&epi. as
select &prvdr., cbsa, def_sub, &cat., count(*) as epi_cnt
from bene_&prvdr._&epi. 
group by &prvdr., cbsa, def_sub, &cat.;
quit;

proc sql;
create table c_p_&prvdr._&epi. as
select a.*, b.year, b.def_id, b.&prvdr., b.provider_type, b.state, b.provider_id, b.epi_exp, %if &type.=pc %then c.total_visit,; c.total_amt, d.epi_cnt
from c_kb_&prvdr._&epi. a
inner join c_exp_&prvdr._&epi. b
	on a.cbsa=b.cbsa and a.def_sub=b.def_sub and a.spec_type=b.spec_type 
		and a.&cat.=b.&cat. and a.index_cm=b.index_cm and a.age_grp=b.age_grp and a.dual=b.dual and a.index_er=b.index_er
left join c_&prvdr._&epi. c
	on b.year=c.year and b.def_id=c.def_id and a.cbsa=c.cbsa and a.def_sub=c.def_sub and a.&var1.=c.&var1. and a.&var2.=c.&var2. and a.&var3.=c.&var3. and a.&var4.=c.&var4.
left join c_c_&prvdr._&epi. d
	on b.&prvdr.=d.&prvdr. and a.cbsa=d.cbsa and a.def_sub=d.def_sub and a.&cat.=d.&cat.;
quit;

proc sql;
create table out.r_&type._&prvdr._&epi.(drop=order) as
select distinct &py. as py, state, cbsa, def_sub, provider_type, provider_id, &cat., &var1., &var2., &var3., &var4., 
	epi_cnt, sum(total_amt>=0)/epi_cnt as obs_cnt_pct, round(sum(exp_svc_pct),0.1)/epi_cnt as exp_cnt_pct, %if &type.=pc %then max(0,sum(total_visit)) as visit_cnt,; 
	max(0,sum(total_amt)) as total_cost, max(0,sum(total_amt))/epi_cnt as avg_observed, sum(epi_exp*pct)/epi_cnt as avg_expected, 
	(calculated avg_observed)/(calculated avg_expected)as avg_oe, &var3.="Total" as order
from c_p_&prvdr._&epi. 
group by provider_id, state, cbsa, def_sub, &cat., &var1., &var2., &var3., &var4.
order by provider_id, state, cbsa, def_sub, &cat., &var1., &var2., order desc, &var3., avg_observed desc, avg_expected desc;
quit;

data out.r_&type._&prvdr._&epi.;
set out.r_&type._&prvdr._&epi.;
if &var3.^="Total" and avg_observed<20 and avg_expected<20 then delete;
run;

proc datasets lib=work noprint; delete &type._:; run;
%mend;


%macro export(prvdr);

*** export epi x spec score ***;
data out.export_&prvdr._epi_spec;
length lvl_1_value $50.;
retain py ppi_type provider_type measure cbsa lvl_1_value spec_desc;

set out.r_sc1_&prvdr.: %if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_sc_&prvdr.:(in=t);;
if t then ppi_type="TCC    "; else ppi_type="Episode";

if ppi_score_adj=. then do;
	avg_expected=.;
	avg_oe=.;
	credibility=.;
	avg_oe_adj=.; 
	rank_type="";
end;

if ppi_score_adj=. then ppi_final_score="N/R"; else ppi_final_score=put(ppi_score_adj,3.);

rename 
lvl_1_value=episode_id 
spec_desc=specialty
;

drop spec_type rpt_lvl_1 del_flag;
run;

proc sort data=out.export_&prvdr._epi_spec; 
by py provider_type measure episode_id specialty cbsa ppi_final_score;
run;


*** export setting report ***;
data out.export_&prvdr._setting;
length provider_type $50. measure $100. index_setting $50.;
retain py ppi_type provider_type measure cbsa def_sub index_setting ;

set out.r_s1_&prvdr.:;
ppi_type="Episode";

if index_setting="PHYS" then index_setting="OFFICE";
rename 
def_sub=episode_id 
;
if epi_cnt<11 then epi_cnt=.;
drop spec_type credibility avg_oe_adj;
run;

*** export pc report ***;
data out.export_&prvdr._pc;
retain py ppi_type provider_type cbsa def_sub type;
length index_setting $50.;

set out.r_pc_&prvdr.: %if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_pc_&prvdr.:(rename=(frailty_cat=index_setting));;
if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";

if index_setting="PHYS" then do;
	if index(def_sub, "PPI_PR_") then index_setting="OFFICE";
	else index_setting="N/A";
end;

if epi_cnt<11 then epi_cnt=.;
if visit_cnt<11 then visit_cnt=.;

rename 
def_sub=episode_id 
visit_cnt=pc_visit_cnt
;
run;

*** export cost breakdown report ***;
data out.export_&prvdr._cost_bd;
length index_setting $50.;
retain py ppi_type provider_type cbsa def_sub;

set out.r_cbd_&prvdr.: %if &prvdr. ne attr_f_bill and &prvdr. ne attr_ccn %then tcc.r_cbd_&prvdr.:(rename=(frailty_cat=index_setting));;
if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";

if index(def_sub, "TCC") or index(def_sub, "CH_MED") or index(def_sub, "CHEMO") then phase="N/A";
if index_setting="PHYS" and index(def_sub, "PPI_PR_") then index_setting="OFFICE";
else if index_setting="PHYS" then index_setting="N/A";
if epi_cnt<11 then epi_cnt=.;

rename 
def_sub=episode_id 
svc_cat=service_description
svc_sub=service_details
;
run;
%mend;

%macro export_var;
*** export epi variation report ***;
data out.export_epi_var;
retain py ppi_type;
set out.r_epi_var_: tcc.r_epi_var_:;

if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";
if epi_cnt<11 then epi_cnt=.;

rename def_sub=episode_id;
run;

*** export NPI x TIN report ***;
data out.export_npi_x_tin;
retain py ppi_type;
set out.r_npi_tin_: tcc.r_npi_tin_:;

if def_sub="PPI_TCC" then ppi_type="TCC    "; 
else ppi_type="Episode";
if epi_cnt<11 then epi_cnt=.;

rename def_sub=episode_id;
run;
%mend;
