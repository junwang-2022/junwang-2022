*** ad hoc steps to add discharge and PAC provider information to current Episode X Beneficiary Base Table ****;

/* run these steps in each year separately */

/* step 1: identify post-discharge provider for each type of episode*/
%macro pac_provider(epi);
proc sort data=out.epi_clm_&epi. out=first_pac_&epi.;
where phase="POST" and clm_type in ("IP" "SNF" "HHA" "HSP");
by def_sub def_id clm_type setting from_dt;
run;
proc sort data=first_pac_&epi. nodupkey; by def_sub def_id clm_type setting; run;

data dis_status_&epi.;
set out.epi_clm_&epi.;
where index_claim=1 and dis_status^="";
run;
%mend;
%pac_provider(PPI_IP_PROC);
%pac_provider(PPI_IOP_PROC);
%pac_provider(PPI_IOP_MJRLE);
%pac_provider(PPI_OP_PROC);
%pac_provider(PPI_PR_PROC);
%pac_provider(PPI_IP_MED);

/* step2: combine discharge and PAC files from all episodes*/
data all_first_pac; set first_pac_:; run;
data all_dis_status; set dis_status_:; run;


/* step 3: add discharge and PAC provider information to current Episode X Beneficiary Base Table */
proc sql;
create table epi_bene_base_table as
select distinct a.*, e.dis_status, e.discharge as index_discharge, f.bill_npi as pac_bill_npi, f.ccn as pac_ccn, f.adm_dt format=mmddyy10. as pac_start_dt
from epi_bene_base_table a /* this is your current Episode X Beneficiary Base Table */
left join all_dis_status e on a.def_sub=e.def_sub and a.def_id=e.def_id
left join all_first_pac f on a.def_sub=f.def_sub and a.def_id=f.def_id and e.discharge=f.setting;
quit;





*** ad hoc steps to create complication report by ACO and APM TIN ****;

/* step1: add ACO and APM TIN to the bene base table */

/* step2: roll up complications to ACO and APM TIN level. run each episode separately */
%macro pc_provider(prvdr);
%add_benchmark(pc, index_setting, type, type, description, total_cost);
%mend;

%macro pc_epi(epi);
%pc_provider(aco);
%pc_provider(apm_tin);
%mend;
%pc_epi(PPI_IP_PROC);
%pc_epi(PPI_IOP_PROC);
%pc_epi(PPI_IOP_MJRLE);
%pc_epi(PPI_OP_PROC);
%pc_epi(PPI_PR_PROC);
%pc_epi(PPI_IP_MED);
%pc_epi(PPI_AC_MED);
%pc_epi(PPI_CH_MED);
%pc_epi(PPI_CHEMO);
%pc_epi(PPI_RO);

/* step3: combine complication reports */
data pc_report;
set r_pc_:;
run;


%macro add_benchmark(type, cat, var1, var2, var3, cost);
%if &type.=pc %then %do;
proc sql;
create table &type._&epi. as 
select *
from out.epi_pc_&epi.
union corr 
select epi_type, def_sub, def_desc, def_id, type, "All" as description, 
		sum(visit_cnt) as visit_cnt, sum(total_cost) as total_cost
from out.epi_pc_&epi.
group by epi_type, def_sub, def_desc, def_id, type;
quit;
%end;
%if &type.=cbd %then %do;
proc sql;
create table &type._&epi. as 
select *
from out.cost_breakdown_&epi.
union corr 
select def_sub, def_desc, def_id, "All" as phase, "All" as clm_type, "All" as svc_cat,
		 sum(tot_amt) as tot_amt
from out.cost_breakdown_&epi.
where svc_cat="Total"
group by def_sub, def_desc, def_id;
quit;
%end;

data base_&epi.; /* subset base table to a single episode type */
set epi_bene_base_table;
where index(def_sub, "&epi.")>0;
run;

proc sql;
create table c_&prvdr._&epi. as
select a.*, %if &epi.=PPI_TCC or &epi.=PPI_CH_MED %then a.&cost./b.month*12; %else a.&cost.; as total_amt, 
	%if &type.=pc %then %do; %if &epi.=PPI_TCC or &epi.=PPI_CH_MED %then round(a.visit_cnt/b.month*12,1); %else a.visit_cnt; as total_visit, %end;
	b.&cat., b.cbsa, b.epi_type, b.clin_cat, b.&prvdr., b.spec_type, b.index_cm, b.age_grp, b.dual, b.index_er, b.t_epi_pay
from &type._&epi. a, base_&epi. b
where a.def_sub=b.def_sub and a.def_id=b.def_id;
quit;

*** decide what to keep in report ***;

proc sql;
create table c_k_&prvdr._&epi. as
select distinct a.def_sub, &var1., &var2., &var3., sum(a.total_amt)/b.epi_cnt as avg
from c_&prvdr._&epi. a, (select def_sub, count(*) as epi_cnt from base_&epi. group by def_sub) b
where a.def_sub=b.def_sub
group by a.def_sub, &var1., &var2., &var3.
having avg>=5
order by def_sub, &var1., &var2., avg desc;
quit;

proc sql;
create table c_b_&prvdr._&epi. as
select distinct a.cbsa, a.clin_cat, a.epi_type, a.def_desc, a.def_sub, a.spec_type, a.&cat., a.index_cm, a.age_grp, a.dual, a.index_er, 
	a.&var1., a.&var2., a.&var3., sum(total_amt)/b.epi_cnt/b.avg as pct
from c_&prvdr._&epi. a, 
	(select cbsa, def_sub, spec_type, &cat., index_cm, age_grp, dual, index_er, count(*) as epi_cnt, 
	%if &epi.=PPI_CH_MED %then mean(epi_tot/month*12); %else mean(epi_pay); as avg
	from base_&epi. group by cbsa, def_desc, def_sub, spec_type, &cat., index_cm, age_grp, dual, index_er) b
where a.cbsa=b.cbsa and a.def_sub=b.def_sub and a.spec_type=b.spec_type 
	and a.&cat.=b.&cat. and a.index_cm=b.index_cm and a.age_grp=b.age_grp and a.dual=b.dual and a.index_er=b.index_er
group by a.cbsa, a.def_desc, a.def_sub, a.spec_type, a.&cat., a.index_cm, a.age_grp, a.dual, a.index_er, a.&var1., a.&var2., a.&var3.;
quit;

proc sql;
create table c_kb_&prvdr._&epi. as
select a.*
from c_b_&prvdr._&epi. a, c_k_&prvdr._&epi. b
where a.def_sub=b.def_sub and a.&var1.=b.&var1. and a.&var2.=b.&var2. and a.&var3.=b.&var3.;
quit;

proc sql;
create table c_exp_&prvdr._&epi. as
select distinct a.def_id, a.&prvdr., b.provider_type, b.provider_id, cbsa, def_desc, def_sub, spec_type, &cat., index_cm, age_grp, 
				dual, index_er, n_p_t_epi_pay as epi_exp 
from base_&epi. a, 	
		(select distinct def_sub, &prvdr. as provider_id, "&prvdr." as provider_type from base_&epi. group by def_sub, &prvdr.) b
where %if &epi.=PPI_TCC %then a.frailty_cat=b.lvl_1_value; %else a.def_sub=b.def_sub; and a.&prvdr.=b.provider_id
;
quit;

proc sql;
create table c_c_&prvdr._&epi. as
select &prvdr., cbsa, def_desc, def_sub, &cat., count(*) as epi_cnt
from base_&epi. 
group by &prvdr., cbsa, def_desc, def_sub, &cat.;
quit;

proc sql;
create table c_p_&prvdr._&epi. as
select a.*, b.def_id, b.&prvdr., b.provider_type, b.provider_id, b.epi_exp, %if &type.=pc %then c.total_visit,; c.total_amt, d.epi_cnt
from c_kb_&prvdr._&epi. a
inner join c_exp_&prvdr._&epi. b
	on  a.cbsa=b.cbsa and a.def_sub=b.def_sub and a.spec_type=b.spec_type 
		and a.&cat.=b.&cat. and a.index_cm=b.index_cm and a.age_grp=b.age_grp and a.dual=b.dual and a.index_er=b.index_er
left join c_&prvdr._&epi. c
	on b.def_id=c.def_id and a.cbsa=c.cbsa and a.def_sub=c.def_sub and a.&var1.=c.&var1. and a.&var2.=c.&var2. and a.&var3.=c.&var3.
left join c_c_&prvdr._&epi. d
	on b.&prvdr.=d.&prvdr. and a.cbsa=d.cbsa and a.def_sub=d.def_sub and a.&cat.=d.&cat.;
quit;

proc sql;
create table r_&type._&prvdr._&epi.(drop=order) as
select distinct clin_cat, epi_type, def_desc, def_sub, provider_type, provider_id, &cat., &var1., &var2., &var3.,
	epi_cnt, %if &type.=pc %then max(0,sum(total_visit)) as visit_cnt,; max(0,sum(total_amt)) as total_cost, max(0,sum(total_amt))/epi_cnt as avg_observed, sum(epi_exp*pct)/epi_cnt as avg_expected, 
	(calculated avg_observed)/(calculated avg_expected)as avg_oe, &var3.="Total" as order
from c_p_&prvdr._&epi. 
group by provider_id, def_desc, def_sub, &cat., &var1., &var2., &var3.
order by provider_id, def_desc, def_sub, &cat., &var1., &var2., order desc, avg_observed desc, avg_expected desc;
quit;

proc datasets lib=work noprint; delete &type._:; run;
%mend;

