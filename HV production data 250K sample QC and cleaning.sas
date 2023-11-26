libname in "T:\Inovalon\Sample250K";
libname f "T:\Inovalon\Full";
libname out "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\HV production data 250K sample QC";
/*libname old "D:\SASData\dua_052882\Sndbx\Chris F\inovalon\raw_from_inovalon";*/
/*libname oldout "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\old sample QC";*/
/*libname meta2 "D:\SASData\dua_052882\Users\Jun_Wang\LDS Test Stand\meta2";*/
libname meta "T:\Inovalon\meta";

options mlogic mprint;


***********************************;
*** data range and completeness ***;
***********************************;

%macro qc_dt;
proc sql noprint;
select name into :dt_list separated by ' '
from sashelp.vcolumn
where libname="IN" and memname="%upcase(&table.)" and type="num" and index(format, "YYMM")>0;
quit;

%do i=1 %to &sqlobs.;
%let var=%scan(&dt_list., &i.);

proc sql;
create table dt_&var. as
select distinct "&table." length=30 as table, claim_type, year(&var.) as year, month(&var.) as month, count(*) as &var.
from &table.
group by claim_type, year(&var.), month(&var.)
order by claim_type, year, month;
quit;

%end;

data qc_dt_&table.;
merge dt_:;
by table claim_type year month;
run;
proc datasets lib=work; delete dt_:; run;

%mend qc_dt;


%macro qc_num;

proc sql noprint;
select name into :num_list separated by ' '
from sashelp.vcolumn
where libname="IN" and memname="%upcase(&table.)" and type="num" 
	and index(format, "YYMM")=0 and index(upcase(name), "UID")=0 and index(upcase(name), "NPI")=0 and index(upcase(name), "INDICATOR")=0 and index(upcase(name), "CODE")=0;
quit;

proc univariate data = &table.(keep=claim_type &num_list.) noprint
	outtable=qc_num_&table.(keep=claim_type _var_ _nobs_ _nmiss_ _mean_ 
							_min_ _p1_ _p5_ _p10_ _q1_ _median_ _q3_ _p90_ _p95_ _p99_ _max_ _range_ _qrange_);
by claim_type;
run;

data qc_num_&table.; 
retain table _var_ claim_type _nobs_ _nmiss_ missing_pct;
length table $30. _var_ $30.;
set qc_num_&table.;
table="&table.";
missing_pct=_nmiss_/(_nobs_+_nmiss_);
run;

proc sort data=qc_num_&table.;
by table _var_ claim_type;
run;

%mend qc_num;


%macro qc_char;

proc sql noprint;
select name into :char_list separated by ' '
from sashelp.vcolumn
where libname="IN" and memname="%upcase(&table.)" 
	and (type="char" or index(upcase(name), "UID")>0 or index(upcase(name), "NPI")>0 or index(upcase(name), "INDICATOR")>0 or index(upcase(name), "CODE")>0);
run;

%do i=1 %to &sqlobs.;
%let var=%scan(&char_list., &i.);

proc sql;
create table char_&var. as
select distinct "&table." length=30 as table, claim_type, "&var." length=30 as char, cats(&var.) length=50 as value, &var. is null as missing, count(*) as cnt
from &table.
group by claim_type, &var.
order by claim_type, char, missing desc, cnt desc;
quit;

proc sql;
create table char_&var. as
select *, cnt/sum(cnt) as pct
from char_&var.
group by claim_type
order by claim_type, char, missing desc, cnt desc;
quit;

data char2_&var.;
set char_&var.;
retain n;
by claim_type char descending missing;
if first.missing then n=1;
if n<=20 then output;
n+1;
run;

%end;

data qc_char_&table.;
set char2_:;
drop n;
run;
proc datasets lib=work; delete char_: char2_:; run;

%mend qc_char;



%macro qc_table(table);

data &table.; set in.&table.; 
%if &table.=claim %then %do;
if professionaltypecode="U" then claim_type="P";
if professionaltypecode="" then do;
	if institutionaltypecode="I" then claim_type="I";
	else claim_type="O";
end;
%end;
%else %do;
claim_type="";
%end;
run;

proc sort data=&table.; by claim_type; run;

%qc_num;
%qc_dt;
%qc_char;

%mend qc_table;

%qc_table(enrollment_member);
%qc_table(enrollment_records);
%qc_table(medical_member);
%qc_table(pharmacy_member);

%qc_table(medical_prv);
%qc_table(pharmacy_prv);
%qc_table(medical_psp);
%qc_table(pharmacy_psp);

%qc_table(pharmacy_rxc);
%qc_table(pharmacy_rxcc);
%qc_table(pharmacy_rxcw);

%qc_table(medical_ccd);
%qc_table(medical_clm);
%qc_table(medical_ipps);
%qc_table(medical_nonipps);
%qc_table(medical_xref);


data out.qc_dt; set qc_dt_:; run;
data out.qc_num; set qc_num_:; run;
data out.qc_char; set qc_char_:; run;

proc datasets lib=work nolist memtype=data nodetails; run; quit;

** compare medical and pharmacy provider tables **;
proc sort data=f.medical_prv out=a; by provideruid npi1 createddate; run;
proc sort data=f.pharmacy_prv out=b; by provideruid npi1 createddate; run;
proc compare
 base = a
 compare = b;
run;

proc sort data=f.medical_psp out=c; by provideruid npinumber createddate; run;
proc sort data=f.pharmacy_psp out=d; by provideruid npinumber createddate; run;
proc compare
 base = c
 compare = d;
run;


**************;
*** member ***;
**************;
proc sort data=in.enrollment_member out=member nodupkey; by _all_; run;
proc sort data=member; by memberuid descending createddate; run;
proc sort data=member out=out.member_final nodupkey; by memberuid; run;
proc sort data=member out=zip3; by zip3value; run;
proc sql; 
select sum(zip3value="")/count(*) as zip_missing, sum(length(zip3value)=3)/count(*) as zip_s, sum(length(zip3value)>3)/count(*) as zip_m
from member; 
quit;

************************;
*** memberenrollment ***;
************************;

** bridge enrollment segments **;
proc sort data=in.enrollment_records out=enroll nodupkey; by _all_; run;
proc sort data=enroll; by memberuid effectivedate terminationdate payergroupcode descending createddate; run;
proc sort data=enroll nodupkey; by memberuid effectivedate terminationdate payergroupcode; run;

%macro bridge_enroll(type, level, flag);
proc sort data=enroll; by memberuid &level. effectivedate terminationdate; run;

data bridge_&type._&level.(keep=memberuid &level. start_dt end_dt);
set enroll;
where &flag.=1;
by memberuid &level.;

format start_dt end_dt mmddyy10.;
retain start_dt end_dt;

if first.&level. then do;
	start_dt=effectivedate;
	end_dt=terminationdate;
end;

else do;
	if effectivedate<=end_dt+1 then end_dt=terminationdate;
	else do;
		output;
		start_dt=effectivedate;
		end_dt=terminationdate;
	end;
end;

if last.&level. then output;
run;

** add member demographics **;
proc sql;
create table out.enrollment_&type._&level. as
select a.*, b.*
from bridge_&type._&level. a
left join out.member b
on a.memberuid=b.memberuid
order by memberuid, start_dt, end_dt; 
quit;

%mend;

%bridge_enroll(med, payergroupcode, medicalindicator);
%bridge_enroll(rx, payergroupcode, rxindicator);
%bridge_enroll(med, payertypecode, medicalindicator);
%bridge_enroll(rx, payertypecode, rxindicator);
%bridge_enroll(med, productcode, medicalindicator);
%bridge_enroll(rx, productcode, rxindicator);

proc sql;
select distinct payergroupcode, cnt/sum(cnt) as payer_pct
from
	(select payergroupcode, count(*) as cnt
	from out.enrollment_med_payergroupcode
	group by payergroupcode);
quit;
proc sql;
select distinct sum(cnt>1)/count(*) as m_pct
from
	(select payergroupcode, memberuid, count(*) as cnt
	from out.enrollment_med_payergroupcode
	group by payergroupcode, memberuid);
quit;

***********************;
*** provider tables ***;
***********************;

/*Pharmacy provider tables are identical to Medical provider tables. Process Medical tables only*/

proc sort data=in.medical_prv out=medical_prv; by provideruid descending createddate; run;
proc sort data=medical_prv out=out.medical_prv nodupkey; by provideruid; run;
proc sort data=in.medical_psp out=medical_psp; by provideruid descending createddate; run;
proc sort data=medical_psp out=out.medical_psp nodupkey; by provideruid; run;

data psp;
set out.medical_psp;
length firstname $50. lastname $50. middlename $50.;
if index(name, ",")>0 then do; 
	lastname=scan(name,1,",");
	firstname=scan(name,2,",");
end;
else do; 
	firstname=scan(name,1," ");
	if length(scan(name,2," "))=1 then do;
		middlename=scan(name,2," ");
		lastname=scan(name,3," ");
	end;
	else lastname=scan(name,2," ");
end;

primarypracticeaddress=strip(address1)||" "||strip(address2);

rename 
name=companyname 
state=practicestate 
city=practicecity
zip=practicezip 
;
drop address1 address2 cj_load_file_nm;
run;

/*combine two provider tables */
data provideruid;
set out.medical_prv(rename=(npi1=npi) in=a) psp(rename=(npinumber=npi) in=b);
if a then flag="prv";
if b then flag="psp";
run;
proc sort data=provideruid; by provideruid descending npi flag; run;
proc sort data=provideruid nodupkey; by provideruid; run;

data provideruid_missing; set provideruid; where npi="" and (lastname^="" or companyname^=""); run;

* check NPI with >1 ProviderUID;
proc sql; 
select distinct flag, uid_cnt>1 as multiple_uid, count(*) as npi_cnt
from(select flag, npi, count(distinct provideruid) as uid_cnt
	from provideruid
	where npi^=""
	group by flag, npi)
group by flag, uid_cnt>1
order by flag, multiple_uid; 
quit;


/*use CJ MDM provider tables to mapping additional NPI by name and location */
proc sort data=meta.health_care_practitioner out=ind_npi; where npi_num^=""; by npi_num descending source_last_update_dt; run; /* this is MDM practitioner NPI table */
proc sort data=ind_npi; by npi_num; run;
proc sort data=meta.health_care_provider_org out=org_npi; where npi_num^=""; by npi_num descending source_last_update_dt; run; /* this is MDM organizational NPI table */
proc sort data=org_npi; by npi_num; run;

proc sql;
create table add_npi as 
select a.*, b.npi_num as npi_ind, strip(upcase(a.firstname))=strip(upcase(b.first_nm)) as match_1, upcase(substr(a.middlename,1,1))=upcase(substr(b.middle_nm,1,1)) as match_2, strip(upcase(a.middlename))=strip(upcase(b.middle_nm)) as match_3,
			d.npi_num as npi_org, substr(a.practicezip,1,5)=substr(d.postal_cd,1,5) as match_4, substr(a.practicezip4,1,4)=substr(d.postal_plus_4_cd,1,4) as match_5
from provideruid_missing a
left join ind_npi(where=(last_nm^="")) b 
on a.lastname^="" and strip(upcase(a.lastname))=strip(upcase(b.last_nm)) and a.firstname^="" and upcase(substr(a.firstname,1,1))=upcase(substr(b.first_nm,1,1))
	and a.practicestate=b.state_cd
left join org_npi(where=(org_nm^="")) d 
on a.companyname^="" and strip(upcase(a.companyname))=strip(upcase(d.org_nm)) and a.practicestate=d.state_cd
order by provideruid, match_1 desc, match_2 desc, match_3 desc, match_4 desc, match_5 desc;
quit;

proc sql;
create table add_npi2 as
select *, count(*) as cnt
from add_npi
where npi_ind^="" or npi_org^=""
group by provideruid, match_1, match_2, match_3, match_4, match_5
order by provideruid, match_1 desc, match_2 desc, match_3 desc, match_4 desc, match_5 desc;
quit;
proc sort data=add_npi2 nodupkey; by provideruid; run;

/*create master CJ Inovalon NPI mapping table*/
proc sql;
create table out.provider_mapping as
select a.*, coalescec(a.npi, b.npi_ind, b.npi_org) as npi_update, c.sole_proprietor_ind, 
	case when (c.npi_num is not null) then 1 when (d.npi_num is not null) then 2 end as npi_type
from provideruid a
left join add_npi2(where=(cnt=1)) b on a.provideruid=b.provideruid
left join ind_npi c on coalescec(a.npi, b.npi_ind, b.npi_org)=c.npi_num
left join org_npi d on coalescec(a.npi, b.npi_ind, b.npi_org)=d.npi_num
order by provideruid, npi_update desc;
quit;

proc sql; select sum(npi="")/count(*) as org_npi_missing_pct, sum(npi_update="")/count(*) as update_npi_missing_pct from out.provider_mapping; quit;



********************************;
*** subset commercial claims ***;
********************************;
%macro subset1(tbl, type, from_dt, thru_dt);
proc sql;
create table out.&tbl. as
select distinct a.*
from in.&tbl. a, out.enrollment_&type._payergroupcode(where=(payergroupcode="C")) b
where a.memberuid=b.memberuid and b.start_dt<=a.&from_dt.<=b.end_dt and b.start_dt<=a.&thru_dt.<=b.end_dt
order by memberuid, &from_dt., &thru_dt.;
quit;
%mend;
%macro subset2(tbl, type);
proc sql;
create table out.&tbl. as
select a.*
from in.&tbl. a, (select distinct memberuid from out.enrollment_&type._payergroupcode(where=(payergroupcode="C"))) b
where a.memberuid=b.memberuid;
quit;
%mend;

%subset1(medical_clm, med, servicedate, servicethrudate);
%subset1(medical_ccd, med, servicedate, servicethrudate);
%subset1(medical_ipps, med, admissiondate, dischargedate);
%subset1(medical_nonipps, med, servicedate, servicethrudate);
%subset2(medical_xref, med);

%subset1(pharmacy_rxc, rx, filldate, filldate);
%subset1(pharmacy_rxcc, rx, filldate, filldate);
%subset2(pharmacy_rxcw, rx);


**************************;
*** clean up rx claims ***;
**************************;
proc sort data=out.pharmacy_rxc out=rx_clm nodupkey; by _all_; run;
proc sort data=out.pharmacy_rxcw out=rx_ref nodupkey; by _all_; run;
proc sort data=out.pharmacy_rxcc out=rx_cost nodupkey; by _all_; run;

proc freq data=rx_clm; tables ClaimstatusCode; run;
proc sort data=rx_clm(drop=rxclaimuid) out=a dupout=b nodupkey; by _all_; run;

proc sql; select sum(unadjustedprice=.)/count(*) from rx_cost; quit;
proc sql; select sum(rxfilluid=.)/count(*) from rx_ref; quit;


** add rx proxy cost **;
proc sql;
create table rx_cost as 
select distinct x.*, y.* 
from rx_ref x, rx_cost y 
where x.memberuid=y.memberuid and x.rxfilluid=y.rxfilluid and x.rxfilluid^=.
order by memberuid, rxclaimuid, filldate, ndc11code, supplydayscount;
quit; 

proc sort data=rx_cost nodupkey; by memberuid rxclaimuid filldate ndc11code supplydayscount; run;
proc sort data=rx_clm nodupkey; by memberuid rxclaimuid filldate ndc11code supplydayscount; run;

proc sql;
create table rx_add_cost as
select a.*, b.rxfilluid, b.unitquantity, b.unadjustedprice
from rx_clm a
left join rx_cost b
on a.memberuid=b.memberuid and a.rxclaimuid=b.rxclaimuid and a.filldate=b.filldate and a.ndc11code=b.ndc11code and a.supplydayscount=b.supplydayscount
order by memberuid, filldate, ndc11code, sourcemodifieddate desc, allowedamount desc, unadjustedprice desc;
quit;


** remove canceled-out pairs **;
proc sort data=rx_add_cost(where=(paidamount=. or paidamount>=0)) out=a; by memberuid filldate ndc11code supplydayscount paidamount; run;
data a; set a; by memberuid filldate ndc11code supplydayscount paidamount; retain seqnum; if first.paidamount then seqnum=1; else seqnum+1; run;
data b; set rx_add_cost; where .<paidamount<0; paidamount=-paidamount; run;
proc sort data=b; by memberuid filldate ndc11code supplydayscount paidamount; run;
data b; set b; by memberuid filldate ndc11code supplydayscount paidamount; retain seqnum; if first.paidamount then seqnum=1; else seqnum+1; run;
data rx_cost_clean;
merge a(in=a) b(in=b);
by memberuid filldate ndc11code supplydayscount paidamount seqnum;
if a and not b;
drop seqnum;
run;

proc sort data=rx_cost_clean(drop=rxclaimuid) out=a dupout=b nodupkey; by _all_; run;
proc sort data=rx_cost_clean; by memberuid filldate ndc11code supplydayscount descending sourcemodifieddate descending allowedamount descending unadjustedprice; run;
proc sort data=rx_cost_clean outout=rx_cost_dedup nodupkey; by memberuid filldate ndc11code supplydayscount; run;

*** map missing npi ***;
proc sql;
create table rx_cost_dedup_npi as
select a.*, b.npi_update as mapped_npi
from rx_cost_dedup(where=(claimstatuscode not in ("D" "P"))) a
left join out.provider_mapping(where=(npi_update^="")) b on a.provideruid=b.provideruid
order by memberuid, filldate, ndc11code, supplydayscount;
quit;

proc sql;
create table rx_npi as
select distinct provideruid, prescribingnpi, count(*) as cnt
from rx_cost_dedup_npi 
where provideruid^=. and prescribingnpi^=""
group by provideruid, prescribingnpi 
order by provideruid, cnt desc;
quit;
 
proc sql;
create table rx_cost_dedup_npi2 as
select a.*, coalescec(a.prescribingnpi, a.mapped_npi, b.prescribingnpi) as prescribingnpi_update
from rx_cost_dedup_npi a
left join (select distinct *, count(*) as npi_cnt from rx_npi group by provideruid having npi_cnt=1) b on a.provideruid=b.provideruid
order by ndc11code, supplydayscount, dispensedquantity, filldate, unadjustedprice desc;
quit;

*** map missing price ***;
data out.rx_claim_final;
set rx_cost_dedup_npi2;
by ndc11code supplydayscount dispensedquantity;
retain rate;
if first.dispensedquantity then rate=unadjustedprice;
unadjustedprice_update=unadjustedprice;
if unadjustedprice=. then unadjustedprice_update=rate; 
if unadjustedprice^=. then rate=unadjustedprice_update;
drop rate;
run;

proc sql;
select count(*) as total_cnt, 
sum(prescribingnpi^="")/count(*) as npi_pct,
sum(mapped_npi^="" and prescribingnpi="")/count(*) as add_npi_pct,
sum(prescribingnpi_update^="")/count(*) as total_npi_pct,
sum(mapped_npi^="" and prescribingnpi^="" and mapped_npi^=prescribingnpi)/sum(mapped_npi^="" and prescribingnpi^="") as unmatch_npi_pct,
sum(unadjustedprice>.)/count(*) as price_pct, 
sum(unadjustedprice_update>.)/count(*) as total_price_pct
from out.rx_claim_final;
quit;


*******************************;
*** clean up medical claims ***;
*******************************;

*** transpose claim code ***;
proc sort data=out.medical_ccd out=claim_code nodupkey; by _all_; run;
proc sort data=claim_code; by memberuid claimuid servicedate servicethrudate codetype ordinalposition descending codevalue; run;
proc sort data=claim_code nodupkey; by memberuid claimuid servicedate servicethrudate codetype ordinalposition; run;

%macro transpose(type, name);
proc transpose data=claim_code out=&name. prefix=&name._;
where codetype=&type.;
by memberuid claimuid servicedate servicethrudate;
var codevalue;
id ordinalposition;
run;

proc sql;
select "&name.", sum(&name._1^="")/sum(&name._0^="") 
from &name.;
quit;
%mend;

/*%transpose(3, cpt);*/
%transpose(4, cpt_mod);
/*%transpose(5, hcpcs);*/
%transpose(6, hcpcs_mod);
%transpose(2, apdrg); ** single col, 2 records in sample **;
%transpose(9, msdrg);
%transpose(10, pos);
%transpose(13, bill_type);
%transpose(16, rev);
%transpose(17, dx);
%transpose(18, px);
%transpose(22, adm_dx); ** single col in sample **;
%transpose(23, reason_dx); ** no record in sample **;
%transpose(24, e_code); ** single col in sample **;

*** remove dup claim lines ***;
proc freq data=out.medical_clm; tables Claimstatuscode; run;

proc sort data=out.medical_clm out=claim nodupkey; 
where claimstatuscode not in ("D" "P"); *** remove denied claims ***;
by _all_; 
run;
proc sort data=claim; by memberuid claimuid servicedate servicethrudate descending sourcemodifieddate; run;
proc sort data=claim nodupkey; by memberuid claimuid servicedate servicethrudate; run;

proc sql;
create table out.claim_flat as
select a.*, coalescec(b.cpt_0, d.hcpcs_0) as cpt, coalescec(c.cpt_mod_0, e.hcpcs_mod_0) as cpt_mod_1, coalescec(c.cpt_mod_1, e.hcpcs_mod_1) as cpt_mod_2, 
	coalescec(c.cpt_mod_2, e.hcpcs_mod_2) as cpt_mod_3, c.cpt_mod_3 as cpt_mod_4,
	f.msdrg_0 as ms_drg, g.pos_0 as pos, h.bill_type_0 as bill_type, i.rev_0 as rev_cd, j.adm_dx_0 as adm_dx_cd, j1.e_code_0 as e_code, j2.*, k.*
from claim a
left join cpt b 		on a.memberuid=b.memberuid and a.claimuid=b.claimuid and a.servicedate=b.servicedate and a.servicethrudate=b.servicethrudate
left join cpt_mod c 	on a.memberuid=c.memberuid and a.claimuid=c.claimuid and a.servicedate=c.servicedate and a.servicethrudate=c.servicethrudate
left join hcpcs d 		on a.memberuid=d.memberuid and a.claimuid=d.claimuid and a.servicedate=d.servicedate and a.servicethrudate=d.servicethrudate
left join hcpcs_mod e 	on a.memberuid=e.memberuid and a.claimuid=e.claimuid and a.servicedate=e.servicedate and a.servicethrudate=e.servicethrudate
left join msdrg f 		on a.memberuid=f.memberuid and a.claimuid=f.claimuid and a.servicedate=f.servicedate and a.servicethrudate=f.servicethrudate
left join pos g			on a.memberuid=g.memberuid and a.claimuid=g.claimuid and a.servicedate=g.servicedate and a.servicethrudate=g.servicethrudate
left join bill_type h 	on a.memberuid=h.memberuid and a.claimuid=h.claimuid and a.servicedate=h.servicedate and a.servicethrudate=h.servicethrudate
left join rev i			on a.memberuid=i.memberuid and a.claimuid=i.claimuid and a.servicedate=i.servicedate and a.servicethrudate=i.servicethrudate
left join adm_dx j 		on a.memberuid=j.memberuid and a.claimuid=j.claimuid and a.servicedate=j.servicedate and a.servicethrudate=j.servicethrudate
left join e_code j1 	on a.memberuid=j1.memberuid and a.claimuid=j1.claimuid and a.servicedate=j1.servicedate and a.servicethrudate=j1.servicethrudate
left join dx j2  		on a.memberuid=j2.memberuid and a.claimuid=j2.claimuid and a.servicedate=j2.servicedate and a.servicethrudate=j2.servicethrudate
left join px k 			on a.memberuid=k.memberuid and a.claimuid=k.claimuid and a.servicedate=k.servicedate and a.servicethrudate=k.servicethrudate
order by memberuid, servicedate, servicethrudate, claimuid;
quit;

*** map provider NPI/proxy price ***;
proc sort data=out.medical_nonipps out=nonipps; by memberuid claimuid provideruid servicedate servicethrudate descending lobadjustedprice; run;
proc sort data=nonipps nodupkey; by memberuid claimuid provideruid servicedate servicethrudate; run;
proc sort data=out.medical_ipps out=ipps; by memberuid dischargeclaimuid provideruid admissiondate dischargedate descending lobadjustedprice; run;
proc sort data=ipps nodupkey; by memberuid dischargeclaimuid provideruid admissiondate dischargedate; run;
proc sort data=out.medical_xref out=ipps_xref nodupkey; by memberuid dischargeuid claimuid; run;

proc sql;
create table out.claim_flat2 as
select distinct a.*, x.dischargeuid, e1.admissiondate, e1.dischargedate, e1.finaldrg, coalesce(e1.unadjustedprice, e2.unadjustedprice) as price, coalesce(e1.lobadjustedprice, e2.lobadjustedprice) as adj_price,
b1.npi_update as npi, 
coalesce(a.renderingprovidernpi, b2.npi_update) as prf_npi, 
coalesce(a.billingprovidernpi, b3.npi_update) as bill_npi,
x.dischargeuid^="" as ipps, e2.claimuid^="" as non_ipps, e2.casetype
from out.claim_flat a
left join ipps_xref x 	on a.memberuid=x.memberuid and a.claimuid=x.claimuid
left join ipps e1 		on x.memberuid=e1.memberuid and x.dischargeuid=e1.dischargeclaimuid and a.provideruid=e1.provideruid
left join nonipps e2 	on a.memberuid=e2.memberuid and a.claimuid=e2.claimuid and a.servicedate=e2.servicedate and a.servicethrudate=e2.servicethrudate and a.provideruid=e2.provideruid
left join out.provider_mapping b1 on coalesce(a.provideruid, e1.provideruid, e2.provideruid)=b1.provideruid
left join out.provider_mapping b2 on a.renderingprovideruid=b2.provideruid
left join out.provider_mapping b3 on a.billingprovideruid=b3.provideruid
order by memberuid, servicedate, servicethrudate, claimuid, adj_price desc;
quit;

data claim_flat3; 
set out.claim_flat2; 

if institutionaltypecode="" then claim_type="P";
else do;
	if ipps=1 then claim_type="I";
	if non_ipps=1 then claim_type="O";
	if ipps=0 and non_ipps=0 then do;
		if ms_drg^="" then claim_type="I";
		else if servicethrudate-servicedate>0 and (pos="21" or substr(bill_type, 1,2)="11") then claim_type="I";
		else claim_type="O";
	end;
end;
run;

proc freq data=claim_flat3; tables claim_type; run;

*** remove dup lines ***;
proc sort data=claim_flat3(drop=claimuid) out=a dupout=b nodupkey; by _all_; run;
proc sort data=claim_flat3; 
by memberuid servicedate servicethrudate claimstatuscode institutionaltypecode claim_type dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price descending sourcemodifieddate;
run;
proc sort data=claim_flat3 out=claim_flat4 dupout=claim_flat3_dup nodupkey; 
by memberuid servicedate servicethrudate claimstatuscode institutionaltypecode claim_type dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price;
run;

proc sort data=claim_flat4; 
by memberuid servicedate claimstatuscode institutionaltypecode claim_type dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price descending servicethrudate;
run;
proc sort data=claim_flat4 out=claim_flat5 dupout=claim_flat4_dup nodupkey; 
by memberuid servicedate claimstatuscode institutionaltypecode claim_type dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price;
run;

proc sort data=claim_flat5; 
by memberuid servicethrudate claimstatuscode institutionaltypecode claim_type dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price servicedate;
run;
proc sort data=claim_flat5 out=claim_flat6 dupout=claim_flat5_dup nodupkey; 
by memberuid servicethrudate claimstatuscode institutionaltypecode claim_type dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price;
run;

** remove reversed claim pairs **;
proc sort data=claim_flat6(where=(claimstatuscode^="R")) out=a; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price dx_0; run;
proc sort data=claim_flat6(where=(claimstatuscode="R"))  out=b; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price; run;
data a; set a; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price; retain seqnum; if first.price then seqnum=1; else seqnum+1; run;
data b; set b; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price; retain seqnum; if first.price then seqnum=1; else seqnum+1; run;
data out.claim_flat6;
merge a(in=a) b(in=b);
by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price seqnum;
if a and not b;
drop seqnum;
run;


*****************************;
*** create header medical ***;
*****************************;

proc sql;
create table out.clm_id as
select *, min(coalescec(dischargeuid, claimuid)) as clm_id
from out.claim_flat6
group by memberuid, servicedate, servicethrudate, claimstatuscode, sourcemodifieddate, claim_type, ipps, non_ipps, dischargeuid, bill_type, bill_npi, prf_npi, npi, provideruid, pos, dx_0, px_0, ms_drg, finaldrg
order by memberuid, servicedate, servicethrudate, clm_id;
quit;

*** clean IP claims ***;

data claim_ip_line;
set out.clm_id; 
where claim_type="I";
if admissiondate=. then admissiondate=servicedate;
if dischargedate=. then dischargedate=servicethrudate;
run;

proc sort data=claim_ip_line; 
by memberuid clm_id descending adj_price descending bill_type descending allowedamount dischargeuid;
run;
proc sort data=claim_ip_line out=claim_ip nodupkey; 
by memberuid clm_id;
run;


*** clean overlapped IP claims ***;
proc sort data=claim_ip; by memberuid provideruid admissiondate descending dischargedate descending adj_price; run;
data stay_ip;
set claim_ip;
by memberuid provideruid;
retain st_dt end_dt stay_id;
if first.provideruid then do; st_dt=admissiondate; end_dt=dischargedate; stay_id=clm_id; end;
if dischargedate>end_dt then do; st_dt=admissiondate; end_dt=dischargedate; stay_id=clm_id; end;
drop st_dt end_dt;
run;
proc sort data=stay_ip out=stay_ip_dedup dupout=stay_ip_dup nodupkey; by memberuid stay_id; run;

*** connect continuous IP stays ***;
proc sort data=stay_ip_dedup; by memberuid provideruid admissiondate dischargedate; run;
data stay_ip2;
set stay_ip_dedup;
by memberuid provideruid;
retain st_dt end_dt stay_id2 flag;
format st_dt end_dt mmddyy10.;

if first.provideruid then do; 
		st_dt=admissiondate; end_dt=dischargedate; stay_id2=stay_id; flag=.;
	if ubpatientdischargestatuscode='30' then flag=1;
end;
else do;
	if flag=1 and (admissiondate=end_dt+1 or admissiondate=end_dt) then do; 
		end_dt=dischargedate;
	end;
	else do; 
		st_dt=admissiondate; end_dt=dischargedate; stay_id2=stay_id; flag=.;
	end;
	if ubpatientdischargestatuscode='30' then flag=1;
end;
run;
proc sort data=stay_ip2; by memberuid stay_id2 descending end_dt; run;
proc sort data=stay_ip2 out=stay_ip_dedup2 dupout=stay_ip_dup2 nodupkey; by memberuid stay_id2; run;

/*create IP ID xwalk table for future pulling complete line level information for child tables */
proc sql;
create table out.ip_id_xwalk as
select a.memberuid, a.claimuid, a.clm_id, b.stay_id, c.stay_id2
from claim_ip_line a
left join stay_ip b on a.clm_id=b.clm_id
left join stay_ip2 c on b.stay_id=c.stay_id
order by a.memberuid, claimuid, stay_id, stay_id2;
quit;


*** clean non-IP claims ***;

data claim_op_line;
set out.clm_id; 
where claim_type^="I";
run;

proc sql;
create table claim_op as
select distinct memberuid, clm_id, servicedate, servicethrudate, claimstatuscode, ipps, non_ipps, claim_type, bill_type, provideruid, bill_npi, prf_npi, npi, dx_0, dx_1, px_0, pos,
	max(ms_drg) as ms_drg, sum(paidamount) as paid_amt, sum(copayamount) as copay_amt, sum(allowedamount) as allowed_amt, sum(price) as price, sum(adj_price) as adj_price,
	max(sourcemodifieddate) as sourcemodifieddate, sum(casetype="LAB")>0 as lab_flag, max(rxproviderindicator) as rxproviderindicator, max(pcpproviderindicator) as pcpproviderindicator
from claim_op_line
group by memberuid, clm_id
order by memberuid, servicedate, servicethrudate, provideruid;
quit;

data out.medical;
set stay_ip_dedup2(in=a) claim_op(in=b);
if a then do;
	admissiondate=st_dt;
	dischargedate=end_dt;
	clm_id=stay_id2;
end;
keep memberuid servicedate servicethrudate claimstatuscode clm_id claim_type bill_type admissiondate dischargedate UBPatientDischargeStatusCode
	bill_npi prf_npi npi provideruid pos adm_dx_cd dx_0 px_0 ms_drg finaldrg paid_amt copay_amt allowed_amt price adj_price sourcemodifieddate lab_flag ipps non_ipps 
	rxproviderindicator pcpproviderindicator;
run;

data out.medical2;
set out.medical;
freq_cd=substr(bill_type,3,1);
billtype=substr(bill_type,1,2);
length clm_type $4;

if claim_type="I" then do;
	clm_type="IP";
	if billtype in ("18" "21") then clm_type="SNF";
	if billtype in ("81" "82") then clm_type="HSP";
end;
if claim_type="O" then do;
	clm_type="OP";
	if billtype in ("32" "33") then clm_type="HHA";
end;
if claim_type="P" then do;
	clm_type="PHYS";
end;

array m(5) price adj_price paid_amt copay_amt allowed_amt;
	do i=1 to 5;
	m(i)=round(m(i),0.01);
	end;
drop i ;
if dx_0="" and price=. then delete; /* delete claims with both dx and price(cpt) missing */ 
run;

proc sql; select sum(dx_0="")/count(*) as dx_missing_pct from out.medical2; quit;


proc sql; select distinct name into :col separated by " "
from sashelp.vcolumn 
where libname="OUT" and memname="MEDICAL2" and upcase(name) not in ("CLM_ID" "SOURCEMODIFIEDDATE" "CLAIMSTATUSCODE" "DX_0" "PRF_NPI" "BILL_NPI" "BILL_TYPE" "BILLTYPE" "FREQ_CD"); quit;
proc sort data=out.medical2; by memberuid servicedate servicethrudate descending dx_0 descending sourcemodifieddate descending npi; run;
proc sort data=out.medical2 out=medical3 nodupkey; by &col.; run;

proc sql; select sum(dx_0="")/count(*) as dx_missing_pct from medical3; quit;


*** fix missing dx ***;
proc sql;
create table missing_dx as
select distinct a.*, coalescec(b.clm_id, a.clm_id) as clm_id_update, c.dx_0 as mapped_dx_0
from medical3 a
left join medical3(where=(dx_0^="")) b on a.dx_0="" and a.memberuid=b.memberuid and a.clm_type=b.clm_type and a.servicedate=b.servicedate and a.servicethrudate=b.servicethrudate
	and (a.provideruid=b.provideruid or a.provideruid=.)
left join medical3(where=(dx_0^="")) c on a.dx_0="" and a.lab_flag=1 and a.memberuid=c.memberuid and a.servicedate=c.servicedate and a.servicethrudate=c.servicethrudate
order by memberuid, servicedate, servicethrudate, provideruid;
quit;
proc sort data=missing_dx nodupkey; by memberuid clm_id; run;
proc sort data=missing_dx; by memberuid servicedate servicethrudate provideruid; run;

proc sql;
create table out.medical3 as
select a.*, b.*, coalescec(dx_0, mapped_dx_0) as dx_0_update
from missing_dx(where=(clm_id=clm_id_update)) a
left join (select memberuid, clm_id_update, sum(price) as price_update, sum(adj_price) as adj_price_update, 
				sum(paid_amt) as paid_amt_update, sum(copay_amt) as copay_amt_update, sum(allowed_amt) as allowed_amt_update
			from missing_dx group by memberuid, clm_id_update) b
on a.memberuid=b.memberuid and a.clm_id_update=b.clm_id_update
order by memberuid, servicedate, servicethrudate, provideruid;
quit;

proc sql;
select distinct clm_type, sum(dx_0="")/count(*) as dx_missing, sum(dx_0_update="")/count(*) as final_dx_missing
from out.medical3
group by clm_type;
quit;
proc sql;
select distinct sum(dx_0="")/count(*) as dx_missing, sum(dx_0_date="")/count(*) as final_dx_missing
from out.medical3;
quit;


***************************;
*** map additional NPIs ***;
***************************;

proc sql;
create table npi_dup as 
select npi_update, count(*) as cnt
from out.provider_mapping
where npi_update^=""
group by npi_update
having cnt>1;
quit;

proc sql;
create table provider_mapping as
select distinct npi_update, npi_type, sole_proprietor_ind
from out.provider_mapping
where npi_update^="";
run;
proc sql;
create table med_npi as
select a.*, b.sole_proprietor_ind as sole_npi, b.npi_type as npi_entity, 
			c.sole_proprietor_ind as sole_bill, c.npi_type as bill_entity, 
			d.sole_proprietor_ind as sole_prf, d.npi_type as prf_entity
from out.medical3 a
left join provider_mapping b on a.npi=b.npi_update
left join provider_mapping c on a.bill_npi=c.npi_update
left join provider_mapping d on a.prf_npi=d.npi_update
order by memberuid, servicethrudate, provideruid;
quit;

data med_npi_update;
set med_npi;
if npi^="" then do;
	if bill_npi="" then do;
		if sole_npi="Y" or npi_entity=2 then bill_npi_mapped=npi;
	end;
	if prf_npi="" then do;
		if npi_entity=1 then prf_npi_mapped=npi;
	end;
end;

if bill_npi^="" and prf_npi="" and sole_bill="Y" then prf_npi=bill_npi;
if prf_npi^="" and bill_npi="" and sole_prf="Y" then bill_npi=prf_npi;

bill_npi_update=coalescec(bill_npi, bill_npi_mapped);
prf_npi_update=coalescec(prf_npi, prf_npi_mapped);
run;

proc sql;
create table prf_to_bill_mapping as
select distinct prf_npi_update, bill_npi_update, clm_type, year(servicethrudate) as year, count(*) as cnt
from med_npi_update
where prf_npi_update^="" and bill_npi_update^=""
group by prf_npi_update, bill_npi_update, clm_type, year(servicethrudate)
order by prf_npi_update, clm_type, year, cnt desc;
quit;
proc sort data=prf_to_bill_mapping nodupkey; by prf_npi_update clm_type year; run;

proc sql;
create table bill_to_prf_mapping as
select distinct bill_npi_update, clm_type, year(servicethrudate) as year, prf_npi_update, count(distinct prf_npi_update) as cnt
from med_npi_update
where prf_npi_update^="" and bill_npi_update^=""
group by bill_npi_update, clm_type, year(servicethrudate)
having cnt=1
order by bill_npi_update, clm_type, year;
quit;

proc sql;
create table provideruid_to_npi_mapping as
select distinct provideruid, bill_npi, prf_npi, count(distinct bill_npi||prf_npi) as cnt
from med_npi_update
where provideruid^=. and npi="" and (bill_npi^="" or prf_npi^="")
group by provideruid
having cnt=1
order by provideruid, bill_npi, prf_npi;
quit;

proc sql;
create table med_npi_update2 as
select a.*, coalescec(a.bill_npi_update, b.bill_npi_update, d.bill_npi) as bill_npi_update2, coalescec(a.prf_npi_update, c.prf_npi_update, d.prf_npi) as prf_npi_update2
from med_npi_update a
left join prf_to_bill_mapping b on a.prf_npi_update^="" and a.bill_npi_update="" and a.prf_npi_update=b.prf_npi_update and a.clm_type=b.clm_type and year(a.servicethrudate)=b.year
left join bill_to_prf_mapping c on a.prf_npi_update="" and a.bill_npi_update^="" and a.bill_npi_update=c.bill_npi_update and a.clm_type=c.clm_type and year(a.servicethrudate)=c.year
left join provideruid_to_npi_mapping d on a.provideruid=d.provideruid
order by memberuid, servicedate, servicethrudate, provideruid;
quit;

proc sql;
create table out.medical_final as
select *
from med_npi_update2 where clm_id_update not in 
(select distinct b.clm_id_update
	from med_npi_update2(where=(bill_npi_update2^="" or prf_npi_update^="")) a,
		 med_npi_update2(where=(bill_npi_update2="" and prf_npi_update="")) b
	where a.memberuid=b.memberuid and a.clm_type=b.clm_type and a.servicedate=b.servicedate and a.servicethrudate=b.servicethrudate and a.dx_0=b.dx_0 and a.price=b.price)
order by memberuid, servicedate, servicethrudate, provideruid;
quit;


*** Check NPI and price missing in the final data ***;
proc sql;
select distinct clm_type, count(*) as clm_cnt, 
	sum(bill_npi^="")/count(*) as bill_npi, sum(bill_npi_update^="")/count(*) as bill_npi_update, sum(bill_npi_update2^="")/count(*) as bill_npi_update2,
	sum(prf_npi^="")/count(*) as prf_npi, sum(prf_npi_update^="")/count(*) as prf_npi_update, sum(prf_npi_update2^="")/count(*) as prf_npi_update2, 
	sum(bill_npi_update2^="" and prf_npi_update2^="")/count(*) as with_both_npi_update2,
	sum(adj_price>.)/count(*) as price_pct, 
	sum(ms_drg^="")/count(*) as msdrg_pct, sum(finaldrg^="")/count(*) as finaldrg_pct, sum(ms_drg^="" and ms_drg=finaldrg)/sum(ms_drg^="" and finaldrg^="") as drg_match
from out.medical_final
group by clm_type;
quit;

proc sql;
create table medical_final as
select *, round(sum(bill_npi_update2="" and prf_npi_update2="")/count(*),0.01) as npi_missing_pct, round(sum(adj_price=.)/count(*),0.01) as price_missing_pct
from out.medical_final 
group by memberuid;
quit;

data medical_final;
set medical_final;
length npi_missing_grp price_missing_grp $20.;

if npi_missing_pct=0 then npi_missing_grp="1.No missing";
else if npi_missing_pct<0.2 then npi_missing_grp="2.1-20%";
else if npi_missing_pct<0.5 then npi_missing_grp="3.20-50%";
else if npi_missing_pct<0.8 then npi_missing_grp="4.50-80%";
else if npi_missing_pct<1 then npi_missing_grp="5.80-99%";
else if npi_missing_pct=1 then npi_missing_grp="6.100% missing";

if price_missing_pct=0 then price_missing_grp="1.No missing";
else if price_missing_pct<0.2 then price_missing_grp="2.1-20%";
else if price_missing_pct<0.5 then price_missing_grp="3.20-50%";
else if price_missing_pct<0.8 then price_missing_grp="4.50-80%";
else if price_missing_pct<1 then price_missing_grp="5.80-99%";
else if price_missing_pct=1 then price_missing_grp="6.100% missing";
run;

proc sql;
select npi_missing_grp, bene_cnt, bene_cnt/sum(bene_cnt) as bene_pct, bill_npi_update2, prf_npi_update2, with_both_npi_update2 from 
	(select distinct npi_missing_grp, count(distinct memberuid) as bene_cnt, count(*) as clm_cnt, 
		sum(bill_npi_update2^="")/count(*) as bill_npi_update2,
		sum(prf_npi_update2^="")/count(*) as prf_npi_update2,
		sum(bill_npi_update2^="" and prf_npi_update2^="")/count(*) as with_both_npi_update2
	from medical_final
	group by npi_missing_grp)
order by npi_missing_grp;
quit;
proc sql;
select price_missing_grp, bene_cnt, bene_cnt/sum(bene_cnt) as bene_pct, price_pct from 
	(select distinct price_missing_grp, count(distinct memberuid) as bene_cnt, count(*) as clm_cnt, 
		sum(adj_price^=.)/count(*) as price_pct
	from medical_final
	group by price_missing_grp)
order by price_missing_grp;
quit;



*** map additional price ***;




***************************;
*** create child tables ***;
***************************;

proc sql;
create table clm_id_list as 
select distinct x.memberuid, x.clm_type, x.clm_id_update, y.clm_id as clm_id_ip
from out.medical_final x
left join out.ip_id_xwalk y 
on x.memberuid=y.memberuid and x.clm_id_update=y.stay_id2;
quit;

proc sql;
create table all_line as
select distinct a.*, b.clm_type, b.clm_id_update
from out.clm_id a, clm_id_list b
where a.memberuid=b.memberuid and a.clm_id=coalescec(clm_id_ip, b.clm_id_update);
quit;

*** create serviceline table ***;
data serviceline;
set all_line;
keep memberuid clm_type clm_id clm_id_update servicedate servicethrudate cpt cpt_mod_1-cpt_mod_4 rev_cd pos 
	paidamount copayamount allowedamount price adj_price sourcemodifieddate majorsurgeryindicator roomboardindicator serviceunitquantity;
run;
proc sort data=serviceline; by memberuid clm_type clm_id_update clm_id descending price; run;
data out.serviceline_final;
set serviceline;
by memberuid clm_type clm_id_update;
retain line_num;
if first.clm_id then line_num=1;
else line_num+1;
run;

*** create dx/px table ***;
%macro dx_px(type);

proc sort data=all_line(keep=memberuid clm_type clm_id clm_id_update claimuid &type._:) out=&type.; by memberuid clm_type clm_id_update clm_id claimuid; run;
proc transpose data=&type. out=&type.2;
by memberuid clm_type clm_id_update clm_id claimuid;
var &type._:;
run;

data &type.3;
set out.medical3(keep=memberuid clm_type clm_id_update &type._0 rename=(&type._0=&type._cd) in=a) &type.2(where=(&type._cd^="") rename=(col1=&type._cd) in=b);
if a then prim_&type.=1; else prim_&type.=0;
if &type._cd^="";
keep memberuid clm_type clm_id_update &type._cd prim_&type.; 
run;
proc sort data=&type.3; by memberuid clm_type clm_id_update &type._cd descending prim_&type.; run;
proc sort data=&type.3 nodupkey; by memberuid clm_type clm_id_update &type._cd; run;
proc sort data=&type.3; by memberuid clm_type clm_id_update descending prim_&type.; run;

data out.&type._final;
set &type.3;
by memberuid clm_type clm_id_update;
retain seq_num;
if first.clm_id_update then seq_num=1;
else seq_num+1;
keep memberuid clm_type clm_id_update &type._cd seq_num;
run;

%mend;

%dx_px(dx);
%dx_px(px);





