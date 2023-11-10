libname in "T:\Inovalon\Sample250K";
libname f "T:\Inovalon\Full";
libname out "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\250K sample QC";
libname old "D:\SASData\dua_052882\Sndbx\Chris F\inovalon\raw_from_inovalon";
libname oldout "D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\old sample QC";
libname meta2 "D:\SASData\dua_052882\Users\Jun_Wang\LDS Test Stand\meta2";
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


data qc.qc_dt; set qc_dt_:; run;
data qc.qc_num; set qc_num_:; run;
data qc.qc_char; set qc_char_:; run;


proc sort data=in.enrollment_member out=enrollment_member; 	by memberuid descending createddate; run;
proc sort data=in.medical_member 	out=medical_member; 	by memberuid descending createddate; run;
proc sort data=in.pharmacy_member 	out=pharmacy_member; 	by memberuid descending createddate; run;

proc sort data=enrollment_member(drop=createddate) out=a nodupkey; 	by _all_; run;
proc sort data=a out=b1 dupout=b2 nodupkey; 	by memberuid; run;

** compare medical and pharmacy provider tables **;
proc sort data=f.medical_prv out=a; by provideruid npi1 createddate; run;
proc sort data=f.pharmacy_prv out=b; by provideruid npi1 createddate; run;
proc compare
 base = a
 compare = b;
/* var provideruid npi1;*/
run;

proc sort data=f.medical_psp out=c; by provideruid npinumber createddate; run;
proc sort data=f.pharmacy_psp out=d; by provideruid npinumber createddate; run;
proc compare
 base = c
 compare = d;
/* var provideruid npi1;*/
run;


**************;
*** member ***;
**************;
proc sort data=in.enrollment_member out=member nodupkey; by _all_; run;
proc sort data=member; by memberuid descending createddate; run;
proc sort data=member out=out.member nodupkey; by memberuid; run;
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
left join member b
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
select distinct sum(cnt>1)/count(*) as m_pct
from
	(select memberuid, count(*) as cnt
	from out.enrollment_med_payergroupcode
	group by memberuid);
quit;

***********************;
*** provider tables ***;
***********************;

/*Pharmacy provider tables are identical to Medical provider tables. Process Medical tables only*/
/*proc sort data=in.pharmacy_prv out=rx_prvdr1; by provideruid descending createddate; run;*/
/*proc sort data=rx_prvdr1 out=out.pharmacy_prv nodupkey; by provideruid; run;*/
/*proc sort data=in.pharmacy_psp(where=(npinumber^="")) out=rx_prvdr2; by provideruid descending createddate; run;*/
/*proc sort data=rx_prvdr2 out=out.pharmacy_psp nodupkey; by provideruid; run;*/

proc sort data=f.medical_prv out=medical_prv; by provideruid descending createddate; run;
proc sort data=medical_prv out=out.medical_prv nodupkey; by provideruid; run;
proc sort data=f.medical_psp out=medical_psp; by provideruid descending createddate; run;
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
drop address1 address2;
run;

data provideruid;
set out.medical_prv(rename=(npi1=npi) in=a) psp(rename=(npinumber=npi) in=b);
if a then flag="prv";
if b then flag="psp";
run;
proc sort data=provideruid; by provideruid descending npi flag; run;
proc sort data=provideruid nodupkey; by provideruid; run;

data provideruid_missing; set provideruid; where npi="" and (lastname^="" or companyname^=""); run;

/*proc sql;*/
/*create table add_npi as */
/*select a.*, b.npi as npi_ind, upcase(a.firstname)=upcase(b.first_name) as match_1, upcase(substr(a.middlename,1,1))=upcase(substr(b.middle_name,1,1)) as match_2, strip(upcase(a.middlename))=strip(upcase(b.middle_name)) as match_3,*/
/*			d.npi as npi_org, substr(a.practicezip,1,5)=substr(d.prvd_bz_addresszip,1,5) as match_4, a.practicecity=d.prvd_bz_addresscity as match_5, strip(upcase(a.primarypracticeaddress))=strip(upcase(d.prvd_bz_address1)) as match_6*/
/*from provideruid_missing a*/
/*left join meta2.npi_spec_x_new(where=(last_name^="")) b */
/*on a.lastname^="" and strip(upcase(a.lastname))=strip(upcase(b.last_name)) and a.firstname^="" and upcase(substr(a.firstname,1,1))=upcase(substr(b.first_name,1,1))*/
/*	and a.practicestate=b.prvd_bz_addressstate*/
/*left join meta2.npi_spec_x_new(where=(providerorganization^="")) d */
/*on a.companyname^="" and strip(upcase(a.companyname))=strip(upcase(d.providerorganization)) and a.practicestate=d.prvd_bz_addressstate*/
/*order by provideruid, match_1 desc, match_2 desc, match_3 desc, match_4 desc, match_5 desc, match_6 desc;*/
/*quit;*/
proc sort data=meta.health_care_practitioner out=ind_npi; where npi_num^=""; by npi_num descending source_last_update_dt; run;
proc sort data=ind_npi; by npi_num; run;
proc sort data=meta.health_care_provider_org out=org_npi; where npi_num^=""; by npi_num descending source_last_update_dt; run;
proc sort data=org_npi; by npi_num; run;

proc sql;
create table add_npi as 
select a.*, b.npi as npi_ind, strip(upcase(a.firstname))=strip(upcase(b.first_nm)) as match_1, upcase(substr(a.middlename,1,1))=upcase(substr(b.middle_name,1,1)) as match_2, strip(upcase(a.middlename))=strip(upcase(b.middle_name)) as match_3,
			d.npi as npi_org, substr(a.practicezip,1,5)=substr(d.prvd_bz_addresszip,1,5) as match_4, a.practicecity=d.prvd_bz_addresscity as match_5, strip(upcase(a.primarypracticeaddress))=strip(upcase(d.prvd_bz_address1)) as match_6
from provideruid_missing a
left join ind_npi(where=(last_nm^="")) b 
on a.lastname^="" and strip(upcase(a.lastname))=strip(upcase(b.last_name)) and a.firstname^="" and upcase(substr(a.firstname,1,1))=upcase(substr(b.first_name,1,1))
	and a.practicestate=b.prvd_bz_addressstate
left join meta2.npi_spec_x_new(where=(providerorganization^="")) d 
on a.companyname^="" and strip(upcase(a.companyname))=strip(upcase(d.providerorganization)) and a.practicestate=d.prvd_bz_addressstate
order by provideruid, match_1 desc, match_2 desc, match_3 desc, match_4 desc, match_5 desc, match_6 desc;
quit;

proc sql;
create table add_npi2 as
select *, count(*) as cnt
from add_npi
where npi_ind^="" or npi_org^=""
group by provideruid, match_1, match_2, match_3, match_4, match_5, match_6
order by provideruid, match_1 desc, match_2 desc, match_3 desc, match_4 desc, match_5 desc, match_6 desc;
quit;
proc sort data=add_npi2 nodupkey; by provideruid; run;

proc sql;
create table out.provider_mapping as
select a.*, coalescec(a.npi, b.npi_ind, b.npi_org) as npi_update
from provideruid a
left join add_npi2(where=(cnt=1)) b on a.provideruid=b.provideruid
order by provideruid, npi_update desc;
quit;
/*proc sort data=out.provider_mapping nodupkey; by provideruid; run;*/
proc sql; select sum(npi_update="")/count(*) as npi_missing_pct from out.provider_mapping; quit;

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

***************************;
*** clean up rx claims ***;
***************************;
proc sort data=out.pharmacy_rxc out=rx_clm nodupkey; by _all_; run;
/*proc sort data=rx_clm(drop=createddate) out=a1 dupout=b1 nodupkey; by _all_; run;*/
/*proc sort data=rx_clm; by memberuid filldate ndc11code sourcemodifieddate; run;*/
/*proc sort data=rx_clm(drop=rxclaimuid) out=a dupout=b nodupkey; by _all_; run;*/
/*proc freq data=rx_clm; tables ClaimstatusCode; run;*/
/*proc sql; select sum(.<paidamount<0)/count(*) as neg_paid from rx_clm; quit;*/

proc sort data=out.pharmacy_rxcw out=rx_ref nodupkey; by _all_; run;
proc sort data=out.pharmacy_rxcc out=rx_cost nodupkey; by _all_; run;
/*proc sort data=rx_cost out=a dupout=b nodupkey; by filldate memberuid ndc11code supplydayscount; run;*/

/*proc sql; select sum(rxfilluid=.)/count(*) from old.rxclaimcost; quit;*/
/*proc sql; select sum(rxfilluid=.)/count(*) from in.pharmacy_rxcc; quit;*/
/*proc sql; select sum(rxfilluid=.)/count(*) from old.rxfillxref; quit;*/
/*proc sql; select sum(rxfilluid=.)/count(*) from in.pharmacy_rxcw; quit;*/

proc sql;
create table rx_cost as 
select distinct x.*, y.* 
from rx_ref x, rx_cost y 
where x.memberuid=y.memberuid and x.rxfilluid=y.rxfilluid and x.rxfilluid^=.
order by memberuid, rxclaimuid, filldate, ndc11code, supplydayscount;
quit; 
proc sort data=rx_cost nodupkey; by memberuid rxclaimuid filldate ndc11code supplydayscount; run;

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

proc sort data=rx_cost_clean(drop=rxclaimuid) out=rx_cost_dedup dupout=rx_dup nodupkey; by _all_; run;
proc sort data=rx_cost_dedup; by memberuid filldate ndc11code supplydayscount descending sourcemodifieddate descending allowedamount descending unadjustedprice; run;
proc sort data=rx_cost_dedup nodupkey; by memberuid filldate ndc11code supplydayscount; run;

proc sql;
create table out.rx_cost_dedup_npi as
select a.*, b.npi_update as mapped_npi
from rx_cost_dedup a
left join out.provider_mapping b on a.provideruid=b.provideruid
order by memberuid, filldate, ndc11code, supplydayscount;
quit;

proc sql;
select 
count(*) as total_cnt, 
sum(prescribingnpi^="")/count(*) as npi_pct,
sum(mapped_npi^="" and prescribingnpi="")/count(*) as add_pct,
sum(mapped_npi^="" or prescribingnpi="")/count(*) as total_pct,
sum(mapped_npi^="" and prescribingnpi^="" and mapped_npi^=prescribingnpi)/sum(mapped_npi^="" and prescribingnpi^="") as unmatch_pct
from out.rx_cost_dedup_npi;
quit;

proc sql;
select  distinct claimstatuscode="D" as deny, sum(allowedamount>0)/count(*) as allowed_pct, sum(unadjustedprice>0)/count(*) as price_pct
from out.rx_cost_dedup_npi
group by claimstatuscode="D";
quit;

*** map missing price ***;
proc sql;
create table out.rx_cost_final as 
select a.*, coalesce(a.unadjustedprice, b.avg_price) as add_unadjustedprice
from out.rx_cost_dedup_npi a
left join (select ndc11code, supplydayscount, dispensedquantity, mean(unadjustedprice) as avg_price from out.rx_cost_dedup_npi(where=(unadjustedprice>0 and claimstatuscode^="D")) group by 1,2,3) b
on a.unadjustedprice=. and a.claimstatuscode^="D" and a.ndc11code=b.ndc11code and a.supplydayscount=b.supplydayscount and a.dispensedquantity=b.dispensedquantity;
quit;

proc sql;
select count(*) as cnt, 
	sum(unadjustedprice=. and add_unadjustedprice>.)/sum(unadjustedprice=.) as add_pct, 
	sum(add_unadjustedprice>.)/count(*) as tot_pct 
from out.rx_cost_final
where claimstatuscode^="D" ;
quit;



*******************************;
*** clean up medical claims ***;
*******************************;

*** map claim code ***;
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

%transpose(3, cpt);
%transpose(4, cpt_mod);
%transpose(5, hcpcs);
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
proc sort data=out.medical_clm out=claim nodupkey; 
where claimstatuscode not in ("D" "P"); *** remove denied claims ***;
by _all_; 
run;
proc sort data=claim; by memberuid claimuid servicedate servicethrudate descending sourcemodifieddate; run;
proc sort data=claim nodupkey; by memberuid claimuid servicedate servicethrudate; run;

proc sql;
create table out.claim_flat as
select a.*, coalescec(b.cpt_0, d.hcpcs_0) as cpt, coalescec(c.cpt_mod_0, e.hcpcs_mod_0) as cpt_mod_1, f.msdrg_0 as ms_drg, g.pos_0 as pos, 
	h.bill_type_0 as bill_type, i.rev_0 as rev_cd, j.adm_dx_0 as adm_dx_cd, j1.e_code_0 as e_code, j2.*, k.*
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
x.dischargeuid^="" as ipps, e2.claimuid^="" as opps, e2.casetype
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
else if institutionaltypecode="I" and claimformtypecode="P" then claim_type="O"; 
else if institutionaltypecode="I" then claim_type="I";
else claim_type="O"; 

if claim_type="O" and ipps=1 then claim_type="I";
if claim_type="I" and opps=1 then claim_type="O";

run;

/*proc freq data=claim_flat3; tables claimstatuscode/list missing; run;*/

*** remove dup lines ***;
/*proc sort data=claim_flat3 nodupkey; by memberuid servicedate servicethrudate claimuid; run;*/

proc sort data=claim_flat3; 
by memberuid servicedate servicethrudate claimstatuscode professionaltypecode institutionaltypecode /*claimnumber claimlinenumber*/ dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price descending sourcemodifieddate;
run;
proc sort data=claim_flat3 out=claim_flat4 dupout=claim_flat3_dup nodupkey; 
by memberuid servicedate servicethrudate claimstatuscode professionaltypecode institutionaltypecode /*claimnumber claimlinenumber*/ dischargeuid bill_type bill_npi prf_npi npi provideruid pos cpt cpt_mod_1 rev_cd dx_0 px_0 ms_drg price;
run;


** remove reversed claim pairs **;
proc sort data=claim_flat4(where=(claimstatuscode^="R")) out=a; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price dx_0; run;
proc sort data=claim_flat4(where=(claimstatuscode="R"))  out=b; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price; run;
data a; set a; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price; retain seqnum; if first.price then seqnum=1; else seqnum+1; run;
data b; set b; by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price; retain seqnum; if first.price then seqnum=1; else seqnum+1; run;
data out.claim_flat6;
merge a(in=a) b(in=b);
by memberuid servicedate servicethrudate dischargeuid claim_type bill_type bill_npi prf_npi npi provideruid price seqnum;
if a and not b;
drop seqnum;
run;
/*proc freq data=out.claim_flat6; tables claimstatuscode/list missing; run;*/


*** create header medical ***;
proc sql;
create table out.clm_id as
select *, min(coalescec(dischargeuid, claimuid)) as clm_id
from out.claim_flat6
group by memberuid, servicedate, servicethrudate, claimstatuscode, sourcemodifieddate, /*claimnumber,*/ claim_type, dischargeuid, bill_type, bill_npi, prf_npi, npi, provideruid, pos, dx_0, px_0, ms_drg, finaldrg
order by memberuid, servicedate, servicethrudate, clm_id;
quit;


data claim_ip_line;
set out.clm_id; 
where claim_type="I";
if admissiondate=. then admissiondate=servicedate;
if dischargedate=. then dischargedate=servicethrudate;
run;

proc sort data=claim_ip_line; 
by memberuid admissiondate dischargedate descending sourcemodifieddate descending adj_price descending servicethrudate servicedate descending bill_type claimstatuscode descending allowedamount dischargeuid /*claimnumber claimlinenumber*/ ;
run;
proc sort data=claim_ip_line out=claim_ip nodupkey; 
by memberuid clm_id;
run;
proc sort data=claim_ip; by memberuid provideruid admissiondate descending dischargedate descending adj_price; run;
data stay_ip;
set claim_ip;
by memberuid provideruid;
retain st_dt end_dt stay_id;
if first.provideruid then do; st_dt=admissiondate; end_dt=dischargedate; stay_id=clm_id; end;
if dischargedate>end_dt then do; st_dt=admissiondate; end_dt=dischargedate; stay_id=clm_id; end;
run;

proc sort data=stay_ip out=stay_ip_dedup dupout=stay_id_dup nodupkey; by memberuid stay_id; run;

data claim_op_line;
set out.clm_id; 
where claim_type^="I";
run;
/*proc sql; select casetype, count(*), sum(dx_0="")/count(*) as dx_missing from claim_op_line group by casetype; quit;*/

proc sql;
create table claim_op as
select distinct memberuid, clm_id, servicedate, servicethrudate, claimstatuscode,/*claimnumber,*/ claim_type, bill_type, provideruid, bill_npi, prf_npi, npi, dx_0, dx_1, px_0, pos,
	max(ms_drg) as ms_drg, sum(paidamount) as paid_amt, sum(copayamount) as copay_amt, sum(allowedamount) as allowed_amt, sum(price) as price, sum(adj_price) as adj_price,
	max(sourcemodifieddate) as sourcemodifieddate, sum(casetype="LAB")>0 as lab_flag
from claim_op_line
group by memberuid, clm_id
order by memberuid, servicedate, servicethrudate /*claimnumber*/;
quit;

data out.medical;
set stay_ip_dedup claim_op;
keep memberuid servicedate servicethrudate claimstatuscode /*claimnumber claimlinenumber*/ clm_id claim_type bill_type admissiondate dischargedate UBPatientDischargeStatusCode
	bill_npi prf_npi npi provideruid pos dx_0 px_0 ms_drg finaldrg paid_amt copay_amt allowed_amt price adj_price sourcemodifieddate lab_flag;
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
drop i;
if dx_0="" and (price=. or price=0) then delete;
run;

proc sql; select distinct name into :col separated by " "
from sashelp.vcolumn 
where libname="OUT" and memname="MEDICAL2" and upcase(name) not in ("CLM_ID" "SOURCEMODIFIEDDATE" "CLAIMSTATUSCODE" "DX_0" "PRF_NPI" "BILL_NPI" "BILL_TYPE" "BILLTYPE" "FREQ_CD"); quit;
proc sort data=out.medical2; by memberuid servicedate servicethrudate descending dx_0 descending sourcemodifieddate; run;
proc sort data=out.medical2 out=medical3 nodupkey; by &col.; run;

*** missing dx ***;

proc sql;
create table missing_dx as
select distinct a.*, coalescec(b.clm_id, a.clm_id) as clm_id_new, c.dx_0 as mapped_dx_0
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
select a.*, b.*, coalescec(dx_0, mapped_dx_0) as dx_0_new
from missing_dx(where=(clm_id=clm_id_new)) a
left join (select memberuid, clm_id_new, sum(price) as price_new, sum(adj_price) as adj_price_new, sum(paid_amt) as paid_amt_new, sum(copay_amt) as copay_amt_new, sum(allowed_amt) as allowed_amt_new
			from missing_dx group by memberuid, clm_id_new) b
on a.memberuid=b.memberuid and a.clm_id_new=b.clm_id_new
order by memberuid, servicedate, servicethrudate, provideruid;
quit;

proc sql;
select clm_type, count(*), sum(dx_0_new="")/count(*)
from out.medical3
group by clm_type;
quit;


proc sql;
select distinct /*claim_type,*/ count(distinct memberuid) as pt_cnt, count(*) as clm_cnt, 
	sum(bill_npi^="")/count(*) as bill_npi, sum(prf_npi^="")/count(*) as prf_npi, sum(npi^="")/count(*) as npi,
	sum(bill_npi^="" or prf_npi^="" or npi^="") as clm_w_npi, sum(bill_npi^="" or prf_npi^="" or npi^="")/count(*) as with_any_npi,
	sum(allowed_amt>0) as clm_w_allowed, sum(allowed_amt>0)/count(*) as allowed_gt_0, sum(allowed_amt=0)/count(*) as zero_allowed, 
	sum(paid_amt>0) as clm_w_paid, sum(paid_amt>0)/count(*) as paid_gt_0, 
	sum(adj_price>0)/count(*) as price_gt_0, sum(adj_price=0)/count(*) as zero_price,
	sum(allowed_amt>0 or adj_price>0)/count(*) as with_any_gt_0,
	sum(adj_price>0 and (bill_npi^="" or prf_npi^="" or npi^=""))/count(*) as have_npi_price, 
	sum(ms_drg="")/count(*) as msdrg_missing, sum(finaldrg="")/count(*) as finaldrg_missing
from out.medical3
/*group by claim_type*/
;
quit;
proc sql;
select distinct clm_type, count(distinct memberuid) as pt_cnt, count(*) as clm_cnt, 
	sum(bill_npi^="")/count(*) as bill_npi, sum(prf_npi^="")/count(*) as prf_npi, sum(npi^="")/count(*) as npi,
	sum(bill_npi^="" or prf_npi^="" or npi^="")/count(*) as with_any_npi,
	sum(allowed_amt>0)/count(*) as allowed_gt_0, sum(allowed_amt=0)/count(*) as zero_allowed, sum(paid_amt>0)/count(*) as paid_gt_0, 
	sum(pos^="")/count(*) as pos_pct,
	sum(adj_price>0)/count(*) as price_gt_0, sum(adj_price=0)/count(*) as zero_price,
	sum(adj_price>0 and (bill_npi^="" or prf_npi^="" or npi^=""))/count(*) as have_npi_price, 
	sum(ms_drg="")/count(*) as msdrg_missing, sum(finaldrg="")/count(*) as finaldrg_missing,
	sum(dx_0="")/count(*) as dx_0_missing, sum(dx_0_new="")/count(*) as dx_0_new_missing
from out.medical3
group by clm_type;
quit;



*** map additional price ***;
proc sort data=out.medical_ipps out=ipps_drg_price; where finaldrg^=""; by finaldrg dischargedate los; run;
proc sql;
create table out.ip_zip as
select a.*, (dischargedate-admissiondate+1) as los, b.zip3value, c.payertypecode, d.productcode
from out.medical(where=(claim_type="I")) a
left join (select distinct memberuid, zip3value from out.enrollment_med_payergroupcode where payergroupcode="C") b on a.memberuid=b.memberuid
left join out.enrollment_med_payertypecode c on a.memberuid=c.memberuid and a.dischargedate between c.start_dt and c.end_dt
left join out.enrollment_med_productcode d on a.memberuid=d.memberuid and a.dischargedate between d.start_dt and d.end_dt
order by finaldrg, zip3value, los, dischargedate, payertypecode, productcode ;
quit;
proc sql;
create table out.op_zip as
select a.*, b.zip3value, c.payertypecode, d.productcode
from out.claim_flat6(where=(claim_type^="I")) a
left join (select distinct memberuid, zip3value from out.enrollment_med_payergroupcode where payergroupcode="C") b on a.memberuid=b.memberuid
left join out.enrollment_med_payertypecode c on a.memberuid=c.memberuid and a.dischargedate between c.start_dt and c.end_dt
left join out.enrollment_med_productcode d on a.memberuid=d.memberuid and a.dischargedate between d.start_dt and d.end_dt
order by cpt, cpt_mod_1, claim_type, pos, zip3value, servicedate, payertypecode, productcode;
quit;

proc sql;
create table cpt_missing as
select cpt, cpt_mod_1, claim_type, count(*) as cnt, sum(price=.)/count(*) as missing_pct
from out.op_zip
group by cpt, cpt_mod_1, claim_type
order by cpt, cpt_mod_1, claim_type;
quit;

data clm_148280814; set out.medical_clm; where claimuid="9531600696170844"; run;
data xref_148280814; set out.medical_xref; where memberuid=148280814; run;
data code_148280814; set out.medical_ccd; where claimuid="9531600696170844"; run;
data cost_148280814; set out.medical_ipps; where dischargeclaimuid="198057154"; run;

data clm_232672583; set out.medical_clm; where claimuid="885647377103548681"; run;
data xref_232672583; set out.medical_xref; where claimuid="885647377103548681"; run;
data code_232672583; set out.medical_ccd; where claimuid="885647377103548681"; run;
data cost_232672583; set out.medical_ipps; where dischargeclaimuid="163855771"; run;

data clm_236505593; set out.medical_clm; where claimuid="676961012295913507"; run;
data xref_236505593; set out.medical_xref; where claimuid="676961012295913507"; run;
data code_236505593; set out.medical_ccd; where claimuid="676961012295913507"; run;
data cost_236505593; set out.medical_ipps; where dischargeclaimuid="56990761";  run;


proc sql;
create table all_line as
select distinct a.*, b.clm_id_new
from out.clm_id a, missing_dx b
where a.memberuid=b.memberuid and a.clm_id=b.clm_id;
quit;

*** create serviceline table ***;
data serviceline;
set all_line;
keep memberuid clm_id clm_id_new servicedate servicethrudate /*claimnumber claimlinenumber*/ cpt cpt_mod_1-cpt_mod_4 rev_cd 
	paidamount copayamount allowedamount price adj_price sourcemodifieddate majorsurgeryindicator roomboardindicator serviceunitquantity;
run;
proc sort data=serviceline; by memberuid clm_id_new clm_id descending price; run;
data out.serviceline;
set serviceline;
by memberuid clm_id_new;
retain line_num;
if first.clm_id then line_num=1;
else line_num+1;
run;

*** create dx/px table ***;
%macro dx_px(type);

proc sort data=all_line(keep=memberuid clm_id clm_id_new claimuid &type._:) out=&type.; by memberuid clm_id_new clm_id claimuid; run;
proc transpose data=&type. out=&type.2;
by memberuid clm_id_new clm_id claimuid;
var &type._:;
run;

data &type.3;
set out.medical3(keep=memberuid clm_id_new &type._0 rename=(&type._0=&type._cd) in=a) &type.2(where=(&type._cd^="") rename=(col1=&type._cd) in=b);
if a then prim_&type.=1; else prim_&type.=0;
if &type._cd^="";
keep memberuid clm_id_new &type._cd prim_&type.; 
run;
proc sort data=&type.3; by memberuid clm_id_new &type._cd descending prim_&type.; run;
proc sort data=&type.3 nodupkey; by memberuid clm_id_new &type._cd; run;
proc sort data=&type.3; by memberuid clm_id_new descending prim_&type.; run;

data out.&type.;
set &type.3;
by memberuid clm_id_new;
retain seq_num;
if first.clm_id_new then seq_num=1;
else seq_num+1;
keep memberuid clm_id_new &type._cd seq_num;
run;

%mend;

%dx_px(dx);
%dx_px(px);








proc sql;
create table sample_st as
select statecode, count(*) as bene_cnt
from 
  (select distinct a.memberuid, a.statecode
  from in.enrollment_member as a
  inner join in.enrollment_records as b on a.memberuid=b.memberuid
  where b.effectivedate<= mdy(12,31,2022) and b.terminationdate>= mdy(1,1,2022))
group by statecode
order by statecode;
quit;

proc sql;
create table sample_st_clm as
select b.statecode, sum(clm_cnt) as clm_cnt
from 
  (select memberuid, count(*) as clm_cnt from out.medical where year(servicethrudate)=2022 group by memberuid) a,
  out.enrollment_med_payergroupcode b 
where a.memberuid=b.memberuid
group by b.statecode
order by statecode;
quit;

proc sql;
create table sample_st_pct as 
select a.*, b.bene_cnt as sample_cnt, b.bene_cnt/a.bene_cnt as sample_pct, c.clm_cnt as sample_clm_cnt, c.clm_cnt/(calculated sample_pct) as clm_cnt
from out.inovalon_comm_member_cnt_2022 a
left join sample_st b on a.statecode=b.statecode
left join sample_st_clm c on a.statecode=c.statecode
order by statecode;
quit;
proc export data=sample_st_pct outfile="D:\SASData\dua_052882\Sndbx\Jun_W\Inovalon\250K sample QC\sample_st_pct.csv" replace; run;

proc sql;
select sum(bene_cnt), sum(sample_cnt)
from sample_st_pct;
quit;

proc sql;
select max(seq_num)
from out.diagnosis;
quit;


proc sql;
select sum(ms_drg=finaldrg)/count(*) as match_pct
from out.medical2
where ms_drg^="" and finaldrg^="";
quit;

proc sql;
select sum(ms_drg=finaldrg)/count(*) as match_pct
from
	(select distinct a.dischargeuid, input(ms_drg,3.) as ms_drg, finaldrg
	from oldout.claim_flat2 a, oldout.ippsclaimcost b
	where ms_drg^="" and finaldrg^=. and a.dischargeuid=b.dischargeuid)
;
quit;


***************************;
*** map additional NPIs ***;
***************************;

proc sql;
create table med_nppes as
select a.*, b.is_sole_proprietor as sole_npi, b.entity_code as npi_entity, 
			c.is_sole_proprietor as sole_bill, c.entity_code as bill_entity, 
			d.is_sole_proprietor as sole_prf, d.entity_code as prf_entity
from out.medical3 a
left join meta2.npi_spec_x_new b on a.npi=b.npi and b.npi^=""
left join meta2.npi_spec_x_new c on a.bill_npi=c.npi and c.npi^=""
left join meta2.npi_spec_x_new d on a.prf_npi=d.npi and d.npi^=""
order by memberuid, servicethrudate, provideruid;
quit;

data med_npi_update;
set med_nppes;
if npi^="" then do;
	if bill_npi="" then do;
		if sole_npi="Y" or npi_entity="2" then bill_npi_mapped=npi;
	end;
	if prf_npi="" then do;
		if npi_entity="1" then prf_npi_mapped=npi;
	end;
end;

if bill_npi^="" and prf_npi="" and sole_bill="Y" then prf_npi=bill_npi;
if prf_npi^="" and bill_npi="" and sole_prf="Y" then bill_npi=prf_npi;

bill_npi_update=coalescec(bill_npi, bill_npi_mapped);
prf_npi_update=coalescec(prf_npi, prf_npi_mapped);
run;

proc sql;
select distinct clm_type, count(*) as clm_cnt, 
	sum(bill_npi^="")/count(*) as bill_npi, sum(bill_npi_update^="")/count(*) as bill_npi_update,
	sum(prf_npi^="")/count(*) as prf_npi, sum(prf_npi_update^="")/count(*) as prf_npi_update,
	sum(bill_npi^="" and prf_npi^="")/count(*) as with_both_npi, sum(bill_npi_update^="" and prf_npi_update^="")/count(*) as with_both_npi_update
from med_npi_update
group by clm_type;
quit;

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
select distinct clm_type, count(*) as clm_cnt, 
	sum(bill_npi^="")/count(*) as bill_npi, sum(bill_npi_update^="")/count(*) as bill_npi_update, sum(bill_npi_update2^="")/count(*) as bill_npi_update2,
	sum(prf_npi^="")/count(*) as prf_npi, sum(prf_npi_update^="")/count(*) as prf_npi_update, sum(prf_npi_update2^="")/count(*) as prf_npi_update2,
	sum(bill_npi^="" and prf_npi^="")/count(*) as with_both_npi, sum(bill_npi_update^="" and prf_npi_update^="")/count(*) as with_both_npi_update, 
	sum(bill_npi_update2^="" and prf_npi_update2^="")/count(*) as with_both_npi_update2
from med_npi_update2
group by clm_type;
quit;

proc sql;
create table out.medical4 as
select *
from med_npi_update2 where clm_id_new not in 
(select distinct b.clm_id_new
	from med_npi_update2(where=(bill_npi_update2^="" or prf_npi_update^="")) a,
		 med_npi_update2(where=(bill_npi_update2="" and prf_npi_update="")) b
	where a.memberuid=b.memberuid and a.clm_type=b.clm_type and a.servicedate=b.servicedate and a.servicethrudate=b.servicethrudate and a.dx_0=b.dx_0 and a.price=b.price)
order by memberuid, servicedate, servicethrudate, provideruid;
quit;

proc sql;
create table medical4 as
select *, round(sum(bill_npi_update2="" and prf_npi_update2="")/count(*),0.01) as npi_missing_pct
from out.medical4 
group by memberuid
order by memberuid, servicedate, servicethrudate, provideruid;
quit;

data medical4;
set medical4;
length npi_missing_grp $20.;
if npi_missing_pct=0 then npi_missing_grp="1.No missing";
else if npi_missing_pct<0.2 then npi_missing_grp="2.1-20%";
else if npi_missing_pct<0.5 then npi_missing_grp="3.20-50%";
else if npi_missing_pct<0.8 then npi_missing_grp="4.50-80%";
else if npi_missing_pct<1 then npi_missing_grp="5.80-99%";
else if npi_missing_pct=1 then npi_missing_grp="6.100% missing";
run;

proc sql;
select distinct npi_missing_grp, count(distinct memberuid) as bene_cnt, count(*) as clm_cnt, 
	sum(bill_npi^="")/count(*) as bill_npi, sum(bill_npi_update^="")/count(*) as bill_npi_update, sum(bill_npi_update2^="")/count(*) as bill_npi_update2,
	sum(prf_npi^="")/count(*) as prf_npi, sum(prf_npi_update^="")/count(*) as prf_npi_update, sum(prf_npi_update2^="")/count(*) as prf_npi_update2,
	sum(bill_npi^="" and prf_npi^="")/count(*) as with_both_npi, sum(bill_npi_update^="" and prf_npi_update^="")/count(*) as with_both_npi_update, 
	sum(bill_npi_update2^="" and prf_npi_update2^="")/count(*) as with_both_npi_update2
from medical4
group by npi_missing_grp
order by npi_missing_grp;
quit;

proc print data=out.medical_prv; where provideruid=380568669;run;
proc print data=out.medical_psp; where provideruid=380568669;run;
proc print data=out.pharmacy_prv; where provideruid=380568669;run;
proc print data=out.pharmacy_psp; where provideruid=380568669;run;
proc print data=provideruid_to_npi_mapping; where provideruid=380568669;run;
data p_380568669_3; set med_npi_update3; where provideruid=380568669;run;
data p_380568669_flat; set out.claim_flat2; where provideruid=380568669;run;


