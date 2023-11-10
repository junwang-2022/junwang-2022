%LET _CLIENTTASKLABEL='Attribution_Macros';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Builder_For_Testing.egp';
%LET _CLIENTPROJECTPATHHOST='apcw10sg08.ap.r53.ccwdata.org';
%LET _CLIENTPROJECTNAME='Builder_For_Testing.egp';
%LET _SASPROGRAMFILE='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Attribution_Macros.sas';
%LET _SASPROGRAMFILEHOST='apcw10sg08.ap.r53.ccwdata.org';

GOPTIONS ACCESSIBLE;
/****************************************************************************
SAS PROGRAM DESCRIPTION
		
PROGRAM NAME: Attribution_Macros.sas
PROGRAM OWNER: Samantha Crow
PROGRAM CREATED:  June 2023
PROGRAM MODIFIED: n/a

PURPOSE: macros for attribution and roll ups for quality measures

INPUT DATA: THRESHOLDS.csv
			o&i._&yr..&id._medical
			o&i._&yr..&id._numerator
			o&i._&yr..&id._numerator_medical
			ccmffs.serviceline_&year._&mn.

OUTPUT DATA: &finaldir..QM_&id._NPI
			&finaldir..QM_&id._FAC
			&finaldir..QM_&id._ASC

EXPORT DATA: QM_&year..csv
			 CONTROL_ROW_COUNT_QM
			 CONTROL_METRICS_QC_QM

****************************************************************************/

%macro roll_up(id,QM,type);

%do i = 1 %to 12;

	libname o&i._&yr. "/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Juan_M/Builder2/V10/Output/Partition_Out_&year._&QM./Output&i." access=readonly;

	/* pull denominator level data using medical dataset */
	data &id._prov00_&year._&i.;
	set o&i._&yr..&id._medical;
	where index_case = 1;
	keep bene_id CLM_TYPE ccn bill_npi OP_NPI AT_NPI prf_npi PRF_SPEC TIN def_id index_claim index_case;
	run;	

	/* identify attributing provider (NPI) */
	proc sort data = &id._prov00_&year._&i. nodupkey out = &id._prov00_&year._&i._;
	where PRF_SPEC ~= '49';
	by bene_id def_id descending index_claim;
	run;

	proc sort data = &id._prov00_&year._&i._ nodupkey out = &id._prov01_&year._&i. (keep = bene_id def_id prf_npi op_npi at_NPI) 
														dupout = &id._dups00_&year._&i.;
	by bene_id def_id;
	run;

	
	/* pull facility */	
	data &id._fac00_&year._&i.;
	set &id._prov00_&year._&i.;
	where ccn ~= '';
	run;

	proc sort data = &id._fac00_&year._&i. nodupkey out = &id._fac00_&year._&i._;
	by bene_id def_id descending index_claim;
	run;

	proc sort data = &id._fac00_&year._&i._ nodupkey out = &id._fac01_&year._&i. (keep = bene_id def_id ccn)
														dupout = &id._dups01_&year._&i.;
	by bene_id def_id;
	run;	


	/* pull ASCs */
	data &id._asc00_&year._&i.;
	set &id._prov00_&year._&i.;
	where PRF_SPEC = '49';
	run;

	proc sort data = &id._asc00_&year._&i. nodupkey out = &id._asc00_&year._&i._;
	by bene_id def_id descending index_claim;
	run;

	proc sort data = &id._asc00_&year._&i._ nodupkey out = &id._asc01_&year._&i. (keep = bene_id def_id prf_npi)
														dupout = &id._dups02_&year._&i.;
	by bene_id def_id;
	run;	


	/* merge all attributing datasets together */
	data &id._attr00_&year._&i.;
	merge 	&id._prov01_&year._&i. (in = a)
			&id._fac01_&year._&i. (in = b)
			&id._asc01_&year._&i. (in = c rename = (prf_npi = asc));
	by bene_id def_id;
	if a = 1 or b = 1 or c = 1;
	length attrib_NPI $12.;
	length attrib_fac $10.;
	length attrib_ASC $12.;

	attrib_npi = coalesce(OP_NPI, PRF_NPI, AT_NPI);	
	attrib_fac = ccn;
	attrib_ASC = ASC;
	run;


	/* finish logic for measures where the attributing NPI is the Performing NPI on the denominator */
	%if "&type." = "TYPE1" %then %do;

		/* merge attributing provider details */
		data &id._msre01_&year._&i.;
		merge o&i._&yr..&id._numerator (in = a)
			&id._attr00_&year._&i. (in = b);
		by bene_id def_id;
		if a = 1;

		numerator_in = sum(of numerator_in_:);

		if exclusion = 0 then denom = 1;
		else denom = 0;

		if exclusion = 0 and numerator_in > 0 then num = 1;
		else num = 0;

		year = &year.;

		run;

	%end;


	/* finish logic for measures where the attributing NPI is the Performing NPI on the denominator
		and must match the Performing NPI on the numerator */

	%if "&type." = "TYPE2" or "&type." = "TYPE3" %then %do;

		/* pull NPI from numerator - make sure it matches the denominator NPI */
		data &id._num00_&year._&i.;
		set o&i._&yr..&id._numerator_medical;
		length NUM_NPI $12.;
		NUM_NPI = coalesce(OP_NPI,prf_npi);
		keep bene_id CLM_TYPE ccn bill_npi OP_NPI prf_npi PRF_SPEC TIN def_id index_claim index_case NUM_NPI;
		run;
	 
		/* create exhaustive list of NPIs from index case */
		proc sort data = &id._num00_&year._&i. nodupkey out = &id._num01_&year._&i. (keep = bene_id def_id NUM_NPI);
		by bene_id def_id NUM_NPI;
		run;	


		/* merge attributing provider details */
		data &id._msre01_&year._&i._;
		merge o&i._&yr..&id._numerator (in = a)
			&id._attr00_&year._&i. (in = b);
		by bene_id def_id;
		if a = 1;

		numerator_in = sum(of numerator_in_:);

		if exclusion = 0 then denom = 1;
		else denom = 0;

		if exclusion = 0 and numerator_in > 0 then num_ = 1;
		else num_ = 0;

		year = &year.;

		run;

		/* join npi from numerator to make sure it matches attributing NPI */
		proc sql;
		create table &id._msre01_&year._&i. as
		select  a.*,
				b.NUM_NPI,
			    case when a.attrib_npi = b.NUM_NPI and num_ = 1 and "&type." = "TYPE2" then 1 
					when "&type." = "TYPE2" then 0
					when a.attrib_npi = b.NUM_NPI and "&type." = "TYPE3" then 0
					when "&type." = "TYPE3" and num_ = 1 then 1
					else 0 end as num
		from &id._msre01_&year._&i._ a
		left join &id._num01_&year._&i. b
		on a.bene_id = b.bene_id
		and a.def_id = b.def_id
		and a.attrib_npi = b.num_npi
		;
		quit;
	
	%end;


	/* summaries */
	proc means data = &id._msre01_&year._&i. nway missing noprint;
	where exclusion = 0;
	class year def_name attrib_npi attrib_fac attrib_asc;
	var allowed_amt denom num;
	output out = &id._msre02_&year._&i. (drop = _TYPE_) sum=;
	run;

%end;

/* combine all iterations */
data &id._msre02_&year.;
set &id._msre02_&year._:;
run;

/* if measure has specific thresholds for the numerator, it needs an extra filter prior to roll up */
	%if "&id." = "GAM_133" %then %do;
		%num_threshold(GAM_133, 2);
	%end;

	%if "&id." = "GAM_150" %then %do;
		%num_threshold(GAM_150, 2);
	%end;


		%macro summaries(type);

			/* summarize again, add denominator rates */
			proc sql;
			create table &id._&type._sum01_&year. as
			select	a.year, 
					a.def_name,
					b.DEF_DESC,
					a.attrib_&type.,
					b.quality_indicator,
					b.DENOM_THRESHOLD,
					case when sum(a.num) < 11 then . else sum(a.denom) end as denominator,
					case when sum(a.num) < 11 then . else sum(a.num) end as numerator,
					sum(a.num)/sum(a.denom) as rate
			from &id._msre02_&year. a
			left join thresholds b
			on a.def_name = b.def_name
			where a.attrib_&type. ~= '' or a.attrib_&type. ~= '.'
			group by 1,2,3,4,5,6
			having sum(a.denom) >= b.DENOM_THRESHOLD
			order by 1,2,3,4,5,6;
			quit;
	
			/* replace logic for new years */

			%if  %sysfunc(exist(&finaldir..QM_&id._&type.)) %then %do;
				data QM_&id._&type._temp;
				set &finaldir..QM_&id._&type.;
				where year ~= &year. and year ~= . ;
				run;

				data &finaldir..QM_&id._&type.;
				set QM_&id._&type._temp &id._&type._sum01_&year.;
				run;
			%end;
		
			/* if table doesnt exist, initialize table and add data to it*/

			%else %do;
				data &finaldir..QM_&id._&type.;
				set &id._&type._sum01_&year.;
				run;
			%end;

		%mend;

		%summaries(NPI);
		%summaries(FAC);
		%summaries(ASC);

%mend;



/* this macro is used above only for measures with a threshold in the numerator */
%macro num_threshold(id, num_thres);

%include "/sas/vrdc/users/&sysuserid./files/dua_052882/prod/utils/Rsubmit_Macros.sas";

	%let first_mon = %scan(&months,1,%str( ));
	%let last_mon = %scan(&months,%sysfunc(countw(&months)),%str( ));

/*	%rsubmit_signon;*/
	%do i =&first_mon. %to &last_mon.;
			%let mn=%sysfunc(putn(&i,z2.));
			%PUT mn=&mn;
/*			%SYSLPUT _ALL_/REMOTE=task&i;*/
/*			rsubmit task&i wait=no connectpersist=no;*/

			/* define macro variable for list of procedure codes needed for filtering */
			%if "&id." = "GAM_133" %then %do;
				%let num_thres_list = '11102','11104','11106';
			%end;

			%if "&id." = "GAM_150" %then %do;
				%let num_thres_list = '64635','64636','64999';
			%end;

			%put num_thres_list=&num_thres_list.;

			%include "&myfiles_root/dua_052882/prod/utils/CCM_Macros.sas";
			%gbl_ccm_libnames(PROD);	

			/* create a numerator threshold by provider. pulling all ffs data as the numerator
					data from the episode builder output uses denominator criteria to pull */
			data &id._num_thres00_&mn.;
			set ccmffs.serviceline_&year._&mn.;
			where cpt in (&num_thres_list.) and LINE_PRF_NPI ~= '';
			keep LINE_PRF_NPI CPT;
			run;

/*			endrsubmit;*/
	%end;
/*	%rsubmit_signoff;*/

/*	 combine all years */
	data &id._num_thres00;
	set &id._num_thres00_:;
	run;

	/* create an exhaustive list of provider with the threshold needed to be included in the measure summary */
	proc sql;
	create table &id._num_thres01 as
	select 	LINE_PRF_NPI as NPI,
			count(*) as NUM_PROCS
	from &id._num_thres00
	group by 1
	having count(*) >= &num_thres.
	order by 1
	;
	quit;

	/* merge list onto final output above to remove providers without cases higher than threshold */
	proc sql;
	create table &id._msre02_&year.00 as
	select	a.*
	from &id._msre02_&year. a
	inner join &id._num_thres01 b
	on compress(a.attrib_npi) = compress(b.NPI)
	;
	quit;

	data &id._msre02_&year.;
	set &id._msre02_&year.00;
	run;

%mend;



%macro gam_100_roll_up(id,QM,type);

%do i = 1 %to 12;

/**********/

	libname m&i._&yr. "/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Juan_M/Builder2/V10/Output/Partition_Out_&year._&QM./Output&i." access=readonly;

	/* pull denominator level data using medical dataset */
	data &id._prov00_&year._&i.;
	set m&i._&yr..&id._medical;
	where index_case = 1;
	keep bene_id CLM_TYPE ccn bill_npi OP_NPI AT_NPI prf_npi PRF_SPEC TIN def_id index_claim index_case;
	run;	

	/* identify attributing provider (NPI) */

	/* Grab necessary CPT codes from metadata */
	data GAM_100_codes;
	set META_DT.cpt;

	if CPT in ('17311' '17313') then GAM_100_DENOM = 'M';
	else GAM_100_DENOM = GAM_100;

	if GAM_100_DENOM = '' then DELETE;
	keep CPT GAM_100 GAM_100_DENOM;
	run;


	/* pull both sets of procedure codes from serviceline with the performing NPI */
	proc sort data = m&i._&yr..&id._serviceline out = &id._serviceline_&year._&i.;
	by CPT;
	run;

	data &id._npi00a_&year._&i. &id._npi00b_&year._&i. PROBLEM;
	merge 	&id._serviceline_&year._&i. (in = a keep = bene_id CLM_TYPE LINE_PRF_NPI LINE_PRF_SPEC def_id CPT)
			GAM_100_codes (in = b keep = CPT GAM_100_DENOM);
	by CPT;
	if a = 1 and b = 1;

	if GAM_100_DENOM = 'M' then output &id._npi00a_&year._&i.;
	else if GAM_100_DENOM = 'A' then output &id._npi00b_&year._&i.;
	else output PROBLEM;
	run;


	/* get a procedure count by provider  */
	proc freq data = &id._npi00a_&year._&i. noprint;
	where LINE_PRF_NPI ~= '';
	tables bene_id * def_id * line_prf_npi / missing out = &id._npi00a_&year._&i._ (drop = PERCENT);
	run;

	proc freq data = &id._npi00b_&year._&i. noprint;
	where LINE_PRF_NPI ~= '';
	tables bene_id * def_id * line_prf_npi / missing out = &id._npi00b_&year._&i._  (drop = PERCENT);
	run;


	/* confirm both procedures in the denominator were done by the same provider. 
			this will also be the attributing provider */
	proc sql;
	create table &id._npi01_&year._&i. as
	select 	a.bene_id, 
			a.def_id,
			b.LINE_PRF_NPI as ATTRIB_NPI,
			a.COUNT as M_COUNT,
			b.COUNT as A_COUNT
	from &id._npi00a_&year._&i._  a
	inner join &id._npi00b_&year._&i._ b
	on a.bene_id = b.bene_id and a.def_id = b.def_id and a.LINE_PRF_NPI = b.LINE_PRF_NPI
	;
	quit;


	/* remove duplicate providers on the off chance there are more than one */
	proc sort data = &id._npi01_&year._&i.;
	by bene_id def_id descending M_COUNT descending A_COUNT;
	run;

	proc sort data = &id._npi01_&year._&i. nodupkey out = &id._npi02_&year._&i. (drop = M_COUNT A_COUNT) dupout = &id.__prov02_&year._&i.;
	by bene_id def_id;
	run;
/**********/

	
	/* pull facility */
	
	data &id._fac00_&year._&i.;
	set &id._prov00_&year._&i.;
	where ccn ~= '';
	run;

	proc sort data = &id._fac00_&year._&i. nodupkey out = &id._fac00_&year._&i._;
	by bene_id def_id descending index_claim;
	run;

	proc sort data = &id._fac00_&year._&i._ nodupkey out = &id._fac01_&year._&i. (keep = bene_id def_id ccn)
														dupout = &id._dups01_&year._&i.;
	by bene_id def_id;
	run;	


	/* pull ASCs */

	data &id._asc00_&year._&i.;
	set &id._prov00_&year._&i.;
	where PRF_SPEC = '49';
	run;

	proc sort data = &id._asc00_&year._&i. nodupkey out = &id._asc00_&year._&i._;
	by bene_id def_id descending index_claim;
	run;

	proc sort data = &id._asc00_&year._&i._ nodupkey out = &id._asc01_&year._&i. (keep = bene_id def_id prf_npi)
														dupout = &id._dups02_&year._&i.;
	by bene_id def_id;
	run;	


	/* merge all attributing datasets together */

	data &id._attr00_&year._&i.;
	merge 	&id._npi02_&year._&i. (in = a)
			&id._fac01_&year._&i. (in = b)
			&id._asc01_&year._&i. (in = c rename = (prf_npi = asc));
	by bene_id def_id;
	if a = 1 or b = 1 or c = 1;
	length attrib_fac $10.;
	length attrib_ASC $12.;

	attrib_fac = ccn;
	attrib_ASC = ASC;
	run;


	/* finish logic for measures where the attributing NPI is the Performing NPI on the denominator */
	%if "&type." = "TYPE1" %then %do;

		/* merge attributing provider details */
		data &id._msre01_&year._&i.;
		merge m&i._&yr..&id._numerator (in = a)
			&id._attr00_&year._&i. (in = b);
		by bene_id def_id;
		if a = 1;

		numerator_in = sum(of numerator_in_:);

		if exclusion = 0 and attrib_npi ~= '' then denom = 1;
		else denom = 0;

		if exclusion = 0 and attrib_npi ~= '' and numerator_in > 0 then num = 1;
		else num = 0;

		year = &year.;

		run;

	%end;


	/* summaries */
	proc means data = &id._msre01_&year._&i. nway missing noprint;
	where exclusion = 0;
	class year def_name attrib_npi attrib_fac attrib_asc;
	var allowed_amt denom num;
	output out = &id._msre02_&year._&i. (drop = _TYPE_) sum=;
	run;

%end;

/* combine all iterations */
data &id._msre02_&year.;
set &id._msre02_&year._:;
run;


		%macro summaries(type);

			/* summarize again, add denominator rates */
			proc sql;
			create table &id._&type._sum01_&year. as
			select	a.year, 
					a.def_name,
					b.DEF_DESC,
					a.attrib_&type.,
					b.quality_indicator,
					b.DENOM_THRESHOLD,
					case when sum(a.num) < 11 then . else sum(a.denom) end as denominator,
					case when sum(a.num) < 11 then . else sum(a.num) end as numerator,
					sum(a.num)/sum(a.denom) as rate
			from &id._msre02_&year. a
			left join thresholds b
			on a.def_name = b.def_name
			where a.attrib_&type. ~= '' or a.attrib_&type. ~= '.'
			group by 1,2,3,4,5,6
			having sum(a.denom) >= b.DENOM_THRESHOLD
			order by 1,2,3,4,5,6;
			quit;
	
			/* replace logic for new years */
			%if  %sysfunc(exist(&finaldir..QM_&id._&type.)) %then %do;
				data QM_&id._&type._temp;
				set &finaldir..QM_&id._&type.;
				where year ~= &year. and year ~= . ;
				run;

				data &finaldir..QM_&id._&type.;
				set QM_&id._&type._temp &id._&type._sum01_&year.;
				run;
			%end;
		
			/* if table doesnt exist, initialize table and add data to it*/
			%else %do;
				data &finaldir..QM_&id._&type.;
				set &id._&type._sum01_&year.;
				run;
			%end;

		%mend;

		%summaries(NPI);
		%summaries(FAC);
		%summaries(ASC);

%mend;






/* EXPORTS */
%macro create_exports(input, type, prov_type, type2);

proc sql;
create table export_&year._&type2._&input. as
select	YEAR,
		def_name as MEASURE_ID,
		"&prov_type." as PROVIDER_ROLE_NM length=25,
		attrib_&type. as PROVIDER_ID format$50.,
		DENOMINATOR,
		NUMERATOR,
		RATE
from &finaldir..QM_&input.
where year = &year. and PROVIDER_ID ~= ''
;
quit;

%mend;



%macro combine_exports();

/* combine exports */
data &exprtdir..QM_PRVDR_HCP_&year.;
set export_&year._HCP_:;
where PROVIDER_ID not in ('', '.', '           .');
run;

data &exprtdir..QM_PRVDR_HCF_&year.;
set export_&year._HCF_:;
where PROVIDER_ID not in ('', '.', '           .');
run;


/* final checks  */
data final_checks00;
set export_&year._HCP_:
	export_&year._HCF_:;
run;

/* any duplicates of key fields? */
proc sort data = final_checks00 nodupkey out = final_checks01 dupout = final_checks02;
by year MEASURE_ID PROVIDER_ROLE_NM PROVIDER_ID;
run;

/* any blanks or key fields? */
data final_checks03;
set final_checks00;
where (year = .) or (MEASURE_ID = '') or (PROVIDER_ROLE_NM = '') or (PROVIDER_ID = '');
run;


/* create control totals dataset */
proc sql;
create table CT_ROW_QM_HCP_&year. as
select	"QM_PRVDR_HCP_&year." as FILENAME,
		count(*) as ROW_COUNT
from &exprtdir..QM_PRVDR_HCP_&year.
group by 1 order by 1;
quit;

proc sql;
create table CT_ROW_QM_HCF_&year. as
select	"QM_PRVDR_HCF_&year." as FILENAME,
		count(*) as ROW_COUNT
from &exprtdir..QM_PRVDR_HCF_&year.
group by 1 order by 1;
quit;

/* final row counts export */
data &exprtdir..CONTROL_ROW_COUNT_QM; 
set CT_ROW_QM_:;
run;

proc sort data = &exprtdir..QM_PRVDR_HCP_&year.;
by YEAR MEASURE_ID;
run;

proc univariate data = &exprtdir..QM_PRVDR_HCP_&year. noprint;
var rate;
by YEAR MEASURE_ID PROVIDER_ROLE_NM;
output out = CT_QC_QM_HCP_&year. N= nobs MEAN= mean PCTLPTS = 0 5 25 50 75 90 100 PCTLPRE = pct_; 
run;

proc sort data = &exprtdir..QM_PRVDR_HCF_&year.;
by YEAR MEASURE_ID;
run;

proc univariate data = &exprtdir..QM_PRVDR_HCF_&year. noprint;
var rate;
by YEAR MEASURE_ID PROVIDER_ROLE_NM;
output out = CT_QC_QM_HCF_&year. N= nobs MEAN= mean PCTLPTS = 0 5 25 50 75 90 100 PCTLPRE = pct_; 
run;

/* control totals with summarized information */
data &exprtdir..CONTROL_METRICS_QC_QM;
set CT_QC_QM_:;
run;

%mend;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

