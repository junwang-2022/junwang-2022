%LET _CLIENTTASKLABEL='na_bene_hcc_outputs_CCM';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='';
%LET _CLIENTPROJECTPATHHOST='';
%LET _CLIENTPROJECTNAME='';
%LET _SASPROGRAMFILE='/sas/vrdc/users/scr269/files/dua_052882/dev/ref/na_bene_hcc_outputs_CCM.sas';
%LET _SASPROGRAMFILEHOST='apcw10sg05.ap.r53.ccwdata.org';

GOPTIONS ACCESSIBLE;
/* prod/ref_tables/na_bene_hcc_outputs.sas */

/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME:  prod/ref_tables/na_bene_hcc.sas
UPDATES: 		S Crow, Jan2023 - updates to how the data is filtered from this document: 
					https://drive.google.com/file/d/1rJJ5M0sWt_-t_-9ZF5ZIIw7tP1pM94Wk/view?usp=share_link
					[Final Industry Memo Medicare Filtering Logic 12 22 15_CPT filter logic.PDF 

Source: https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/Downloads/mc86c07.pdf

PURPOSE:      Create an annualized output file that has all the outputs produced by the CMS HCC code 
				as well as average HCC for fully enrolled benes

OVERVIEW: 	  1) Get Bene info
			  2) Get Eligible Diag codes 
			  3) Calculate HCC score
			  4) Grab fully enrolled Benes
			  5) Calculate average HCC score

INPUT DATA: 
	Only inputs are from CMS Supplied files
	HCC SAS Packages

OUTPUT FILES: 
	&finaldir..ref_hcc_bene_inputs
	&finaldir..na_bene_hcc_outputs 
	&finaldir..REF_CJ_HCC_NORMALIZATION_FACTOR

NOTES FROM RESDAC:	This variable serves as the optional third component of bill type. Many different 
		types of services can be appear on an encounter institutional claim, and knowing the type of 
		bill helps to distinguish them. The type of bill is the concatenation of three variables: the 
		facility type (CLM_FAC_TYPE_CD), the service classification type code (CLM_SRVC_CLSFCTN_TYPE_CD), 
		and the claim frequency code (CLM_FREQ_CD).

************************************************************************************************************/
options fullstimer;

libname TEST "/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Sam_C/TEST/";

%include "&myfiles_root/dua_052882/prod/utils/CCM_Macros.sas";
%gbl_ccm_libnames(PROD);

%let finaldir = TEST;
%let year = 2021;
%let last_year = %sysevalf(&year.-1);

/*Macro to get the bene info from the current year MBSF*/
%macro patient(); 

/* retain first date of dual eligibility */
data dual;
set ccmelig.dual;
by bene_id;

if first.bene_id then orig_dt = start_dt;
retain orig_dt;

format orig_dt mmddyy10.;
run;

/* dedup to only have one line per bene */
proc sort data = dual nodupkey out = dual_&year.;
where year(start_dt) <= &year. and year(end_dt) >= &year.;
by bene_id;
run;

/* create eligibility input for HCC Package */
PROC SQL;					
CREATE TABLE INP as 
SELECT a.BENE_ID, 
       bene_gender AS sex, 
	   &year. as year,
       bene_dob_dt AS dob, 
	   . as LTIMCAID,
/*       case when b.start_dt ~= . then 1 else 0 end as MCAID, */
       case when b.start_dt ~= . and year(b.orig_dt) =&year. then 1 else 0 end as NEMCAID,
	   orec

FROM (select * from ccmelig.enrollment_&year. where enroll_type ~= 'MA') a /* does this need to be part a AND b coverage? */
left join dual_&year. b 
on a.bene_id = b.bene_id
; 
quit; 

/* make sure there are no duplicates for enrollment type */
proc sort data = INP nodupkey out = INP_&year. dupout = checks00; 
by bene_id;
run;

	%if  %sysfunc(exist(&finaldir..ref_hcc_bene_inputs)) %then %do;
		data bene_temp;
		set &finaldir..ref_hcc_bene_inputs;
		where year ~= &year. and year ~= . ;
		run;

		data &finaldir..ref_hcc_bene_inputs;
		set bene_temp INP_&year. ;
		run;
	%end;
		

	/* if table doesnt exist, initialize table and add data to it*/

	%else %do;
		data &finaldir..ref_hcc_bene_inputs;
			set INP_&year.  ;
		run;
	%end;


%mend;

/*Macro to get diag codes that are eligible from PREVIOUS year*/
%macro get_hcc_diagnosis_data_set(year, months); 

/* Used before to pull input diagnosis codes but no longer necessary */
/*%let firstm = %scan(&months.,1 , %str( ));*/
/*%let lastm = %scan(&months.,%sysfunc(countw(&months.)) , %str( ));*/
/*%include "/sas/vrdc/users/&sysuserid./files/dua_052882/prod/utils/generate_configuration.sas";*/
/*%generate_configuration(year=&last_year.,type=.)*/


/* filter data down to IP & OP diagnosis codes, merge to medical to filter for bill type */
data diag_ip_op_&last_year.;
set ccmffs.diagnosis_&last_year._01 - ccmffs.diagnosis_&last_year._12 (keep=bene_id DX_CD CLM_TYPE CLM_ID);
where CLM_TYPE in ('IP' 'OP');
rename DX_CD = diag;
run;

/* add filtering here, not in merge */
data bill_type_ip_op_&last_year.;
set ccmffs.medical_&last_year._01 - ccmffs.medical_&last_year._12 (keep=bene_id BILL_TYPE FREQ_CD CLM_TYPE CLM_ID);
where CLM_TYPE in ('IP' 'OP');

if clm_type = 'IP' then do;
	if (BILL_TYPE = '11' and FREQ_CD not in ('8')) or
		(BILL_TYPE = '41');
end;

else if clm_type = 'OP' then do;
	if  (BILL_TYPE = '12' and FREQ_CD not in ('8')) or 
		(BILL_TYPE = '13') or
		(BILL_TYPE = '43') or
		(BILL_TYPE = '71') or
		(BILL_TYPE = '73') or
		(BILL_TYPE = '76') or
		(BILL_TYPE = '77') or
		(BILL_TYPE = '85');
end;

run;

proc sort data = diag_ip_op_&last_year.;
by bene_id clm_id;
run;

proc sort data = bill_type_ip_op_&last_year.;
by bene_id clm_id;
run;

/* get list of claim ids with an inclusion procedure code */
data ip_op_&last_year.;
merge 	diag_ip_op_&last_year. (in = a)
		bill_type_ip_op_&last_year. (in = b);
by bene_id clm_id;
if a = 1 and b = 1;
run;

/* separate out IP & OP to remove PROC CODE filter from OP */

data ip_&last_year.;
set ip_op_&last_year.;
where CLM_TYPE = 'IP';
run;

data op_diag_&last_year.;
set ip_op_&last_year.;
where CLM_TYPE = 'OP';
run;

/* get list of procedure codes for carrier data to filter correct diagnosis codes  */
data cpt_op_&last_year.;
set ccmffs.serviceline_&last_year._01 - ccmffs.serviceline_&last_year._12 (keep=bene_id CPT CLM_TYPE CLM_ID);
where CLM_TYPE in ('OP');
run;

proc sort data = cpt_op_&last_year.;
by CPT;
run;

proc sort data = sh052882.OD_HCC_CPT_INCL out = OD_HCC_CPT_INCL;
where year = &last_year.; 
by proc_code;
run;

/* get list of claim ids with an inclusion procedure code */
data op_IDS_&last_year.;
merge 	cpt_op_&last_year. (in = a)
		OD_HCC_CPT_INCL (in = b rename = (proc_code = CPT));
by CPT;
if a = 1 and b = 1;
run;

/* get unique list of claim ids */
proc sort data = op_IDS_&last_year. nodupkey out = op_IDS00_&last_year. (keep = bene_id clm_id);
by bene_id clm_id;
run;

proc sort data = op_diag_&last_year.;
by bene_id clm_id;
run;

/* merge claim ids back onto list of diagnosis codes - we want to include all diagnosis codes
	that had an inclusion procedure code on ANY line of the claim */
data op_&last_year.;
merge 	op_diag_&last_year. (in = a)
		op_IDS00_&last_year. (in = b);
by bene_id clm_id;
if a = 1 and b = 1;
drop clm_id;
run;


/* filter data down to Caqrrier diagnosis codes, merge to medical to filter for bill type */
data diag_phys_&last_year.;
set ccmffs.diagnosis_&last_year._01 - ccmffs.diagnosis_&last_year._12 (keep=bene_id DX_CD CLM_TYPE CLM_ID);
where CLM_TYPE in ('PHYS');
rename DX_CD = diag;
run;

/* get list of procedure codes for carrier data to filter correct diagnosis codes  */
data cpt_phys_&last_year.;
set ccmffs.serviceline_&last_year._01 - ccmffs.serviceline_&last_year._12 (keep=bene_id CPT CLM_TYPE CLM_ID);
where CLM_TYPE in ('PHYS');
run;

proc sort data = cpt_phys_&last_year.;
by CPT;
run;

/* get list of claim ids with an inclusion procedure code */
data phys_IDS_&last_year.;
merge 	cpt_phys_&last_year. (in = a)
		OD_HCC_CPT_INCL (in = b rename = (proc_code = CPT));
by CPT;
if a = 1 and b = 1;
run;

/* get unique list of claim ids */
proc sort data = phys_IDS_&last_year. nodupkey out = phys_IDS00_&last_year. (keep = bene_id clm_id);
by bene_id clm_id;
run;

proc sort data = diag_phys_&last_year.;
by bene_id clm_id;
run;

/* merge claim ids back onto list of diagnosis codes - we want to include all diagnosis codes
	that had an inclusion procedure code on ANY line of the claim */
data phys_&last_year.;
merge 	diag_phys_&last_year. (in = a)
		phys_IDS00_&last_year. (in = b);
by bene_id clm_id;
if a = 1 and b = 1;
drop clm_id;
run;

data diag;
set phys_&last_year. ip_&last_year. op_&last_year.;
run;

/* Get rid of dup diag codes*/
proc sort data= diag nodupkey out = diag_&year. (keep = bene_id diag);
by bene_id diag; 
run;

/* 2016 uses 2015 diags which have both ICD 9 and 10, which need to be differentiated for the macro*/
%if &year = 2016 %then %do;
proc sql;
create table diag_&year. as 
select a.bene_id, a.diag, case when b.diag = '' then '9' else '0' end as diag_type
from diag_&year. a
left join sh052882.ref_icd_10_cds b 
on a.diag=b.diag
order by 1,2
;quit;
%end;
%MEND;

/* Calculate HCC's */
%macro avg_hcc(year);

%include "/sas/vrdc/users/&sysuserid/files/dua_052882/prod/utils/generate_configuration.sas";
%let last_year = %sysevalf(&year.-1);
%generate_configuration(year=&last_year, type=.);
%let mbsf_prev = &mbsf_file.;

%generate_configuration(year=&year, type='hcc');

/* Run HCC's for current year - macro is declared in CMS HCC program found in gen config */
%run_hcc(&year.);

/* dedup mcare status dataset before joining */
proc sort data = ccmelig.mdcr_stus_&last_year. out = mdcr_stus;
by bene_id decending s_dt;
run;

proc sort data = mdcr_stus nodupkey out = mdcr_stus_&last_year.;
by bene_id;
run;

/*Get full enrolled for last year population for avg hcc calc*/
proc sql;  	
create table full_enrolled as
select   a.*
		,b.mdcr_status_code
		,c.enroll_type as mcaid_type
from (select * from ccmelig.enrollment_&last_year. 
		where month(s_dt) = 1 and month(e_dt) = 12 and enroll_type = 'FFS_AB') a
left join mdcr_stus_&last_year. b
on a.bene_id = b.bene_id
left join dual_&year. c
on a.bene_id = c.bene_id
;
quit;

proc sort data = full_enrolled nodupkey out = full_enrolled_&last_year.;
by bene_id;
run;

/*HCC models changed after 2016 from 1 community score to 4 community scores, so calcs need to be broken out*/
%if &year. <= 2016 %then %do;
proc sql;
create table person_&year. as 
select a.*,&year. as year, case when b.bene_id = . then SCORE_NEW_ENROLLEE else score_community end as current_score
from person_&year. a
left join full_enrolled_&last_year. b 
on a.bene_id = b.bene_id
;quit;
%end;

%else %if  &year. > 2016 %then %do;
proc sql;
create table person_&year. as 
select a.*,&year. as year,
case when b.bene_id = . then SCORE_NEW_ENROLLEE 
	 when mdcr_status_code in ('11' '21' '31') then score_community_na
     when mdcr_status_code = '20' and mcaid_type ~= '' then score_community_fbd
	 when mdcr_status_code = '20' and mcaid_type = '' then score_community_nd
     when mdcr_status_code = '10' and mcaid_type ~= '' then score_community_fba
     when mdcr_status_code = '10' and mcaid_type = '' then score_community_na 
	 else score_community_na end
as current_score
from person_&year. a
left join full_enrolled_&last_year. b 
on a.bene_id = b.bene_id
;quit;
%end;


	%if  %sysfunc(exist(&finaldir..na_bene_hcc_outputs)) %then %do;
		data person_temp;
		set &finaldir..na_bene_hcc_outputs;
		where year ~= &year. and year ~= . ;
		run;

		data &finaldir..na_bene_hcc_outputs;
		set person_temp person_&year.;
		run;
	%end;
		

	/* if table doesnt exist, initialize table and add data to it*/

	%else %do;
		data &finaldir..na_bene_hcc_outputs;
			set person_&year. ;
		run;
	%end;

/* Calculate average score for people who were fully enrolled in the previous year */
proc sql;
create table avg_&year. as
select avg(current_score) as norm_factor, &year. as year
from (select bene_id, current_score from person_&year.) a
inner join (select bene_id from full_enrolled_&last_year.) b
on a.bene_id = b.bene_id
;quit;

	%if  %sysfunc(exist(&finaldir..REF_CJ_HCC_NORMALIZATION_FACTOR)) %then %do;
		data norm_temp;
		set &finaldir..REF_CJ_HCC_NORMALIZATION_FACTOR;
		where year ~= &year. and year ~= . ;
		run;

		data &finaldir..REF_CJ_HCC_NORMALIZATION_FACTOR;
		set norm_temp avg_&year.;
		run;
	%end;
		

	/* if table doesnt exist, initialize table and add data to it*/

	%else %do;
		data &finaldir..REF_CJ_HCC_NORMALIZATION_FACTOR;
			set avg_&year. ;
		run;
	%end;
%mend;


%macro run_hccs(year, tag, months);
%include "/sas/vrdc/users/&sysuserid/files/dua_052882/prod/utils/generate_configuration.sas";
%let last_year = %sysevalf(&year.-1);
%generate_configuration(year=&year, type='hcc', version = 'v22'); /* SSC - added version */
%patient();
%get_hcc_diagnosis_data_set(year=&year., months=&months.);
%avg_hcc(year=&year.);

/*proc datasets lib=work kill;*/
/*run;*/
%mend;

/*%run_hccs(year=2011, tag=2011, months = 01 12);*/
/*%run_hccs(year=2012, tag=2012, months = 01 12);*/
/*%run_hccs(year=2013, tag=2013, months = 01 12);*/
/*%run_hccs(year=2014, tag=2014, months = 01 12);*/
/*%run_hccs(year=2015, tag=2015, months = 01 12);*/
/*%run_hccs(year=2016, tag=2016, months = 01 12);*/
/*%run_hccs(year=2017, tag=2017, months = 01 12);*/
/*%run_hccs(year=2018, tag=2018, months = 01 12);*/
/*%run_hccs(year=2019, tag=2019, months = 01 12);*/
/*%run_hccs(year=2020, tag=2020, months = 01 12);*/
%run_hccs(year=2021, tag=2021, months = 01 12);



GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

