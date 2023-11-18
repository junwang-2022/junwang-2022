/*Only ICD-10-CM diagnosis codes from sources allowable for risk adjustment should be*/
/*included in the DIAGNOSIS data set. ICD-10 codes that are not listed in Table 3 may*/
/*be included in the DIAGNOSIS data set but are ignored by the software. The steps*/
/*below provide logic to determine which diagnoses are allowable. Note that Steps 1*/
/*and 3 refer to Table 2, CPT/HCPCS Included List for Diagnosis Code Filtering, which*/
/*provides the 2019 (for historical data purposes) and 2020 CPT/HCPCS codes used to*/
/*define service or procedure types that are acceptable sources of diagnoses for risk*/
/*adjustment.  */

/*•	The CPT/HCPCS codes identifying services with diagnoses allowable for risk */
/*adjustment are listed in column A of Table 2.  */
/**/
/*•	Column B lists the short descriptions of the CPT/HCPCS codes.  */
/**/
/*•	Columns C and D, respectively, indicate whether a CPT/HCPCS code is acceptable */
/*in 2019 or 2020.  */
/**/
/*•	Column E identifies applicable footnotes on the CPT/HCPCS codes.  */
/**/
/*•	Notes begin on row 6,596 of the Excel table with the line “Notes:” and should*/
/*not be imported by any program.*/

/*The DIAGNOSIS data set should include diagnoses from claims/encounter records*/
/*with discharge dates or through dates within the benefit year. Though the term*/
/*“claim” is used in the steps below, the steps apply equally to encounter records.*/
/*For the EDGE server, only claims with discharge diagnoses are used for HHS risk*/
/*adjustment.*/



/*1.	Professional source of diagnosis*/
/**/
/*a.	For professional records, use diagnoses from records that have at least one line item*/
/*with an acceptable CPT/HCPCS code (Table 2). If there is at least one acceptable line on*/
/*the record, use all the header diagnoses. There are three possible values for CPT/HCPCS*/
/*codes in columns C and D:*/
/**/
/*i.	yes = code is acceptable in that calendar year*/
/**/
/*ii.	no = code is not acceptable in that calendar year*/
/**/
/*iii.	N/A = code is not in existence in that calendar year*/

LIBNAME ECI00001 'D:\SASData\SAS_Shared_Data\shared\Edge';

/*data rarecalmr_2020_diag;*/
/*  set eci00001.rarecalmr_2020(rename=(service_cd=hcpcs_cd));*/
/*  length diag_cd $20.;*/
/*  do i = 1 to countw(diag_cds, '-');*/
/*    diag_cd = scan(diag_cds, i, '-');*/
/*    output;*/
/*  end;*/
/*  drop i diag_cds;*/
/*  keep sysid start_dt form_type bill_type hcpcs_cd diag_cd;*/
/*run;*/

/*proc sort data=rarecalmr_2020_diag;*/
/*  by hcpcs_cd;*/
/*run;*/

proc sort data=eci00001.hhs_hcc_table2;
  by hcpcs_cd;
run;

data eci00001.diagnosis_eligible;
  merge rarecalmr_2020_diag(in=d)
	    eci00001.hhs_hcc_table2(in=l);
  by hcpcs_cd;
  if form_type = 'I' and bill_type in ('111', '117') then eligible = 'ie';
  else if form_type = 'I' and bill_type not in ('111', '117') then eligible = 'in';
  else if form_type = 'I'
	and bill_type in ('131', '137', '711', '717', '761', '767', '771', '777', '851', '857')
	and d and l then eligible = 'oe';
  else if form_type = 'I'
	and bill_type in ('131', '137', '711', '717', '761', '767', '771', '777', '851', '857')
	and d and not l then eligible = 'on';
  else if form_type = 'P' and d and l then eligible = 'pe';
  else if form_type = 'P' and d and not l then eligible = 'pn';
  else eligible = 'xx';

  diagnosis_service_date = input(put(start_dt, yymmddn8.), 8.);
  format diagnosis_service_date 8.;

  if eligible in ('ie', 'oe', 'pe') ;
keep sysid diagnosis_service_date form_type bill_type hcpcs_cd diag_cd eligible;
run;

data eci00001.diagnosis_20230516;
set eci00001.diagnosis_eligible(rename=(diag_cd=diag));
keep sysid diag diagnosis_service_date;
run;

proc sort data=eci00001.diagnosis_20230516 out=eci00001.diagnosis_20230516_dd nodupkey;;
   by sysid diag diagnosis_service_date;
run;

/*b.	For professional records, if a line item has an acceptable CPT/HCPCS code,*/
/*use all diagnoses from the line item. */
/**/
/*c.	If there are no acceptable service lines on the record, do not use any of*/
/*the diagnoses for risk adjustment.*/

/*2.	Inpatient facility source of diagnosis*/
/**/
/*a.	Use all header diagnoses from records where facility bill type code equals one of the following:  */
/**/
/*i.	111 (inpatient admit through discharge); or */
/**/
/*ii.	117 (inpatient replacement of prior claim). */
/**/
/*b.	There is no procedure screen for inpatient facility record types.*/
/*3.	Outpatient facility source of diagnosis*/
/**/
/*a.	Restrict records to those with facility bill type code equal to:*/
/**/
/*i.	131 (hospital outpatient admit through discharge); or*/
/**/
/*ii.	137 (hospital outpatient replacement of prior claim); or*/
/**/
/*iii.	711 (rural health clinic admit through discharge); or*/
/**/
/*iv.	717 (rural health clinic replacement of prior claim); or*/
/**/
/*v.	761 (community mental health center admit through discharge); or*/
/**/
/*vi.	767 (community mental health center replacement of prior claim); or*/
/**/
/*vii.	771 (federally qualified health center admit through discharge); or*/
/**/
/*viii.	777 (federally qualified health center replacement of prior claim).*/
/**/
/*ix.	851 (critical access hospital admit through discharge); or*/
/**/
/*x.	857 (critical access hospital replacement of prior claim).*/
/**/
/*b.	For records with at least one acceptable CPT/HCPCS code (Table 2) on a service line,*/
/*use all header diagnoses. Otherwise, do not use the diagnoses for risk adjustment.*/
/**/
