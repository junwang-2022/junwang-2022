%LET _CLIENTTASKLABEL='Attribution_Config';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Builder_For_Testing.egp';
%LET _CLIENTPROJECTPATHHOST='apcw10sg08.ap.r53.ccwdata.org';
%LET _CLIENTPROJECTNAME='Builder_For_Testing.egp';
%LET _SASPROGRAMFILE='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Attribution_Config.sas';
%LET _SASPROGRAMFILEHOST='apcw10sg08.ap.r53.ccwdata.org';

GOPTIONS ACCESSIBLE;
/****************************************************************************
SAS PROGRAM DESCRIPTION
		
PROGRAM NAME: Attribution_Config.sas
PROGRAM OWNER: Samantha Crow
PROGRAM CREATED:  June 2023
PROGRAM MODIFIED: n/a

PURPOSE: Configuration for attribution and roll ups for quality measures

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

%let year = 2022;
%let yr = 22;
%let months = 01 02 03 04 05 06 07 08 09 10 11 12;
%let finaldir = pl052882;
%let exprtdir = pl052882;


/* import metadata/thresholds for quality measures */
data WORK.THRESHOLDS ;
infile "/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Meta/Meta_RAW/thresholds.csv"	delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
 informat DEF_SRC $10. ;
 informat DEF_TYPE $10. ;
 informat DEF_NAME $20. ;
 informat DEF_DESC $250. ;
 informat DENOM_THRESHOLD best32.;
 informat QUALITY_INDICATOR $20. ;
 format DEF_SRC $10. ;
 format DEF_TYPE $10. ;
 format DEF_NAME $20. ;
 format DEF_DESC $250. ;
 format DENOM_THRESHOLD best12. ;
 format QUALITY_INDICATOR $20. ;
input
          DEF_SRC  $
          DEF_TYPE  $
          DEF_NAME  $
		  DEF_DESC  $
		  DENOM_THRESHOLD 
          QUALITY_INDICATOR  $
;
run;
 


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

