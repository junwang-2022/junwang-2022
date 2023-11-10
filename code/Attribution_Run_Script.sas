%LET _CLIENTTASKLABEL='Attribution_Run_Script';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Builder_For_Testing.egp';
%LET _CLIENTPROJECTPATHHOST='apcw10sg08.ap.r53.ccwdata.org';
%LET _CLIENTPROJECTNAME='Builder_For_Testing.egp';
%LET _SASPROGRAMFILE='/sas/vrdc/users/scr269/files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Attribution_Run_Script.sas';
%LET _SASPROGRAMFILEHOST='apcw10sg08.ap.r53.ccwdata.org';

GOPTIONS ACCESSIBLE;
/****************************************************************************
SAS PROGRAM DESCRIPTION
		
PROGRAM NAME: Attribution_Run_Script.sas
PROGRAM OWNER: Samantha Crow
PROGRAM CREATED:  June 2023
PROGRAM MODIFIED: n/a

PURPOSE: macro execution for attribution and roll ups for quality measures

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

/**********************************************************************************/
/* track log, create a copy */
/**********************************************************************************/
%include "/sas/vrdc/users/&sysuserid./files/dua_052882/prod/utils/read_log2.sas";

/**********************************************************************************/
*** macro variables logpath and logname are default for these macros.
	if you create macro variables with these names, you will not need to 
	pass them as arguments to the macros.;
/**********************************************************************************/
%let logpath=/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Output/attribution_logs;
%let logname=attribution_log_%fnc_log_dt.log;
%PUT logname=&logname;

/**********************************************************************************/
*** start a log and timer using the default macro variables;
/**********************************************************************************/
%start_log();

 
/**********************************************************************************/
/* include attribution_macros, config */
/**********************************************************************************/
%include "/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Attribution_Config.sas";
%include "/sas/vrdc/users/&sysuserid./files/dua_052882/Sndbx/Sam_C/Builder_Test/V9/Attribution_Macros.sas";


/**********************************************************************************/
/* run attribution logic for measures and additonal logic required episode builder outputs */
/**********************************************************************************/

/*%roll_up(CMS_001,QM1,TYPE1);*/
/*%roll_up(CMS_112,QM1,TYPE1);*/
/*%roll_up(CMS_113,QM1,TYPE1);*/
/*%roll_up(CMS_134,QM1,TYPE1);*/
/*%roll_up(CMS_128,QM1,TYPE1);*/
/*%roll_up(CMS_141,QM1,TYPE1);*/
/*%roll_up(CMS_226_1,QM1,TYPE1);*/
/*%roll_up(CMS_226_2,QM1,TYPE1);*/
/*%roll_up(CMS_236,QM1,TYPE1); */
/*%roll_up(CMS_317,QM1,TYPE1);*/
/*%roll_up(CMS_422,QM1,TYPE1);*/
/*%roll_up(GAM_08,QM1,TYPE1);*/
/*%roll_up(GAM_32,QM1,TYPE1);*/
/*%roll_up(GAM_05,QM1,TYPE1);*/
/*%roll_up(OP_32,QM2,TYPE1); */
/*%roll_up(OP_36,QM2,TYPE1); */
/*%roll_up(OP_35,QM2,TYPE1);*/
/*%roll_up(GAM_07,QM1,TYPE2);*/
/*%roll_up(GAM_09,QM1,TYPE2);*/
/*%roll_up(GAM_127,QM_B2,TYPE1);*/
/*%roll_up(GAM_69,QM_B2,TYPE1);*/
/*%roll_up(GAM_11,QM_B2,TYPE1);*/
/*%roll_up(GAM_06,QM_B2,TYPE1);*/
/*%roll_up(GAM_71,QM_B2,TYPE1);*/
/*%roll_up(GAM_34,QM_B2,TYPE1);*/
/*%roll_up(GAM_108,QM_B2,TYPE2);*/
/*%roll_up(GAM_102,QM_B2,TYPE3);*/
/*%roll_up(GAM_150,QM_B2,TYPE1);*/
/*%roll_up(GAM_133,QM_B2,TYPE2);*/
/*%gam_100_roll_up(GAM_100,QM_B2,TYPE1);*/


/**********************************************************************************/
/* create exports */
/**********************************************************************************/

%create_exports(CMS_001_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_112_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_113_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_128_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_134_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_141_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_226_1_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_226_2_NPI,NPI,Physician NPI,HCP);
%create_exports(CMS_317_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_05_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_05_ASC,ASC,Facility Billing NPI,HCF);
%create_exports(GAM_05_FAC,FAC,Facility CCN,HCF);
%create_exports(OP_32_NPI,NPI,Physician NPI,HCP);
%create_exports(OP_32_FAC,FAC,Facility CCN,HCF);
%create_exports(OP_35_NPI,NPI,Physician NPI,HCP);
%create_exports(OP_35_ASC,ASC,Facility Billing NPI,HCF);
%create_exports(OP_35_FAC,FAC,Facility CCN,HCF);
%create_exports(OP_36_NPI,NPI,Physician NPI,HCP);
%create_exports(OP_36_FAC,FAC,Facility CCN,HCF);
%create_exports(GAM_127_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_127_ASC,ASC,Facility Billing NPI,HCF);
%create_exports(GAM_127_FAC,FAC,Facility CCN,HCF);
%create_exports(GAM_108_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_102_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_133_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_100_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_150_NPI,NPI,Physician NPI,HCP);
%create_exports(GAM_150_ASC,ASC,Facility Billing NPI,HCF);
%create_exports(GAM_150_FAC,FAC,Facility CCN,HCF);

%combine_exports();


/**********************************************************************************/
*** end the timer and write the log.;
/**********************************************************************************/
%end_log();

/**********************************************************************************/
*** check the log for errors and notes/warnings that may need to be treated as errors. save to a dataset.;
/**********************************************************************************/
%read_log();

/**********************************************************************************/
*** echo the contents of the log back to the SAS Session;
/**********************************************************************************/
%echo_log();


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

