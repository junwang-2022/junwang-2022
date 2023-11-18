/**********************************************************************
				SAS Program Description

Program Name: rsubmit_example.sas
Author: Scott Walters
Date: 6/15/2023

Description: exmple for month based rsubmit
**********************************************************************/
%let utilspath=&myfiles_root/dua_052882/Sndbx/Scott_W/utils;

%include "&utilspath/Rsubmit_Macros.sas";
%include "&utilspath/read_log2.sas";

libname erdata "&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test" compress=yes;

%let logpath=&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test;
%let logname=Rsubmit_Example.log;

%start_log();
proc datasets lib=erdata nolist;
	delete visits_:;
	run;
quit;
*** create list of months to loop through ***;
%rsubmit_month_list(start_month=01jan2020, 
					end_month=01dec2021, 
					session_count=12, 
					outdsn=erdata.month_list, 
					max_rsub_var=rsub_max);

%macro wpr_rsubmit_example;

	%rsubmit_signon;
	/*** create rsub_max # of rsubmit sessions ***/
	%do x = 1 %to &rsub_max ;
		%syslput _USER_/remote=task&x;
		rsubmit task&x wait=no connectpersist=no;
			/*** load needed libraries and code files ***/
			%include "D:/SASData/dua_052882/prod/utils/sas_init.sas";
			%let utilspath=&myfiles_root/dua_052882/Sndbx/Scott_W/utils;
			%include "&utilspath/Rsubmit_Macros.sas";

			libname rawdata "&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test/data";
			libname erdata "&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test" compress=yes;

			%macro rsb_rsubmit_example; 
				/*** make a dataset of all the months assigned to rsbumit x ***/
				%rsubmit_assigned_month(indsn=erdata.month_list, outdsn=rsublist, rid=&x);

				/*** if no months are assigned to an rsubmit, throw an error and exit ***/
				%if &RID_error_flag %then %goto %exit; 
				
				/*** loop through each month assigned to rsubmit x ***/
				%do y = 1 %to &rsub_month_ct;
				
					/*** extract the month, year, date, month_id, session_id, and rsub_id from the current month being processed ***/
					%rsubmit_session_vars(dsn=rsublist, rsub_id=&x, session_id=&y);

					/*** use the month and year values in dataset name references ***/
					data erdata.visits_&rsubyear._&rsubmonth;
						set rawdata.medical_&rsubyear._&rsubmonth;
						where er_flag=1;
					run;
				%end;

				%exit:
			%mend rsb_rsubmit_example;
			%rsb_rsubmit_example;
		
		endrsubmit;
	%end;

	%rsubmit_signoff;
%mend wpr_rsubmit_example;
%wpr_rsubmit_example;

%end_log();
%read_log();
%echo_log();

