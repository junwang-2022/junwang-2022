/*********************************************************************
SAS Program Name: Rsubmit_Testing.sas
Author: 	Scott Walters
Date 		6/5/2023
Description: Test rsubmit code 
*********************************************************************/

%let utilspath=&myfiles_root/dua_052882/Sndbx/Scott_W/utils;
%include "&utilspath/read_log2.sas";
%include "&utilspath/Rsubmit_Macros.sas";

libname erdata "&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test" compress=char;
proc datasets lib=erdata nolist;
delete visits_:;
run;
quit;

%macro test_rsub;

	%rsubmit_signon;

	%do x =2018 %to 2021;
		%syslput _USER_/remote=task&x;
		rsubmit task&x wait=no connectpersist=no;
			%include "D:/SASData/dua_052882/Sndbx/Scott_W/VDI/sas_init.sas";
			%let utilspath=&myfiles_root/dua_052882/Sndbx/Scott_W/utils;

			%include "&utilspath/read_log2.sas";
			%include "&utilspath/CCM_Utils.sas";
			%include "&utilspath/Rsubmit_Macros.sas";

			%gbl_ccm_libnames(env=TEST);
			libname erdata "&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test" compress=char;

			%macro er_visits(yr);
				data erdata.visits_&yr;
					set ccmchc.medical_&yr;
					where er_flag=1;
				run;
			%mend er_visits;
			%er_visits(&x);
		endrsubmit;
	%end;
	%rsubmit_signoff;
%mend test_rsub;
%test_rsub;
