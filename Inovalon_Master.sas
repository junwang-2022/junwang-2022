%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\sas_init.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\snowflake_connection.sas";
%let samplesuffix=_250k;
%let exportfolder=t:\inovalon\full;
libname out "&ExportFolder";


%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_enrollment_member.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_enrollment_record.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_ccd.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_clm.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_member.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_ipps.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_nonipps.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_xref.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_prv.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Medical_psp.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_Member.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_RXC.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_RXCC.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_RXCW.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_PRV.sas";
%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_PSP.sas";

%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\Inovalon\Inovalon_Pharmacy_indices.sas";
