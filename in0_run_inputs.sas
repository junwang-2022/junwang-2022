%include "D:\SASData\dua_052882\Sndbx\Michelle_V\in1_person.sas";
%include "D:\SASData\dua_052882\Sndbx\Michelle_V\in2_diag.sas";
%include "D:\SASData\dua_052882\Sndbx\Michelle_V\in3_ndc.sas";
%include "D:\SASData\dua_052882\Sndbx\Michelle_V\in4_hcpcs.sas";

*for 2017 there is no NDC and HCPCS files; 

%let year = 2017;

%person(&year.);
%diagnosis(&year.);
*%ndc(&year.);
*%hcpcs(&year.);

proc contents data=data.person;
run;

proc contents data=data.diagnosis;
run;
/**/
/*proc contents data=data.hcpcs;*/
/*run;*/
/**/
/*proc contents data=data.ndc;*/
/*run;*/
