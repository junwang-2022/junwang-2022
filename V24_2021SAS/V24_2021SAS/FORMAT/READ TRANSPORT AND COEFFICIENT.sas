filename inc "D:\Bundle Engine\PATIENT CASE MIX ADJUSTERS\V24_2021SAS\FORMAT\C2419P1M";
libname incoef "D:\Bundle Engine\PATIENT CASE MIX ADJUSTERS\V24_2021SAS\FORMAT";
proc cimport data=incoef.hcccoefn infile=inc;
run;

filename inf "D:\Bundle Engine\PATIENT CASE MIX ADJUSTERS\V24_2021SAS\FORMAT\F2421P1M";
libname library "D:\Bundle Engine\PATIENT CASE MIX ADJUSTERS\V24_2021SAS\FORMAT";
proc cimport library=library infile=inf;
run;
