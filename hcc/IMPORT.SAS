** Model coefficients **;
FILENAME INC     "D:\SASData\dua_052882\Sndbx\ming\hcc\C0520T4.TRN";
LIBNAME  INCOEF  "D:\SASData\dua_052882\Sndbx\ming\hcc";

proc cimport infile=INC data=INCOEF.Coefficients; run;

** Format library **;
FILENAME INF     "D:\SASData\dua_052882\Sndbx\ming\hcc\CY20F05C.TRN";
LIBNAME LIBRARY  "D:\SASData\dua_052882\Sndbx\ming\hcc";

proc cimport infile=INF library=LIBRARY; run;
