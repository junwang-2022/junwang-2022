libname qm "D:\SASData\dua_052882\Sndbx\Jun_W\CHC QM\meta\VRDC_25OCT2023";
data qm.def_spec;
set qm.def_spec;
clm_wc1=tranwrd(clm_wc1,'DNL_FLAG_LINE','LINE_DNL_FLAG');
clm_wc2=tranwrd(clm_wc2,'DNL_FLAG_LINE','LINE_DNL_FLAG');
if def_name="CMS_001" and code_type="NDC" then clm_type="RX";
if def_name="CMS_112" and code_type="NDC" then clm_type="RX";
run;
