libname rsubdata "&myfiles_root/dua_052882/Sndbx/Scott_W/rsubmit_test/data";
%let utilspath=&myfiles_root/dua_052882/Sndbx/Scott_W/utils;
%include "&utilspath/CCM_Utils.sas";
%gbl_ccm_libnames(env=TEST);


data rsubdata.medical_2021_01
rsubdata.medical_2021_02
rsubdata.medical_2021_03
rsubdata.medical_2021_04
rsubdata.medical_2021_05
rsubdata.medical_2021_06
rsubdata.medical_2021_07
rsubdata.medical_2021_08
rsubdata.medical_2021_09
rsubdata.medical_2021_10
rsubdata.medical_2021_11
rsubdata.medical_2021_12;

set ccmchc.medical_2021;

if intnx('month',thru_dt,0,'B')='01jan2021'd then output rsubdata.medical_2021_01;
else if intnx('month',thru_dt,0,'B')='01feb2021'd then output rsubdata.medical_2021_02;
else if intnx('month',thru_dt,0,'B')='01mar2021'd then output rsubdata.medical_2021_03;
else if intnx('month',thru_dt,0,'B')='01apr2021'd then output rsubdata.medical_2021_04;
else if intnx('month',thru_dt,0,'B')='01may2021'd then output rsubdata.medical_2021_05;
else if intnx('month',thru_dt,0,'B')='01jun2021'd then output rsubdata.medical_2021_06;
else if intnx('month',thru_dt,0,'B')='01jul2021'd then output rsubdata.medical_2021_07;
else if intnx('month',thru_dt,0,'B')='01aug2021'd then output rsubdata.medical_2021_08;
else if intnx('month',thru_dt,0,'B')='01sep2021'd then output rsubdata.medical_2021_09;
else if intnx('month',thru_dt,0,'B')='01oct2021'd then output rsubdata.medical_2021_10;
else if intnx('month',thru_dt,0,'B')='01nov2021'd then output rsubdata.medical_2021_11;
else if intnx('month',thru_dt,0,'B')='01dec2021'd then output rsubdata.medical_2021_12;
RUN;

data rsubdata.medical_2020_01
rsubdata.medical_2020_02
rsubdata.medical_2020_03
rsubdata.medical_2020_04
rsubdata.medical_2020_05
rsubdata.medical_2020_06
rsubdata.medical_2020_07
rsubdata.medical_2020_08
rsubdata.medical_2020_09
rsubdata.medical_2020_10
rsubdata.medical_2020_11
rsubdata.medical_2020_12;

set ccmchc.medical_2020;

if intnx('month',thru_dt,0,'B')='01jan2020'd then output rsubdata.medical_2020_01;
else if intnx('month',thru_dt,0,'B')='01feb2020'd then output rsubdata.medical_2020_02;
else if intnx('month',thru_dt,0,'B')='01mar2020'd then output rsubdata.medical_2020_03;
else if intnx('month',thru_dt,0,'B')='01apr2020'd then output rsubdata.medical_2020_04;
else if intnx('month',thru_dt,0,'B')='01may2020'd then output rsubdata.medical_2020_05;
else if intnx('month',thru_dt,0,'B')='01jun2020'd then output rsubdata.medical_2020_06;
else if intnx('month',thru_dt,0,'B')='01jul2020'd then output rsubdata.medical_2020_07;
else if intnx('month',thru_dt,0,'B')='01aug2020'd then output rsubdata.medical_2020_08;
else if intnx('month',thru_dt,0,'B')='01sep2020'd then output rsubdata.medical_2020_09;
else if intnx('month',thru_dt,0,'B')='01oct2020'd then output rsubdata.medical_2020_10;
else if intnx('month',thru_dt,0,'B')='01nov2020'd then output rsubdata.medical_2020_11;
else if intnx('month',thru_dt,0,'B')='01dec2020'd then output rsubdata.medical_2020_12;
RUN;
