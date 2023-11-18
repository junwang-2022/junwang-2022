/*LIBNAME ECI00001 'D:\SASData\SAS_Shared_Data\shared\Edge';*/
/**/
data rarecalmr_2020_diag;
  set eci00001.rarecalmr_2020(rename=(service_cd=hcpcs_cd) obs=1000);
  length diag_cd $20.;
  do i = 1 to countw(diag_cds, '-');
    diag_cd = scan(diag_cds, i, '-');
    output;
  end;
  drop i diag_cds;
  keep sysid claimid start_dt form_type hcpcs_cd diag_cd;
run;

