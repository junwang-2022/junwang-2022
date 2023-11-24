libname a "D:\SASData\dua_052882\Sndbx\Chris F\ad_hoc\builder_meta";
proc print data=a.provider_x_cbsa_2019; where provider_id="1558466516";run;
proc print data=a.provider_x_cbsa_2020; where provider_id="1558466516";run;
proc print data=a.provider_x_cbsa_2021; where provider_id="1558466516";run;
proc print data=a.provider_x_cbsa_2022; where provider_id="1558466516";run;

proc print data=a.npi_x_spec_2019; where npi in ("1558466516" "1407866627");run;
proc print data=a.npi_x_spec_2020; where npi in ("1558466516" "1407866627");run;
proc print data=a.npi_x_spec_2021; where npi in ("1558466516" "1407866627");run;
proc print data=a.npi_x_spec_2022; where npi in ("1558466516" "1407866627");run;



DATA WORK.NPI_NO_CBSA;
    LENGTH
        PROVIDER_ID      $ 10
        DEACTIVATION_YEAR   8
        STATE            $ 2 ;
    FORMAT
        PROVIDER_ID      $CHAR10.
        DEACTIVATION_YEAR BEST4.
        STATE            $CHAR2. ;
    INFORMAT
        PROVIDER_ID      $CHAR10.
        DEACTIVATION_YEAR BEST4.
        STATE            $CHAR2. ;
    INFILE 'W:\SASWork\_TD28128_SAS-PROD-1_\#LN00119'
        LRECL=18
        ENCODING="WLATIN1"
        TERMSTR=CRLF
        DLM='7F'x
        MISSOVER
        DSD ;
    INPUT
        PROVIDER_ID      : $CHAR10.
        DEACTIVATION_YEAR : BEST4.
        STATE            : $CHAR2. ;
RUN;

proc sql;
create table npi as
select a.*, 
a.provider_id=b1.npi as in_2019, a.provider_id=b2.npi as in_2020, a.provider_id=b3.npi as in_2021, a.provider_id=b4.npi as in_2022,
a.provider_id=c1.provider_id as cbsa_2019, a.provider_id=c2.provider_id as cbsa_2020, a.provider_id=c3.provider_id as cbsa_2021, a.provider_id=c4.provider_id as cbsa_2022,
b4.*,c4.*
from NPI_NO_CBSA a
left join a.npi_x_spec_2019 b1 on a.provider_id=b1.npi
left join a.npi_x_spec_2020 b2 on a.provider_id=b2.npi
left join a.npi_x_spec_2021 b3 on a.provider_id=b3.npi
left join a.npi_x_spec_2022 b4 on a.provider_id=b4.npi
left join a.provider_x_cbsa_2019 c1 on a.provider_id=c1.provider_id and c1.provider_type="phys_npi"
left join a.provider_x_cbsa_2020 c2 on a.provider_id=c2.provider_id and c2.provider_type="phys_npi"
left join a.provider_x_cbsa_2021 c3 on a.provider_id=c3.provider_id and c3.provider_type="phys_npi"
left join a.provider_x_cbsa_2022 c4 on a.provider_id=c4.provider_id and c4.provider_type="phys_npi"
order by provider_id;
quit;

proc sql;
create table cbsa_21_22 as 
select a.*, b.provider_id as in_2022, c.provider_id as missing_cbsa, c.deactivation_year, c.state as nppes_st
from a.provider_x_cbsa_2021(where=(provider_type="phys_npi")) a
left join a.provider_x_cbsa_2022 b on b.provider_type="phys_npi" and a.provider_id=b.provider_id
left join npi_no_cbsa c on a.provider_id=c.provider_id;
quit;

proc sql;
select distinct missing_cbsa^="" as missing_cbsa, .<deactivation_year<2023 as deactivate, in_2022^="" as in_2022,  count(*) as cnt, mean(cbsa_total) as mean
from cbsa_21_22
group by in_2022^="", missing_cbsa^="", .<deactivation_year<2023
order by missing_cbsa, deactivate, in_2022;
quit;
proc sql;
select distinct .<deactivation_year<2023 as deactivate, count(*) as cnt
from cbsa_21_22
where missing_cbsa^=""
group by .<deactivation_year<2023;
quit;

proc export data=a.provider_x_cbsa_2019 outfile="D:\SASData\dua_052882\Sndbx\Chris F\ad_hoc\builder_meta\provider_x_cbsa_2019.csv";
proc export data=a.provider_x_cbsa_2020 outfile="D:\SASData\dua_052882\Sndbx\Chris F\ad_hoc\builder_meta\provider_x_cbsa_2020.csv";
proc export data=a.provider_x_cbsa_2021 outfile="D:\SASData\dua_052882\Sndbx\Chris F\ad_hoc\builder_meta\provider_x_cbsa_2021.csv";
proc export data=a.provider_x_cbsa_2022 outfile="D:\SASData\dua_052882\Sndbx\Chris F\ad_hoc\builder_meta\provider_x_cbsa_2022.csv";


