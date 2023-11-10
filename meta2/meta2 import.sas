libname meta2 "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2";

data meta2.cms_spec_cd    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\meta2\cms_spec_cd.csv' delimiter = ',' MISSOVER
 DSD lrecl=32767 firstobs=2 ;
        informat Spec_cd $2. ;
        informat Spec_desc $100. ;
        informat Spec_type $5. ;
        format Spec_cd $2. ;
        format Spec_desc $100. ;
        format Spec_type $5. ;
    input
                 Spec_cd $
                 Spec_desc  $
				 Spec_type $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;
data meta2.cms_spec_cd;
set meta2.cms_spec_cd;
if length(spec_cd)=1 then spec_cd="0"||spec_cd;
run;

data a.ccn_name    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\PPI3.0\taxonomy\npi pos\CCN_INFORMATION_FULL.csv' delimiter = ','
 MISSOVER DSD lrecl=32767 firstobs=2 ;
        informat Federal_Provider_Number $6. ;
        informat Type $6. ;
        informat Provider_City $30. ;
        informat Provider_Name $100. ;
        informat Provider_Address $50. ;
        informat Provider_State $2. ;
        informat Provider_Zip_Code $5. ;
        format Federal_Provider_Number $6. ;
        format Type $6. ;
        format Provider_City $30. ;
        format Provider_Name $100. ;
        format Provider_Address $50. ;
        format Provider_State $2. ;
        format Provider_Zip_Code $5. ;
     input
                 Federal_Provider_Number $
                 Type  $
                 Provider_City  $
                 Provider_Name  $
                 Provider_Address  $
                 Provider_State  $
                 Provider_Zip_Code $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;


  /**********************************************************************
  *   PRODUCT:   SAS
  *   VERSION:   9.4
  *   CREATOR:   External File Interface
  *   DATE:      21MAR23
  *   DESC:      Generated SAS Datastep Code
  *   TEMPLATE SOURCE:  (None Specified.)
  ***********************************************************************/
     data WORK.ppi_spec_mapping    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2\ppi_spec_mapping.csv' delimiter = ','
 MISSOVER DSD lrecl=32767 firstobs=2  ;
		informat Clinical_category $50. ;
        informat Episode_type $30. ;
        informat def_sub $30. ;
        informat Episode_name $50. ;
        informat var1 $2. ;
        informat var2 $2. ;
        informat var3 $2. ;
        informat var4 $2. ;
        informat var5 $2. ;
        informat var6 $2. ;
        informat var7 $2. ;
        informat var8 $2. ;
        informat var9 $2. ;
        informat var10 $2. ;
        informat var11 $2. ;
        informat var12 $2. ;
        informat var13 $2. ;
        informat var14 $2. ;
        informat var15 $2. ;
        informat var16 $2. ;
        informat var17 $2. ;
        informat var18 $2. ;
        informat var19 $2. ;
        informat var20 $2. ;
        informat var21 $2. ;
        informat var22 $2. ;
        informat var23 $2. ;
        informat var24 $2. ;
        informat var25 $2. ;
        informat var26 $2. ;
        informat var27 $2. ;
        informat var28 $2. ;
        informat var29 $2. ;
        informat var30 $2. ;
        informat var31 $2. ;
        informat var32 $2. ;
        informat var33 $2. ;
        informat var34 $2. ;
        informat var35 $2. ;
        informat var36 $2. ;
        informat var37 $2. ;
        informat var38 $2. ;
        informat var39 $2. ;
        informat var40 $2. ;
        informat var41 $2. ;
        informat var42 $2. ;
        informat var43 $2. ;
        informat var44 $2. ;
        informat var45 $2. ;
        informat var46 $2. ;
        informat var47 $2. ;
        informat var48 $2. ;
        informat var49 $2. ;
        informat var50 $2. ;
        informat var51 $2. ;
        informat var52 $2. ;
        informat var53 $2. ;
        informat var54 $2. ;
        informat var55 $2. ;
        informat var56 $2. ;
        informat var57 $2. ;
        informat var58 $2. ;
        informat var59 $2. ;
        informat var60 $2. ;
        informat var61 $2. ;
        informat var62 $2. ;
        informat var63 $2. ;
        informat var64 $2. ;
        informat var65 $2. ;
        format Clinical_category $50. ;
        format Episode_type $30. ;
        format def_sub $30. ;
        format Episode_name $50. ;
        format var1 $2. ;
        format var2 $2. ;
        format var3 $2. ;
        format var4 $2. ;
        format var5 $2. ;
        format var6 $2. ;
        format var7 $2. ;
        format var8 $2. ;
        format var9 $2. ;
        format var10 $2. ;
        format var11 $2. ;
        format var12 $2. ;
        format var13 $2. ;
        format var14 $2. ;
        format var15 $2. ;
        format var16 $2. ;
        format var17 $2. ;
        format var18 $2. ;
        format var19 $2. ;
        format var20 $2. ;
        format var21 $2. ;
        format var22 $2. ;
        format var23 $2. ;
        format var24 $2. ;
        format var25 $2. ;
        format var26 $2. ;
        format var27 $2. ;
        format var28 $2. ;
        format var29 $2. ;
        format var30 $2. ;
        format var31 $2. ;
        format var32 $2. ;
        format var33 $2. ;
        format var34 $2. ;
        format var35 $2. ;
        format var36 $2. ;
        format var37 $2. ;
        format var38 $2. ;
        format var39 $2. ;
        format var40 $2. ;
        format var41 $2. ;
        format var42 $2. ;
        format var43 $2. ;
        format var44 $2. ;
        format var45 $2. ;
        format var46 $2. ;
        format var47 $2. ;
        format var48 $2. ;
        format var49 $2. ;
        format var50 $2. ;
        format var51 $2. ;
        format var52 $2. ;
        format var53 $2. ;
        format var54 $2. ;
        format var55 $2. ;
        format var56 $2. ;
        format var57 $2. ;
        format var58 $2. ;
        format var59 $2. ;
        format var60 $2. ;
        format var61 $2. ;
        format var62 $2. ;
        format var63 $2. ;
        format var64 $2. ;
        format var65 $2. ;
     input
       		     Clinical_category $
        		 Episode_type $
				 def_sub $
         		 Episode_name $
                 var1  $
                 var2  $
                 var3  $
                 var4  $
                 var5  $
                 var6  $
                 var7  $
                 var8  $
                 var9  $
                 var10  $
                 var11  $
                 var12  $
                 var13  $
                 var14  $
                 var15  $
                 var16  $
                 var17  $
                 var18  $
                 var19  $
                 var20  $
                 var21  $
                 var22  $
                 var23  $
                 var24  $
                 var25  $
                 var26  $
                 var27  $
                 var28  $
                 var29  $
                 var30  $
                 var31  $
                 var32  $
                 var33  $
                 var34  $
                 var35  $
                 var36  $
                 var37  $
                 var38  $
                 var39  $
                 var40  $
                 var41  $
                 var42  $
                 var43  $
                 var44  $
                 var45  $
                 var46  $
                 var47  $
                 var48  $
                 var49  $
                 var50  $
                 var51  $
                 var52  $
                 var53  $
                 var54  $
                 var55  $
                 var56  $
                 var57  $
                 var58  $
                 var59  $
                 var60  $
                 var61  $
                 var62  $
                 var63  $
                 var64  $
                 var65  $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

proc sort data=ppi_spec_mapping;
by clinical_category episode_type episode_name def_sub;
run;
proc transpose data=ppi_spec_mapping out=ppi_spec_mapping2(rename=(col1=spec_cd));
by clinical_category episode_type episode_name def_sub;
var var1-var65;
run;

data ppi_spec_mapping2;
set ppi_spec_mapping2;
where spec_cd^="";
if length(spec_cd)=1 then spec_cd="0"||spec_cd;
drop _name_;
run;

proc sql;
create table meta2.ppi_spec_mapping as
select distinct a.*, b.spec_desc, b.spec_type
from ppi_spec_mapping2 a
left join meta2.cms_spec_cd b
on a.spec_cd=b.spec_cd
order by clinical_category, episode_type, episode_name, spec_type, spec_cd;
quit;

proc freq data=meta2.ppi_spec_mapping;
tables spec_type;
run;



   /**********************************************************************
   *   PRODUCT:   SAS
   *   VERSION:   9.4
   *   CREATOR:   External File Interface
   *   DATE:      23JUL23
   *   DESC:      Generated SAS Datastep Code
   *   TEMPLATE SOURCE:  (None Specified.)
   ***********************************************************************/
      data meta2.wi    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'C:\Users\jun.wang\Episode Builder\meta2\WI_ALL_YEARS.csv' delimiter = ',' MISSOVER
 DSD lrecl=32767 firstobs=2 ;
         informat CCN $6. ;
         informat WI best32. ;
         informat Year best32. ;
         format CCN $6. ;
         format WI best32. ;
         format Year best12. ;
      input
                  CCN $
                  WI  
                  Year
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;

   /**********************************************************************
   *   PRODUCT:   SAS
   *   VERSION:   9.4
   *   CREATOR:   External File Interface
   *   DATE:      23JUL23
   *   DESC:      Generated SAS Datastep Code
   *   TEMPLATE SOURCE:  (None Specified.)
   ***********************************************************************/
      data meta2.drg_weight    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'D:\SASData\Jun_Wang\drg_weight.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
         informat MS_DRG $3. ;
         informat MDC $3. ;
         informat TYPE $4. ;
         informat MS_DRG_Title $150. ;
         informat Weights best32. ;
         informat Geometric_mean_LOS best32. ;
         informat Arithmetic_mean_LOS best32. ;
         informat FY best32. ;
         format MS_DRG $3. ;
         format MDC $3. ;
         format TYPE $4. ;
         format MS_DRG_Title $150. ;
         format Weights best12. ;
         format Geometric_mean_LOS best12. ;
         format Arithmetic_mean_LOS best12. ;
         format FY best12. ;
      input
                  MS_DRG $
                  MDC  $
                  TYPE  $
                  MS_DRG_Title  $
                  Weights
                  Geometric_mean_LOS
                  Arithmetic_mean_LOS
                  FY
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;

	  data meta2.drg_weight;
	  set meta2.drg_weight;
	  if length(ms_drg)=1 then ms_drg="00"||ms_drg;
	  if length(ms_drg)=2 then ms_drg="0"||ms_drg;
	  run;

/* --------------------------------------------------------------------
   Code generated by a SAS task
   
   Generated on Tuesday, October 24, 2023 at 1:28:55 PM
   By task:     Import Data Wizard
   
   Source file: D:\SASData\dua_052882\Sndbx\Jun_W\builder
   test\meta2\cbd_spec_mapping.csv
   Server:      SASApp
   
   Output data: WORK.CBD_SPEC_MAPPING_0002
   Server:      SASApp
   -------------------------------------------------------------------- */

DATA WORK.CBD_SPEC_MAPPING;
    LENGTH
        'Service Description'n $ 100
        'Service Details'n $ 100
        'Musculoskeletal system diseases'n $ 100
        'Circulatory system diseases'n $ 100
        'Eye and adnexa diseases'n $ 100
        Cancer         $ 100
        'Nervous system diseases'n $ 100
        'Endocrine metabolic diseases'n $ 100
        'GI system diseases'n $ 100
        'Respiratory system diseases'n $ 100
        'GU system diseases'n $ 100
        'Ear and mastoid diseases'n $ 100
        'Skin subcutaneous diseases'n $ 100
        'Infectious diseases'n $ 100
        'Blood diseases'n $ 100
        Injury           $ 79
        'Mental health'n $ 100
        'Perinatal care'n $ 100 ;
    FORMAT
        'Service Description'n $CHAR100.
        'Service Details'n $CHAR100.
        'Musculoskeletal system diseases'n $CHAR100.
        'Circulatory system diseases'n $CHAR100.
        'Eye and adnexa diseases'n $CHAR100.
        Cancer         $CHAR100.
        'Nervous system diseases'n $CHAR100.
        'Endocrine metabolic diseases'n $CHAR100.
        'GI system diseases'n $CHAR100.
        'Respiratory system diseases'n $CHAR100.
        'GU system diseases'n $CHAR100.
        'Ear and mastoid diseases'n $CHAR100.
        'Skin subcutaneous diseases'n $CHAR100.
        'Infectious diseases'n $CHAR100.
        'Blood diseases'n $CHAR100.
        Injury           $CHAR100.
        'Mental health'n $CHAR100.
        'Perinatal care'n $CHAR100. ;
   INFORMAT
        'Service Description'n $CHAR100.
        'Service Details'n $CHAR100.
        'Musculoskeletal system diseases'n $CHAR100.
        'Circulatory system diseases'n $CHAR100.
        'Eye and adnexa diseases'n $CHAR100.
        Cancer         $CHAR100.
        'Nervous system diseases'n $CHAR100.
        'Endocrine metabolic diseases'n $CHAR100.
        'GI system diseases'n $CHAR100.
        'Respiratory system diseases'n $CHAR100.
        'GU system diseases'n $CHAR100.
        'Ear and mastoid diseases'n $CHAR100.
        'Skin subcutaneous diseases'n $CHAR100.
        'Infectious diseases'n $CHAR100.
        'Blood diseases'n $CHAR100.
        Injury           $CHAR100.
        'Mental health'n $CHAR100.
        'Perinatal care'n $CHAR100. ;

    INFILE 'D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2\cbd_spec_mapping.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
    INPUT
        'Service Description'n : $CHAR100.
        'Service Details'n : $CHAR100.
        'Musculoskeletal system diseases'n : $CHAR100.
        'Circulatory system diseases'n : $CHAR100.
        'Eye and adnexa diseases'n : $CHAR100.
        Cancer         : $CHAR100.
        'Nervous system diseases'n : $CHAR100.
        'Endocrine metabolic diseases'n : $CHAR100.
        'GI system diseases'n : $CHAR100.
        'Respiratory system diseases'n : $CHAR100.
        'GU system diseases'n : $CHAR100.
        'Ear and mastoid diseases'n : $CHAR100.
        'Skin subcutaneous diseases'n : $CHAR100.
        'Infectious diseases'n : $CHAR100.
        'Blood diseases'n : $CHAR100.
        Injury           : $CHAR100.
        'Mental health'n : $CHAR100.
        'Perinatal care'n : $CHAR100. ;
RUN;


proc sort data= CBD_SPEC_MAPPING(where=('Service Description'n<>"")) out=b; by 'Service Description'n 'Service Details'n; run;
proc transpose data=b out=c;
by 'Service Description'n 'Service Details'n;
var _all_;
run;

data meta2.cbd_spec_mapping;
set c;
where _name_ not in ("Service Description" "Service Details");

if _name_="Endocrine metabolic diseases" then _name_="Endocrine/metabolic diseases";
if _name_="Skin subcutaneous diseases" then _name_="Skin/subcutaneous diseases";

rename _name_=clin_cat col1=spec_details 'Service Description'n=Service_Description 'Service Details'n=Service_Details;
run;


DATA meta2.ppi_name_mapping;
    LENGTH
        EPISODE_TYPE     $ 30
        DEF_NAME         $ 15
        DEF_SUB          $ 30
        DEF_DESC         $ 100
        EPISODE_NAME     $ 100
        Clinical_category $ 100 ;
    FORMAT
        EPISODE_TYPE     $CHAR30.
        DEF_NAME         $CHAR15.
        DEF_SUB          $CHAR30.
        DEF_DESC         $CHAR100.
        EPISODE_NAME     $CHAR100.
        Clinical_category $CHAR100. ;
    INFORMAT
        EPISODE_TYPE     $CHAR30.
        DEF_NAME         $CHAR15.
        DEF_SUB          $CHAR30.
        DEF_DESC         $CHAR100.
        EPISODE_NAME     $CHAR100.
        Clinical_category $CHAR100. ;
    INFILE 'D:\SASData\dua_052882\Sndbx\Jun_W\builder test\meta2\ppi_name_mapping.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
    INPUT
        EPISODE_TYPE     : $CHAR30.
        DEF_NAME         : $CHAR15.
        DEF_SUB          : $CHAR30.
        DEF_DESC         : $CHAR100.
        EPISODE_NAME     : $CHAR100.
        Clinical_category : $CHAR100. ;
RUN;
