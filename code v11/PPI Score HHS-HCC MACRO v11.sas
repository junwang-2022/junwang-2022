
%let package_loc=D:\SASData\dua_052882\Sndbx\Michelle_V;
%let package=hhs_hcc_&year.;

%global CV YY0 YY1 YY2;

%if &year.=2019 %then %do;
	%let CV=05;

	%let YY0=19; 
	%let YY1=20; 
	%let YY2=21; 

	%let I0EMACRO=I0V05ED3;
	%let LABELMAC=V05128L1;
	%let HIERMAC=V05128H1;

	%let CCFMT0Y1 = HHS_V&CV.Y&YY1.OC;
	%let CCFMT0Y2 = HHS_V&CV.Y&YY1.OC;
	%let AGEFMT0  = I0AGEY&YY1.MCE;
    %let SEXFMT0  = I0SEXY&YY1.MCE;
	%let N_CC=266;
%end;

%if &year.=2020 %then %do;
	%let CV=05;

	%let YY0=19; 
	%let YY1=20; 
	%let YY2=21; 

	%let I0EMACRO=I0V05ED4;
	%let LABELMAC=V05128L1;
	%let HIERMAC=V05128H1;

	%let CCFMT0Y1 = HHS_V&CV.FY&YY1.P128C;
	%let CCFMT0Y2 = HHS_V&CV.FY&YY2.P128C;
	%let N_CC=267;
%end;

%if &year.=2021 %then %do;
	%let CV=07;

	%let YY0=20; 
	%let YY1=21; 
	%let YY2=22; 

	%let I0EMACRO=I0V07ED1;
	%let LABELMAC=V07141L1;
	%let HIERMAC=V07141H1;

	%let CCFMT0Y1 = HHS_V&CV.FY&YY1.Q141C;
	%let CCFMT0Y2 = HHS_V&CV.FY&YY2.Q141C;
	%let N_CC=274;
%end;

***;

libname library "&package_loc.\&package.";
filename in0 "&package_loc.\&package.";

%inc IN0("&I0EMACRO..SAS") / source2;/* Diagnosis edit */
%inc IN0("&HIERMAC..SAS") / source2;/* HCC hierarchy */

libname in "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\test v10\builder_output\year_&year.";
%let IDVAR=def_id;

%if &year.<2021 %then %do;
%let CC_FULL_LIST=%str(
CC1 CC2 CC3 CC4 CC5 CC6 CC7 CC8 CC9
CC10 CC11 CC12 CC13 CC14 CC15 CC16 CC17 CC18 CC19
CC20 CC21 CC22 CC23 CC24 CC25 CC26 CC27 CC28 CC29
CC30 CC31 CC32 CC33 CC34 CC35 CC36 CC37_1 CC37_2 CC38 CC39
CC40 CC41 CC42 CC43 CC44 CC45 CC46 CC47 CC48 CC49
CC50 CC51 CC52 CC53 CC54 CC55 CC56 CC57 CC58 CC59
CC60 CC61 CC62 CC63 CC64 CC65 CC66 CC67 CC68 CC69
CC70 CC71 CC72 CC73 CC74 CC75 CC76 CC77 CC78 CC79
CC80 CC81 CC82 CC83 CC84 CC85 CC86 CC87 CC88 CC89
CC90 CC91 CC92 CC93 CC94 CC95 CC96 CC97 CC98 CC99
CC100 CC101 CC102 CC103 CC104 CC105 CC106 CC107 CC108 CC109
CC110 CC111 CC112 CC113 CC114 CC115 CC116 CC117 CC118 CC119
CC120 CC121 CC122 CC123 CC124 CC125 CC126 CC127 CC128 CC129
CC130 CC131 CC132 CC133 CC134 CC135 CC136 CC137 CC138 CC139
CC140 CC141 CC142 CC143 CC144 CC145 CC146 CC147 CC148 CC149
CC150 CC151 CC152 CC153 CC154 CC155 CC156 CC157 CC158 CC159
CC160 CC161 CC162 CC163 CC164 CC165 CC166 CC167 CC168 CC169
CC170 CC171 CC172 CC173 CC174 CC175 CC176 CC177 CC178 CC179
CC180 CC181 CC182 CC183 CC184 CC185 CC186 CC187 CC188 CC189
CC190 CC191 CC192 CC193 CC194 CC195 CC196 CC197 CC198 CC199
CC200 CC201 CC202 CC203 CC204 CC205 CC206 CC207 CC208 CC209
CC210 CC211 CC212 CC213 CC214 CC215 CC216 CC217 CC218 CC219
CC220 CC221 CC222 CC223 CC224 CC225 CC226 CC227 CC228 CC229
CC230 CC231 CC232 CC233 CC234 CC235 CC236 CC237 CC238 CC239
CC240 CC241 CC242 CC243 CC244 CC245 CC246 CC247 CC248 CC249
CC250 CC251 CC252 CC253 CC254 CC255 CC256 CC257 CC258 CC259
CC260 CC261 CC262 CC263 CC264 CC265 CC266 
);

**===========================================================================**;
** HCCs, all, note numbering difference vs. HHS HCCs (267)                   **;
%let HCC_FULL_LIST=%str(
HCC1 HCC2 HCC3 HCC4 HCC5 HCC6 HCC7 HCC8 HCC9
HCC10 HCC11 HCC12 HCC13 HCC14 HCC15 HCC16 HCC17 HCC18 HCC19
HCC20 HCC21 HCC22 HCC23 HCC24 HCC25 HCC26 HCC27 HCC28 HCC29
HCC30 HCC31 HCC32 HCC33 HCC34 HCC35 HCC36 HCC37_1 HCC37_2 HCC38 HCC39
HCC40 HCC41 HCC42 HCC43 HCC44 HCC45 HCC46 HCC47 HCC48 HCC49
HCC50 HCC51 HCC52 HCC53 HCC54 HCC55 HCC56 HCC57 HCC58 HCC59
HCC60 HCC61 HCC62 HCC63 HCC64 HCC65 HCC66 HCC67 HCC68 HCC69
HCC70 HCC71 HCC72 HCC73 HCC74 HCC75 HCC76 HCC77 HCC78 HCC79
HCC80 HCC81 HCC82 HCC83 HCC84 HCC85 HCC86 HCC87 HCC88 HCC89
HCC90 HCC91 HCC92 HCC93 HCC94 HCC95 HCC96 HCC97 HCC98 HCC99
HCC100 HCC101 HCC102 HCC103 HCC104 HCC105 HCC106 HCC107 HCC108 HCC109
HCC110 HCC111 HCC112 HCC113 HCC114 HCC115 HCC116 HCC117 HCC118 HCC119
HCC120 HCC121 HCC122 HCC123 HCC124 HCC125 HCC126 HCC127 HCC128 HCC129
HCC130 HCC131 HCC132 HCC133 HCC134 HCC135 HCC136 HCC137 HCC138 HCC139
HCC140 HCC141 HCC142 HCC143 HCC144 HCC145 HCC146 HCC147 HCC148 HCC149
HCC150 HCC151 HCC152 HCC153 HCC154 HCC155 HCC156 HCC157 HCC158 HCC159
HCC160 HCC161 HCC162 HCC163 HCC164 HCC165 HCC166 HCC167 HCC168 HCC169
HCC170 HCC171 HCC172 HCC173 HCC174 HCC175 HCC176 HCC177 HCC178 HCC179
HCC180 HCC181 HCC182 HCC183 HCC184 HCC185 HCC186 HCC187 HCC188 HCC189
HCC190 HCC191 HCC192 HCC193 HCC194 HCC195 HCC196 HCC197 HCC198 HCC199
HCC200 HCC201 HCC202 HCC203 HCC204 HCC205 HCC206 HCC207 HCC208 HCC209
HCC210 HCC211 HCC212 HCC213 HCC214 HCC215 HCC216 HCC217 HCC218 HCC219
HCC220 HCC221 HCC222 HCC223 HCC224 HCC225 HCC226 HCC227 HCC228 HCC229
HCC230 HCC231 HCC232 HCC233 HCC234 HCC235 HCC236 HCC237 HCC238 HCC239
HCC240 HCC241 HCC242 HCC243 HCC244 HCC245 HCC246 HCC247 HCC248 HCC249
HCC250 HCC251 HCC252 HCC253 HCC254 HCC255 HCC256 HCC257 HCC258 HCC259
HCC260 HCC261 HCC262 HCC263 HCC264 HCC265 HCC266 
);
%end;
%else %do;
%let HCC_FULL_LIST=%str(
HCC1 HCC2 HCC3 HCC4 HCC5 HCC6 HCC7 HCC8 HCC9 HCC10 HCC11 HCC12 HCC13 HCC14 HCC15
HCC16 HCC17 HCC18 HCC19 HCC20 HCC21 HCC22 HCC23 HCC24 HCC25 HCC26 HCC27 HCC28 HCC29
HCC30 HCC31 HCC32 HCC33 HCC34 HCC35_1 HCC35_2 HCC36 HCC37_1 HCC37_2 HCC39 HCC40
HCC41 HCC42 HCC43 HCC44 HCC45 HCC46 HCC47 HCC48 HCC49 HCC50 HCC51 HCC52 HCC53 HCC54
HCC55 HCC56 HCC57 HCC58 HCC59 HCC60 HCC61 HCC62 HCC63 HCC64 HCC65 HCC66 HCC67 HCC68
HCC69 HCC70 HCC71 HCC72 HCC73 HCC74 HCC75 HCC76 HCC77 HCC78 HCC79 HCC80 HCC81 HCC82
HCC83 HCC84 HCC85 HCC86_1 HCC86_2 HCC86_3 HCC86_4 HCC87_1 HCC87_2 HCC88 HCC90 HCC91
HCC92 HCC93 HCC94 HCC95 HCC96 HCC97 HCC98 HCC99 HCC100 HCC101 HCC102 HCC103 HCC104
HCC105 HCC106 HCC107 HCC108 HCC109 HCC110 HCC111 HCC112 HCC113 HCC114 HCC115 HCC116
HCC117 HCC118 HCC119 HCC120 HCC121 HCC122 HCC123 HCC124 HCC125 HCC126 HCC127 HCC128
HCC129 HCC130 HCC131 HCC132 HCC133 HCC134 HCC135 HCC136 HCC137 HCC138 HCC139 HCC140
HCC141 HCC142 HCC143 HCC144 HCC145 HCC146 HCC147 HCC148 HCC149 HCC150 HCC151 HCC152
HCC153 HCC154 HCC155 HCC156 HCC157 HCC158 HCC159 HCC160 HCC161_1 HCC161_2 HCC162 HCC163
HCC164 HCC165 HCC166 HCC167 HCC168 HCC169 HCC170 HCC171 HCC172 HCC173 HCC174 HCC175
HCC176 HCC177 HCC178 HCC179 HCC180 HCC181 HCC182 HCC183 HCC184 HCC185 HCC186 HCC187
HCC188 HCC189 HCC190 HCC191 HCC192 HCC193 HCC194 HCC195 HCC196 HCC197 HCC198 HCC199
HCC200 HCC201 HCC202 HCC203 HCC204 HCC205 HCC206 HCC207 HCC208 HCC209 HCC210 HCC211
HCC212 HCC213 HCC214 HCC215 HCC216 HCC217 HCC218 HCC219 HCC220 HCC222 HCC223 HCC224
HCC225 HCC226 HCC228 HCC230 HCC231 HCC232 HCC233 HCC234 HCC235_1 HCC235_2 HCC235_3
HCC236_1 HCC236_2 HCC236_3 HCC237 HCC238 HCC239_1 HCC239_2 HCC239_3 HCC240 HCC241_1
HCC241_2 HCC242 HCC243 HCC244 HCC245 HCC246 HCC247 HCC248 HCC249 HCC250 HCC251 HCC252
HCC253 HCC254 HCC256 HCC257 HCC258 HCC260 HCC261 HCC262 HCC263 HCC264 HCC265 HCC266
HCC267
);

%let CC_FULL_LIST=%str(
CC1 CC2 CC3 CC4 CC5 CC6 CC7 CC8 CC9 CC10 CC11 CC12 CC13 CC14 CC15
CC16 CC17 CC18 CC19 CC20 CC21 CC22 CC23 CC24 CC25 CC26 CC27 CC28 CC29
CC30 CC31 CC32 CC33 CC34 CC35_1 CC35_2 CC36 CC37_1 CC37_2 CC39 CC40
CC41 CC42 CC43 CC44 CC45 CC46 CC47 CC48 CC49 CC50 CC51 CC52 CC53 CC54
CC55 CC56 CC57 CC58 CC59 CC60 CC61 CC62 CC63 CC64 CC65 CC66 CC67 CC68
CC69 CC70 CC71 CC72 CC73 CC74 CC75 CC76 CC77 CC78 CC79 CC80 CC81 CC82
CC83 CC84 CC85 CC86_1 CC86_2 CC86_3 CC86_4 CC87_1 CC87_2 CC88 CC90 CC91
CC92 CC93 CC94 CC95 CC96 CC97 CC98 CC99 CC100 CC101 CC102 CC103 CC104
CC105 CC106 CC107 CC108 CC109 CC110 CC111 CC112 CC113 CC114 CC115 CC116
CC117 CC118 CC119 CC120 CC121 CC122 CC123 CC124 CC125 CC126 CC127 CC128
CC129 CC130 CC131 CC132 CC133 CC134 CC135 CC136 CC137 CC138 CC139 CC140
CC141 CC142 CC143 CC144 CC145 CC146 CC147 CC148 CC149 CC150 CC151 CC152
CC153 CC154 CC155 CC156 CC157 CC158 CC159 CC160 CC161_1 CC161_2 CC162 CC163
CC164 CC165 CC166 CC167 CC168 CC169 CC170 CC171 CC172 CC173 CC174 CC175
CC176 CC177 CC178 CC179 CC180 CC181 CC182 CC183 CC184 CC185 CC186 CC187
CC188 CC189 CC190 CC191 CC192 CC193 CC194 CC195 CC196 CC197 CC198 CC199
CC200 CC201 CC202 CC203 CC204 CC205 CC206 CC207 CC208 CC209 CC210 CC211
CC212 CC213 CC214 CC215 CC216 CC217 CC218 CC219 CC220 CC222 CC223 CC224
CC225 CC226 CC228 CC230 CC231 CC232 CC233 CC234 CC235_1 CC235_2 CC235_3
CC236_1 CC236_2 CC236_3 CC237 CC238 CC239_1 CC239_2 CC239_3 CC240 CC241_1
CC241_2 CC242 CC243 CC244 CC245 CC246 CC247 CC248 CC249 CC250 CC251 CC252
CC253 CC254 CC256 CC257 CC258 CC260 CC261 CC262 CC263 CC264 CC265 CC266
CC267
);
%end;

%macro SET0(CC=, HIER=);
 %let K=1;
 if HCC&CC=1 then do;
  %DO %UNTIL(%SCAN(&HIER,&K)=);
    HCC%SCAN(&HIER,&K) = 0;
    %LET K=%EVAL(&K+1);
  %END;
 end ;
%mend SET0; 
%MACRO FIND_IND;
  CC_VAR=cats("CC",translate(CC,"_",".")) ;
  %let J=1;
  %do %until( &J > &N_CC) ;
      IF CC_VAR="%SCAN(&CC_FULL_LIST,&J)"  THEN IND=&J;                                                                                    
      %LET J=%EVAL(&J+1);                                                                                                            
  %END;   
%MEND FIND_IND;

**===========================================================================**;
** lookup CCs for primary and secondary assignment, VC is 9/0                **;
%macro FIND_CCs(VC);
** 
set elements of 0/1 array _C(*) to 1.                                     
steps:                                                               
 o CC initialized to 9999.                                                 
 o DIAG is sent to diagnoses edit macro. If CC is changed to -1 it means   
    DIAG did not pass edits and nothing else needs to be done with it.     
 o If CC is still 9999, primary assignment and secondary assigment       
    must be done.                                                           
**;
if CC ne "-1.0" and CC ne "9999" then do;
 ** c to n conversion **;
 %FIND_IND;
 ** poke returned valid array pointer CC into 0/1 array with value=1 **;
 if 1 <= IND <= dim(_C) then _C(IND)=1;
 ** for debugging **;
 *file print; *put "Find " DIAG= CC= IND=;
end;
else if CC="9999" then do; 
 ** some diagnoses get multiple CCs **;
 ** CC creation depends on ICD version code **;
 ** CC creation uses a separate format for 2020 fiscal year and 2021 fiscal year **;
if Diagnosis_service_date < mdy(10,1,&year.) then do ;
   ** primary assignment **;
   CC = input(left(put(DIAG,$I&VC&&CCFMT0Y1..)),8.); %FIND_IND;
   if 1 <= IND <= dim(_C) then _C(IND)=1;
   ** for debugging **;
   *file print; *put "Find " DIAG= CC= IND=;
   ** duplicate assignment **;
   CC = input(left(put(DIAG,$I&VC.dup_&&CCFMT0Y1..)),8.); %FIND_IND; 
   if 1 <= IND <= dim(_C) then _C(IND)=1;
   ** for debugging **;
   *file print; *put "Find " DIAG= CC= IND=;
 end ;
 else do ;  
   ** primary assignment **;
   CC = input(left(put(DIAG,$I&VC&&CCFMT0Y2..)),8.); %FIND_IND;
   if 1 <= IND <= dim(_C) then _C(IND)=1;
   ** for debugging **;
   *file print; *put "Find " DIAG= CC= IND=;
   ** duplicate assignment **;
   CC = input(left(put(DIAG,$I&VC.dup_&&CCFMT0Y2..)),8.); %FIND_IND;
   if 1 <= IND <= dim(_C) then _C(IND)=1;
   ** for debugging **;
   *file print; *put "Find " DIAG= CC= IND=;
 end ;
end;
** CC=-1 means diag did not pass edits **;
%mend FIND_CCs;


%macro hcc;

%if epi.=PPI_CH_MED %then %do;
proc sql;
create table &epi._hcc_dx as
select a.*, a.dx_cd as diag, b.prf_beg as diagnosis_service_date format=mmddyy10., b.bene_dob_dt as dob, b.bene_gender as sex, "" as metal, 
floor(yrdif(DOB,prf_beg,"AGE")) as age, (calculated age) as age_last
from in.&epi._hcc_dx a
left join (select distinct def_id, prf_beg, bene_gender, bene_dob_dt from in.&epi._trigger) b
on a.def_id=b.def_id
order by def_sub, def_id; 
quit;
%end;

%else %do;
proc sql;
create table &epi._hcc_dx as
select a.*, a.dx_cd as diag, b.index_dt as diagnosis_service_date format=mmddyy10., b.bene_dob_dt as dob, b.bene_gender as sex, "" as metal, 
floor(yrdif(DOB,index_dt,"AGE")) as age, (calculated age) as age_last
from in.&epi._hcc_dx a
left join in.&epi._trigger b
on a.def_sub=b.def_sub and a.def_id=b.def_id
order by def_sub, def_id; 
quit;
%end;

data out.hcc_&epi.(keep=def_sub def_id hcc: cnt_hcc);
length &CC_FULL_LIST &HCC_FULL_LIST IND DeleteRecord DeltaAge 3 cc $4.;
retain &CC_FULL_LIST SEX_1_2;

 array _C(*)     &CC_FULL_LIST;
 array _HCC(*)   &HCC_FULL_LIST;

set &epi._hcc_dx;
by def_sub def_id;

  **==========================================================================**;
  ** first record for person **;
if first.def_id then do;
   SEX=upcase(substr(left(SEX),1,1));
   ** remap SEX (M/F) to comply with age/sex edits macro (1/2) **;
   SEX_1_2=" ";
   if      SEX in("M","1") then SEX_1_2="1";
   else if SEX in("F","2") then SEX_1_2="2";
   ** convert yyyymmdd to SAS date, suppress error message if invalid **;
   OriginalDOB=DOB;
   DOB=input(put(DOB,z8.),?? yymmdd8.); 
   METAL=upcase(substr(left(METAL),1,1));

   ** set CCs to 0 **;
   do i=1 to dim(_C);
    _C(i)=0;
   end;
end /*if first.IDVAR*/;

  **=========================================================================**;
  ** if there are diagnoses for enrollee then do:                            **;
  **  - create CCs using format &CCFMT0Y1 and &CCFMT0Y2                      **;
  **  - perform ICD10 edits using macro &I0EMACRO                            **;
  **  - assign additional CCs if applicable                                  **;
  **=========================================================================**;
   DIAG=upcase(left(DIAG));
   ** compute age at diagnosis for MCE edits **;
   if DOB ne . and DIAGNOSIS_SERVICE_DATE ne . then 
     AGE_AT_DIAGNOSIS=floor(yrdif(DOB,DIAGNOSIS_SERVICE_DATE,"AGE"));
   ** no negatives **;
   if (. < AGE_AT_DIAGNOSIS < 0) then AGE_AT_DIAGNOSIS=0;
 
   **=========================================================================**;
   ** errors, do not perform lookup **;
   DeltaAge=0;
   if AGE_AT_DIAGNOSIS ne . and AGE_LAST ne . then DeltaAge=AGE_LAST-AGE_AT_DIAGNOSIS;
   if missing(DIAG)                          or 
      missing(DIAGNOSIS_SERVICE_DATE)        or 
      AGE_AT_DIAGNOSIS < 0                   or 
      SEX_1_2 not in("1","2")                or 
      missing(AGE_LAST)                      or
      AGE_AT_DIAGNOSIS > AGE_LAST            or
      (not missing(DOB) and not missing(DIAGNOSIS_SERVICE_DATE) and DOB > DIAGNOSIS_SERVICE_DATE) or
      DeltaAge > 1 then delete;
 
   **=========================================================================**;
   ** no errors, attempt lookup **;

    ** ICD10 age/sex edits, returns updated array pointer CC (string), passes SEX as 1/2 **;
    ** use AGE_AT_DIAGNOSIS for MCE edit, use AGE_LAST for CC (RTI) edit **;
    *CC="9999"; **now set in validity check step 11/22/2016 ;
    ** ICD10 **;
     ** for debugging **;
     *file print; *put;
    ** check validity of icd codes ;
 	   if Diagnosis_service_date < mdy(10,1,&year.) then do ;
        _valid = input(left(put(DIAG,$I0&&CCFMT0Y1..)),8.);
         if 1 <= _valid <= dim(_C) then CC ="9999" ;
         else CC="-1.0" ;
       end ;
 	   else do ;  
         _valid = input(left(put(DIAG,$I0&&CCFMT0Y2..)),8.);
         if 1 <= _valid <= dim(_C) then CC ="9999" ;
         else CC="-1.0" ;
       end ;

   if CC="9999" then do ;
     %&I0EMACRO(AGERTI=AGE_LAST,AGEMCE=AGE_AT_DIAGNOSIS,SEX=SEX_1_2,ICD0=DIAG);
   end ;
     %FIND_CCS(0);

  if last.def_id then do;

    ** map CCs to HCCs, apply HCC hierarchies **;
     %&HIERMAC; 

   if AGE_LAST >= 21 then do;
	HCC028 = 0;
	HCC064 = 0;
	HCC242 = 0;
	HCC243 = 0;
	HCC244 = 0;
	HCC245 = 0;
	HCC246 = 0;
	HCC247 = 0;
	HCC248 = 0;
	HCC249 = 0;   
   end;

   else if 2 <= AGE_LAST <=  20 then do;
	HCC022 = 0;
	HCC064 = 0;
	HCC174 = 0;
	HCC242 = 0;
	HCC243 = 0;
	HCC244 = 0;
	HCC245 = 0;
	HCC246 = 0;
	HCC247 = 0;
	HCC248 = 0;
	HCC249 = 0;
   end;

   else if 0 <= AGE_LAST <=  1 then do;
	HCC022 = 0;
	HCC087 = 0;
	HCC087_1 = 0;
	HCC087_2 = 0;
	HCC088 = 0;
	HCC089 = 0;
	HCC090 = 0;
	HCC094 = 0;
	HCC123 =0;
	HCC174 =0;	
	HCC203 = 0;
	HCC204 = 0;
	HCC205 = 0;
	HCC207 = 0;
	HCC208 = 0; 
	HCC209 = 0;
	HCC210 =0;
	HCC211 =0;
	HCC212 =0;
   end;

   ** count HCCs **;
   cnt_hcc=0;
   do i=1 to dim(_HCC);
    if _HCC(i)=1 then cnt_hcc+1;
   end;

   ** write ONE record per enrollee **;
   output out.hcc_&epi.;

  end; /*if last.IDVAR*/

run;

%mend;

