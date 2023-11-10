libname meta "D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11";

data META.DEF_LIST    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\def_list.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
        informat DEF_SRC $10. ;
        informat DEF_TYPE $10. ;
        informat DEF_NAME $15. ;
        informat DEF_SUB $30. ;
        informat DEF_DESC $100. ;
        informat KEEP $10. ;
        informat SUB_F1 $100. ;
        informat SUB_F2 $100. ;
        informat SUB_F3 $100. ;
        informat SUB_F4 $100. ;
        format DEF_SRC $10. ;
        format DEF_TYPE $10. ;
        format DEF_NAME $15. ;
        format DEF_SUB $30. ;
        format DEF_DESC $100. ;
        format KEEP $10. ;
        format SUB_F1 $100. ;
        format SUB_F2 $100. ;
        format SUB_F3 $100. ;
        format SUB_F4 $100. ;
	input
                 DEF_SRC  $
                 DEF_TYPE  $
				 DEF_NAME $
                 DEF_SUB  $
                 DEF_DESC  $
				 KEEP $
				 SUB_F1 $
				 SUB_F2 $
				 SUB_F3 $
				 SUB_F4 $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;



data META.MS_DRG    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\ms_drg.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
        informat MS_DRG_TYPE $1. ;
        informat MS_DRG_CAT $2. ;
        informat MS_DRG_CAT_DESC $100. ;
        informat MS_DRG_GRP $3. ;
        informat MS_DRG_GRP_DESC $100. ;
        informat MS_DRG $3. ;
        informat MS_DRG_DESC $101. ;
        informat BUNDLE_NAME $100. ;
        informat Weights best32. ;
        informat Geometric_mean_LOS best32. ;
        informat Arithmetic_mean_LOS best32. ;
        informat VERSION $3. ;
        informat EFFECTIVE_DATE $1. ;
        informat TERMINATION_DATE mmddyy10. ;
        informat BPCIA $5. ;
        informat CJ_PERINATAL $5. ;
        informat KCC $5. ;
        informat PPI_IP_PROC $5. ;
        informat PPI_IOP_PROC $5. ;
        informat PPI_IOP_MJRLE $5. ;
        informat PPI_OP_PROC $5. ;
        informat PPI_PR_PROC $5. ;
        informat PPI_IP_MED $10. ;
        informat PPI_CH_MED $10. ;
        informat PPI_AC_MED $10. ;
		informat PPI_CHEMO $10. ;
		informat PPI_PERINATAL $10. ;
   		format MS_DRG_TYPE $1. ;
        format MS_DRG_CAT $2. ;
        format MS_DRG_CAT_DESC $100. ;
        format MS_DRG_GRP $3. ;
        format MS_DRG_GRP_DESC $100. ;
        format MS_DRG $3. ;
        format MS_DRG_DESC $101. ;
        format BUNDLE_NAME $100. ;
        format Weights best12. ;
        format Geometric_mean_LOS best12. ;
        format Arithmetic_mean_LOS best12. ;
        format VERSION $3. ;
        format EFFECTIVE_DATE $1. ;
        format TERMINATION_DATE mmddyy10. ;
        format BPCIA $5. ;
        format CJ_PERINATAL $5. ;
        format KCC $5. ;
        format PPI_IP_PROC $5. ;
        format PPI_IOP_PROC $5. ;
        format PPI_IOP_MJRLE $5. ;
        format PPI_OP_PROC $5. ;
        format PPI_PR_PROC $5. ;
        format PPI_IP_MED $10. ;
        format PPI_CH_MED $10. ;
        format PPI_AC_MED $10. ;
        format PPI_CHEMO $10. ;
		format PPI_PERINATAL $10. ;
input
                 MS_DRG_TYPE  $
                 MS_DRG_CAT $
                 MS_DRG_CAT_DESC  $
                 MS_DRG_GRP $
                 MS_DRG_GRP_DESC  $
                 MS_DRG $
                 MS_DRG_DESC  $
                 BUNDLE_NAME  $
                 Weights
                 Geometric_mean_LOS
                 Arithmetic_mean_LOS
                 VERSION  $
                 EFFECTIVE_DATE  $
                 TERMINATION_DATE
				 BPCIA $
                 CJ_PERINATAL  $
                 KCC  $
				 PPI_IP_PROC $
 				 PPI_IOP_PROC $
				 PPI_IOP_MJRLE $
				 PPI_OP_PROC $
				 PPI_PR_PROC $
				 PPI_IP_MED $
				 PPI_CH_MED $
     		   	 PPI_AC_MED $
				 PPI_CHEMO $
				 PPI_PERINATAL $
    ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

	data meta.ms_drg;
	set meta.ms_drg;
	if length(ms_drg)=1 then ms_drg="00"||ms_drg;
	if length(ms_drg)=2 then ms_drg="0"||ms_drg;
	run;

data META.ICD_10_DX    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\icd_10_dx.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
        informat DX0_DESC $50. ;
        informat DX1_DESC $50. ;
		informat final_DX1_DESC $50. ;
        informat DX2_DESC $50. ;
        informat DX3 $3. ;
        informat DX3_DESC $50. ;
        informat DX3_LONGTITLE $50. ;
        informat DX4 $4. ;
        informat DX4_DESC $50. ;
        informat DX4_LONGTITLE $50. ;
        informat DX5 $5. ;
        informat DX5_DESC $50. ;
        informat DX5_LONGTITLE $50. ;
        informat DX6 $6. ;
        informat DX6_DESC $50. ;
        informat DX6_LONGTITLE $50. ;
        informat DX7 $7. ;
        informat DX7_DESC $50. ;
        informat DX7_LONGTITLE $50. ;
        informat EFFECTIVE_DATE mmddyy10. ;
        informat TERMINATION_DATE mmddyy10. ;
        informat CCS $3. ;
        informat CCS_DESC $100. ;
        informat PAC_RF $10. ;
        informat PAC_DESC $100. ;
        informat HAC_RF $10. ;
        informat HAC_DESC $100. ;
        informat HCC_V21_1 $6. ;
        informat HCC_V21_1_DESC $67. ;
        informat HCC_V21_2 $6. ;
        informat HCC_V21_2_DESC $67. ;
        informat HCC_V21_3 $6. ;
        informat HCC_V21_3_DESC $67. ;
        informat RXHCC_1 $6. ;
        informat RXHCC_1_DESC $67. ;
        informat RXHCC_2 $6. ;
        informat RXHCC_2_DESC $67. ;
        informat CHR_FLAG $1. ;
        informat POA_EXEMPT $1. ;
        informat SEX_FLAG $1. ;
        informat FLAG $1. ;
		informat PPI_Chronic_Med $100.;
		informat PPI_Acute_Med $100.;
        informat KCC $5. ;
        informat CJ_PERINATAL $5. ;
        informat GAM_08 $5. ;
        informat GAM_33 $5. ;
        informat GAM_34 $5. ;
       	informat GAM_04 $5. ;
       	informat LVC_01 $5. ;
       	informat LVC_02 $5. ;
       	informat LVC_04_12 $5. ;
       	informat LVC_07_10 $5. ;
       	informat LVC_09 $5. ;
       	informat LVC_11 $5. ;
        informat LVC_13 $5. ;
       	informat LVC_16 $5. ;
       	informat LVC_17 $5. ;
		informat HEDIS_BCS $5.;
 		informat MERCK_GI $5.;
 		informat LILLY_ADRD_1_2 $5.;
 		informat HANGER_DFU $5.;
  		informat MIPS_WHR $5.;
		informat PFIZER_ATTR_CM $5.;
		informat ACOREACH_TFU $20. ;
 		informat GAM_31 $20. ;
		informat PPI_IP_PROC $20. ;
 		informat PPI_IOP_PROC $20. ;
 		informat PPI_IOP_MJRLE $20. ;
  		informat PPI_OP_PROC $20. ;
 		informat PPI_PR_PROC $20. ;
 		informat PPI_IP_MED $20. ;
  		informat PPI_CH_MED $20. ;
 		informat PPI_AC_MED $20. ;
 		informat PPI_CHEMO $20. ;
 		informat PPI_RO $20. ;
		informat PPI_PERINATAL $20. ;
        format DX0_DESC $50. ;
        format DX1_DESC $50. ;
		format final_DX1_DESC $50. ;
        format DX2_DESC $50. ;
        format DX3 $3. ;
        format DX3_DESC $50. ;
        format DX3_LONGTITLE $50. ;
        format DX4 $4. ;
        format DX4_DESC $50. ;
        format DX4_LONGTITLE $50. ;
        format DX5 $5. ;
        format DX5_DESC $50. ;
        format DX5_LONGTITLE $50. ;
        format DX6 $6. ;
        format DX6_DESC $50. ;
        format DX6_LONGTITLE $50. ;
        format DX7 $7. ;
        format DX7_DESC $50. ;
        format DX7_LONGTITLE $50. ;
        format EFFECTIVE_DATE mmddyy10. ;
        format TERMINATION_DATE mmddyy10. ;
        format CCS $3. ;
        format CCS_DESC $100. ;
        format PAC_RF $10. ;
        format PAC_DESC $100. ;
        format HAC_RF $10. ;
        format HAC_DESC $100. ;
        format HCC_V21_1 $6. ;
        format HCC_V21_1_DESC $67. ;
        format HCC_V21_2 $6. ;
        format HCC_V21_2_DESC $67. ;
        format HCC_V21_3 $6. ;
        format HCC_V21_3_DESC $67. ;
        format RXHCC_1 $6. ;
        format RXHCC_1_DESC $67. ;
        format RXHCC_2 $6. ;
        format RXHCC_2_DESC $67. ;
        format CHR_FLAG $1. ;
        format POA_EXEMPT $1. ;
        format SEX_FLAG $1. ;
        format FLAG $1. ;
		format PPI_Chronic_Med $100.;
		format PPI_Acute_Med $100.;
        format KCC $5. ;
        format CJ_PERINATAL $5. ;
        format GAM_08 $5. ;
        format GAM_33 $5. ;
        format GAM_34 $5. ;
        format GAM_04 $5. ;
       	format LVC_01 $5. ;
       	format LVC_02 $5. ;
       	format LVC_04_12 $5. ;
       	format LVC_07_10 $5. ;
       	format LVC_09 $5. ;
       	format LVC_11 $5. ;
        format LVC_13 $5. ;
       	format LVC_16 $5. ;
       	format LVC_17 $5. ;
  		format HEDIS_BCS $5.;
 		format MERCK_GI $5.;
 		format LILLY_ADRD_1_2 $5.;
  		format HANGER_DFU $5.;
   		format MIPS_WHR $5.;
  		format PFIZER_ATTR_CM $5.;
		format ACOREACH_TFU $20. ;
 		format GAM_31 $20. ;
		format PPI_IP_PROC $20. ;
		format PPI_IOP_PROC $20. ;
		format PPI_IOP_MJRLE $20. ;
		format PPI_OP_PROC $20. ;
 		format PPI_PR_PROC $20. ;
 		format PPI_IP_MED $20. ;
 		format PPI_CH_MED $20. ;
 		format PPI_AC_MED $20. ;
		format PPI_CHEMO $20. ;
 		format PPI_RO $20. ;
		format PPI_PERINATAL $20. ;
input
                 DX0_DESC  $
                 DX1_DESC  $
				 final_DX1_DESC $
                 DX2_DESC  $
                 DX3  $
                 DX3_DESC  $
                 DX3_LONGTITLE  $
                 DX4  $
                 DX4_DESC  $
                 DX4_LONGTITLE  $
                 DX5  $
                 DX5_DESC  $
                 DX5_LONGTITLE  $
                 DX6  $
                 DX6_DESC  $
                 DX6_LONGTITLE  $
                 DX7  $
                 DX7_DESC  $
                 DX7_LONGTITLE  $
                 EFFECTIVE_DATE
                 TERMINATION_DATE  
                 CCS	$
                 CCS_DESC  $
                 PAC_RF  $
                 PAC_DESC  $
                 HAC_RF  $
                 HAC_DESC  $
                 HCC_V21_1  $
                 HCC_V21_1_DESC  $
                 HCC_V21_2  $
                 HCC_V21_2_DESC  $
                 HCC_V21_3  $
                 HCC_V21_3_DESC  $
                 RXHCC_1  $
                 RXHCC_1_DESC  $
                 RXHCC_2  $
                 RXHCC_2_DESC  $
                 CHR_FLAG	$
                 POA_EXEMPT  $
				 SEX_FLAG $
                 FLAG  $
				 PPI_Chronic_Med $
		  		 PPI_Acute_Med $
                 KCC  $
                 CJ_PERINATAL  $
                 GAM_08  $
				 GAM_33 $
         		 GAM_34 $
                 GAM_04  $
				 LVC_01 $
 				 LVC_02 $
 				 LVC_04_12 $
 				 LVC_07_10 $
 				 LVC_09 $
 				 LVC_11 $
 				 LVC_13 $
 				 LVC_16 $
 				 LVC_17 $
 		 		 HEDIS_BCS $
  		 		 MERCK_GI $
 		 		 LILLY_ADRD_1_2 $
				 HANGER_DFU $
				 MIPS_WHR $
				 PFIZER_ATTR_CM $
		         ACOREACH_TFU $
				 GAM_31 $
				 PPI_IP_PROC $
				 PPI_IOP_PROC $
				 PPI_IOP_MJRLE $
				 PPI_OP_PROC $
 				 PPI_PR_PROC $
				 PPI_IP_MED $
				 PPI_CH_MED $
 		         PPI_AC_MED $
				 PPI_CHEMO $
				 PPI_RO $
 		 		 PPI_PERINATAL $
  ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

data META.ICD_10_PX    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\icd_10_px.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
        informat PX3 $3. ;
        informat PX3_DESC $100. ;
        informat PX7 $7. ;
        informat PX7_DESC $100. ;
        informat PX7_LONGTITLE $100. ;
        informat PX_DESC $100. ;
        informat EFFECTIVE_DATE mmddyy10. ;
        informat TERMINATION_DATE mmddyy10. ;
        informat CCS $4. ;
        informat CCS_DESC $100. ;
        informat BPCIA $5. ;
        informat KCC $5. ;
        informat CJ_PERINATAL $5. ;
        informat LVC_02 $5. ;
        informat LVC_07_10 $5. ;
        informat LVC_09 $5. ;
        informat LVC_11 $5. ;
        informat LVC_04_12 $5. ;
        informat LVC_16 $5. ;
        informat LVC_18 $5. ;
  		informat HANGER_DFU $5.;
        informat MIPS_WHR $5.;
		informat PFIZER_ATTR_CM $5.;
        informat PPI_IOP_MJRLE $5. ;
        informat PPI_CHEMO $5. ;
		informat PPI_PERINATAL $5. ;
		format PX3 $3. ;
        format PX3_DESC $100. ;
        format PX7 $7. ;
        format PX7_DESC $100. ;
        format PX7_LONGTITLE $100. ;
        format PX_DESC $100. ;
        format EFFECTIVE_DATE mmddyy10. ;
        format TERMINATION_DATE mmddyy10. ;
        format CCS $4. ;
        format CCS_DESC $100. ;
        format BPCIA $5. ;
        format KCC $5. ;
        format CJ_PERINATAL $5. ;
        format LVC_02 $5. ;
        format LVC_07_10 $5. ;
        format LVC_09 $5. ;
        format LVC_11 $5. ;
        format LVC_04_12 $5. ;
        format LVC_16 $5. ;
        format LVC_18 $5. ;
		format HANGER_DFU $5.;
   		format MIPS_WHR $5.;
 		format PFIZER_ATTR_CM $5.;
        format PPI_IOP_MJRLE $5. ;
        format PPI_CHEMO $5. ;
		format PPI_PERINATAL $5. ;
input
                 PX3 $
                 PX3_DESC  $
                 PX7  $
                 PX7_DESC  $
                 PX7_LONGTITLE  $
				 PX_DESC $
                 EFFECTIVE_DATE
                 TERMINATION_DATE  
				 CCS $
				 CCS_DESC $
				 BPCIA $
                 KCC  $
                 CJ_PERINATAL  $
                 LVC_02  $
                 LVC_07_10  $
                 LVC_09  $
                 LVC_11  $
                 LVC_04_12  $
                 LVC_16  $
                 LVC_18  $
				 HANGER_DFU $
				 MIPS_WHR $
				 PFIZER_ATTR_CM $
				 PPI_IOP_MJRLE $
				 PPI_CHEMO $
				 PPI_PERINATAL $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

	data meta.ICD_10_PX;
set meta.ICD_10_PX;
if length(PX7)=5 then PX7='00'||PX7;
if length(PX7)=6 then PX7='0'||PX7;
run;

data META.CPT    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11\cpt.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
        informat TYPE $1. ;
        informat CPT $5. ;
        informat CPT_DESC $50. ;
        informat MEDIUM_DESC $100. ;
        informat DESC_DETAIL $346. ;
        informat add_dt mmddyy10. ;
        informat term_dt mmddyy10. ;
        informat SI $2. ;
        informat APC $4. ;
        informat SEX_FLAG $1. ;
        informat asc $1. ;
        informat MAJOR_SERV $1. ;
        informat CORE $1. ;
        informat SERVICE_DESC $10. ;
        informat BUNDLE_NAME $100. ;
        informat ppi_epi_name $100. ;
        informat BETOS_CAT $2. ;
        informat CAT_DESC $23. ;
        informat BETOS_CD $3. ;
        informat BETOS_DESC $19. ;
        informat CCS $3. ;
        informat CCS_DESC $50. ;
		informat RBCS_ID $6.;	
		informat RBCS_Cat $1.;	
		informat RBCS_Cat_Desc	$60.;
		informat RBCS_Cat_Subcat $2.;	
		informat RBCS_Subcat_Desc $60.;	
		informat RBCS_Fam_id	best12.;
		informat RBCS_Fam_Desc	$60.;
		informat SVC_DESC	$60.;
		informat RBCS_Major_Ind	$1.;
		informat RBCS_Eff_Dt mmddyy10.;	
		informat RBCS_End_Dt mmddyy10.;
        informat BPCIA $5. ;
        informat KCC $5. ;
        informat CJ_PERINATAL $5. ;
        informat GAM_08 $5. ;
        informat GAM_33 $5. ;
        informat GAM_34 $5. ;
        informat GAM_04 $5. ;
		informat NQF_0041 $5.;
		informat LVC_09 $5.;
		informat LVC_11 $5.;
		informat LVC_13 $5.;
		informat LVC_15 $5.;
		informat LVC_18 $5.;
 		informat HEDIS_BCS $5.;
		informat HANGER_DFU $5.;
		informat PFIZER_ATTR_CM $5.;
		informat ACOREACH_TFU $5. ;
		informat GAM_31 $5. ;
		informat PPI_IOP_PROC $5.;
		informat PPI_IOP_MJRLE $5.;
		informat PPI_OP_PROC $5.;
		informat PPI_PR_PROC $5.;
		informat PPI_AC_MED $5.;
		informat PPI_CH_MED $5.;
 		informat PPI_CHEMO $5.;
 		informat PPI_RO $5.;
		informat PPI_PERINATAL $5. ;
        format TYPE $1. ;
        format CPT $5. ;
        format CPT_DESC $50. ;
        format MEDIUM_DESC $100. ;
        format DESC_DETAIL $346. ;
        format add_dt mmddyy10. ;
        format term_dt mmddyy10. ;
        format SI $2. ;
        format APC $4. ;
        format SEX_FLAG $1. ;
        format asc $1. ;
        format MAJOR_SERV $1. ;
        format CORE $1. ;
        format SERVICE_DESC $50. ;
        format BUNDLE_NAME $100. ;
        format ppi_epi_name $100. ;
        format BETOS_CAT $2. ;
        format CAT_DESC $23. ;
        format BETOS_CD $3. ;
        format BETOS_DESC $19. ;
        format CCS $3. ;
        format CCS_DESC $50. ;
        format RBCS_ID $6.;	
		format RBCS_Cat $1.;	
		format RBCS_Cat_Desc	$60.;
		format RBCS_Cat_Subcat $2.;	
		format RBCS_Subcat_Desc $60.;	
		format RBCS_Fam_id	best12.;
		format RBCS_Fam_Desc	$60.;
		format SVC_DESC	$60.;
		format RBCS_Major_Ind	$1.;
		format RBCS_Eff_Dt mmddyy10.;	
		format RBCS_End_Dt mmddyy10.;
		format BPCIA $5. ;
        format KCC $5. ;
        format CJ_PERINATAL $5. ;
        format GAM_08 $5. ;
        format GAM_33 $5. ;
        format GAM_34 $5. ;
       	format GAM_04 $5. ;
		format NQF_0041 $5.;
		format LVC_09 $5.;
		format LVC_11 $5.;
 		format LVC_13 $5.;
		format LVC_15 $5.;
		format LVC_18 $5.;
		format HEDIS_BCS $5.;
		format HANGER_DFU $5.;
		format PFIZER_ATTR_CM $5.;
		format ACOREACH_TFU $5. ;
		format GAM_31 $5. ;
		format PPI_IOP_PROC $5.;
		format PPI_IOP_MJRLE $5.;
		format PPI_OP_PROC $5.;
		format PPI_PR_PROC $5.;
		format PPI_AC_MED $5.;
		format PPI_CH_MED $5.;
 		format PPI_CHEMO $5.;
  		format PPI_RO $5.;
		format PPI_PERINATAL $5. ;

   input
                 TYPE  $
                 CPT  $
                 CPT_DESC  $
                 MEDIUM_DESC  $
                 DESC_DETAIL  $
                 add_dt
                 term_dt
                 SI  $
                 APC  $
                 SEX_FLAG  $
                 asc  $
                 MAJOR_SERV  $
                 CORE  $
                 SERVICE_DESC  $
                 BUNDLE_NAME  $
				 PPI_EPI_NAME $
                 BETOS_CAT $
                 CAT_DESC  $
                 BETOS_CD  $
                 BETOS_DESC  $
                 CCS $
                 CCS_DESC  $
				 RBCS_ID $ 	
			 	 RBCS_Cat $	
		 		 RBCS_Cat_Desc	$
		 		 RBCS_Cat_Subcat $	
		 		 RBCS_Subcat_Desc $	
				 RBCS_Fam_id
				 RBCS_Fam_Desc	$
				 SVC_DESC $
				 RBCS_Major_Ind	$
				 RBCS_Eff_Dt 
		 		 RBCS_End_Dt
                 BPCIA  $
                 KCC  $
                 CJ_PERINATAL  $
                 GAM_08  $
           		 GAM_33 $ 
         		 GAM_34 $
               	 GAM_04  $
				 NQF_0041 $
				 LVC_09 $
				 LVC_11 $
				 LVC_13 $
				 LVC_15 $
				 LVC_18 $
				 HEDIS_BCS $
				 HANGER_DFU $
				 PFIZER_ATTR_CM $
  		         ACOREACH_TFU $
				 GAM_31
				 PPI_IOP_PROC $
				 PPI_IOP_MJRLE $
 				 PPI_OP_PROC $
				 PPI_PR_PROC $
				 PPI_AC_MED $
				 PPI_CH_MED $
				 PPI_CHEMO $
				 PPI_RO $
				 PPI_PERINATAL $ 

  ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

	data meta.cpt;
set meta.cpt;
if length(cpt)=3 then cpt='00'||cpt;
if length(cpt)=4 then cpt='0'||cpt;
run;
	proc sort data=meta.cpt; by cpt; run;




data META.REV    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\rev.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
        informat Rev3 $4. ;
        informat rev3_desc $51. ;
        informat REv11 $4. ;
        informat REv11_DESC $80. ;
        informat KCC $1. ;
 		informat ACOREACH_TFU $5. ;
        format Rev3 $4. ;
        format rev3_desc $51. ;
        format REv11 $4. ;
        format REv11_DESC $80. ;
        format KCC $1. ;
   		format ACOREACH_TFU $5. ;
  input
                 Rev3  $
                 rev3_desc  $
                 REv11 $
                 REv11_DESC  $
                 KCC  $
				 ACOREACH_TFU 
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;
data META.NDC    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\ndc.csv' delimiter = ',' MISSOVER DSD
 lrecl=13106 firstobs=2 ;
        informat NDC_type $1. ;
        informat NDC $11. ;
        informat BN $50. ;
        informat GNN $50. ;
        informat STR $20. ;
        informat GCDF $2. ;
        informat GCDF_DESC $50. ;
        informat GAM_33 $5. ;
        informat LILLY_ADRD_3 $5. ;
		informat PFIZER_ATTR_CM $5.;
 		informat PPI_CHEMO $5.;
 		informat PPI_RO $5.;
        format NDC_type $1. ;
        format NDC $11. ;
        format BN $50. ;
        format GNN $50. ;
        format STR $20. ;
        format GCDF $2. ;
        format GCDF_DESC $50. ;
        format GAM_33 $5. ;
        format LILLY_ADRD_3 $5. ;
		format PFIZER_ATTR_CM $5.;
  		format PPI_CHEMO $5.;
   		format PPI_RO $5.;
  input
             NDC_type $
			 NDC $
             BN  $
             GNN  $
             STR  $
             GCDF  $
             GCDF_DESC  $
			 GAM_33 $
        	 LILLY_ADRD_3 $
			 PFIZER_ATTR_CM $
			 PPI_CHEMO $
			 PPI_RO $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
	 run;

	data meta.ndc;
set meta.ndc;
if ndc_type="X" then do;
	if length(ndc)<9 then do;
		if length(ndc)=8 then ndc='0'||strip(ndc);
		if length(ndc)=7 then ndc='00'||strip(ndc);
		if length(ndc)=6 then ndc='000'||strip(ndc);
		if length(ndc)=5 then ndc='0000'||strip(ndc);
	end;
end;
else do;
	if length(ndc)<11 then do;
		if length(ndc)=10 then ndc='0'||strip(ndc);
		if length(ndc)=9 then ndc='00'||strip(ndc);
		if length(ndc)=8 then ndc='000'||strip(ndc);
		if length(ndc)=7 then ndc='0000'||strip(ndc);
		if length(ndc)=6 then ndc='00000'||strip(ndc);
		if length(ndc)=5 then ndc='000000'||strip(ndc);
	end;
end;
run;

data META.DEF_SPEC;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'D:\SASData\dua_052882\Sndbx\Jun_W\builder test\Builder v11\meta v11\def_spec.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2 ;
         informat DEF_SRC $10. ;
         informat DEF_TYPE $10. ;
         informat DEF_NAME $15. ;
         informat DEF_CAT $20. ;
         informat DEF_SUB $30. ;
         informat DEF_RULE $50. ;
         informat CHECK_BEG $15. ;
         informat CHECK_END $15. ;
         informat CHECK_BEG_OFFSET best32. ;
         informat CHECK_BEG_OFFSET_UNIT $5. ;
         informat CHECK_END_OFFSET best32. ;
         informat CHECK_END_OFFSET_UNIT $5. ;
         informat CLM_N best32. ;
         informat CLM_GAP_MIN best32. ;
         informat CLM_GAP_MAX best32. ;
         informat CLM_LOS_MIN best32. ;
         informat CLM_LOS_MAX best32. ;
         informat CLM_TYPE $20. ;
         informat SETTING $20. ;
         informat INDEX_DT $15. ;
         informat LINK $10. ;
         informat CLM_WC1 $100. ;
         informat CLM_WC2 $100. ;
         informat CODE_TYPE $15. ;
         informat CODE_POSITION $5. ;
         informat CODE  $100. ;
         informat CODE_LIST $10. ;
         informat CODE_INT $1. ;
         informat CODE_DESC $100. ;
         format DEF_SRC $10. ;
         format DEF_TYPE $10. ;
         format DEF_NAME $15. ;
         format DEF_CAT $20. ;
         format DEF_SUB $30. ;
         format DEF_RULE $50. ;
         format CHECK_BEG $15. ;
         format CHECK_END $15. ;
         format CHECK_BEG_OFFSET best12. ;
         format CHECK_BEG_OFFSET_UNIT $5. ;
         format CHECK_END_OFFSET best12. ;
         format CHECK_END_OFFSET_UNIT $5. ;
         format CLM_N best32. ;
         format CLM_GAP_MIN best32. ;
         format CLM_GAP_MAX best32. ;
         format CLM_LOS_MIN best32. ;
         format CLM_LOS_MAX best32. ;
         format CLM_TYPE $20. ;
         format SETTING $20. ;
         format INDEX_DT $15. ;
         format LINK $10. ;
         format CLM_WC1 $100. ;
         format CLM_WC2 $100. ;
         format CODE_TYPE $15. ;
         format CODE_POSITION $5. ;
         format CODE $100. ;
         format CODE_LIST $10. ;
         format CODE_INT $1. ;
         format CODE_DESC $100. ;
      input
                  DEF_SRC  $
                  DEF_TYPE  $
				  DEF_NAME $
                  DEF_CAT  $
          		  DEF_SUB $
          		  DEF_RULE $
                  CHECK_BEG  $
                  CHECK_END  $
                  CHECK_BEG_OFFSET
                  CHECK_BEG_OFFSET_UNIT  $
                  CHECK_END_OFFSET
                  CHECK_END_OFFSET_UNIT  $
                  CLM_N  
                  CLM_GAP_MIN  
                  CLM_GAP_MAX  
                  CLM_LOS_MIN  
                  CLM_LOS_MAX  
                  CLM_TYPE  $
                  SETTING  $
                  INDEX_DT  $
                  LINK  $
				  CLM_WC1 $
				  CLM_WC2 $
                  CODE_TYPE  $
                  CODE_POSITION  $
                  CODE	$
                  CODE_LIST  $
                  CODE_INT  $
                  CODE_DESC  $
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;


data META.CPT_HCC ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     infile 'C:\Users\jun.wang\Episode Builder\Episode builder v11\meta v11\cpt_hcc_2017.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
        informat proc_code $5. ;
        informat year best32. ;
        format proc_code $5. ;
        format year best32. ;
     input
                 proc_code  $
                 year
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

