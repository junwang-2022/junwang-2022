

proc sql;
    %sql_sf_connection(env=RI, db=DEV_STAGE_RAW, wh=, conn=snowcon);
	create table clmcd_raw as select * from CONNECTION TO snowcon(select top 10 *
	      from STAGE_HEALTHVERITY.MEDICAL_CCD);

proc sql;
    %sql_sf_connection(env=RI, db=SAS_SERVER_DB, wh=SAS_SERVER_DB, conn=snowcon);
	create table out.MEDICAL_CCD as select 

ClaimUID as ClaimUID length=20 label="Claim Service Line Unique ID",
input(MemberUID,12.) as MemberUID label="Member Unique ID",
input(ServiceDate,yymmdd10.) as ServiceDate format=yymmddd10. label="Service Date for the Beginning Date of Service",
input(ServiceThruDate,yymmdd10.) as ServiceThruDate format=yymmddd10. label="Date when the Service Ended",
CodeType as CodeType label="Claim Code",
OrdinalPosition as OrdinalPosition label="Ordinal Position",
CodeValue as CodeValue length=11 label="Claim Code Value",
DerivedIndicator as DerivedIndicator label='Other Data from the Claim used to Approximate the Code',
/*case when DerivedIndicator=1 then 'Y'
     when DerivedIndicator=0 then 'N' end as DerivedIndicator length=1 label='Other Data from the Claim used to Approximate the Code',*/

input(CreatedDate,yymmdd10.) as CreatedDate format=yymmddd10. label="Created Date",
		 batch_id as CJ_BatchID length=10,
		 Load_File_NM as CJ_Load_File_Nm length=123,
		 Load_File_Row_Num as CJ_Load_File_Row_Num
    from CONNECTION TO snowcon(select *
	      from DEV_STAGE_RAW.STAGE_HEALTHVERITY.MEDICAL_CCD as a
               inner join inovalon.member_list&samplesuffix. as b
			    on a.memberuid=b.memberuid
          )
         ;
quit;
