*%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\sas_init.sas";
*%include "D:\SASData\dua_052882\Sndbx\Jim_Jones\snowflake_connection.sas";

%let utilspath=&myfiles_root/dua_052882/Sndbx/Scott_W/utils;
%include "&utilspath/snowflake_connection.sas";

%let exportfolder=t:\inovalon\full;
libname out "&ExportFolder";

*%let runnumber=14; /* max runnumber is 34 */

%macro medical_clm_batch(min_batch=, max_batch=);

%do runnumber= &min_batch %to &max_batch;
	%PUT runnumber=&runnumber;
	proc sql;
	    create table xfiles as 
		select * 
		from out._files_remaining 
		where run=&runnumber;
	quit;

	data _null_;
	   set xfiles end=lastobs;
	   length xstr $ 20000;
	   retain xstr "";
	   xstr = strip(xstr) ||"'"|| strip(LOAD_FILE_NM)||"'";
	   if lastobs then call symput('filenames',strip(xstr));
	   else xstr=strip(xstr)||",";
   run;

	proc sql;
	    %sql_sf_connection(env=RI, db=SAS_SERVER_DB, wh=, conn=snowcon);
		create table out.MEDICAL_CLM&runnumber. as 
		select 
			ClaimUID as ClaimUID length=20 label="Claim Service Line ID",
			input(MemberUID,12.) as MemberUID label="Member Unique ID",
			ProviderUID as ProviderUID label="Provider Unique ID",
			ClaimStatusCode as ClaimStatusCode length=1 label="Claim Payment Status",
			input(ServiceDate,yymmdd10.) as ServiceDate format=yymmddd10. label="Claim Service Start Date",
			input(ServiceThruDate,yymmdd10.) as ServiceThruDate format=yymmddd10. label="Claim Service End Date",
			UBPatientDischargeStatusCode as UBPatientDischargeStatusCode length=2 label="National Uniform Billing Committee (UB) Patient Discharge Status Code",
			ServiceUnitQuantity as ServiceUnitQuantity label="Quantity per Service Unit",
			DeniedDaysCount as DeniedDaysCount label="Days Not Covered (Inpatient Claims Only)",
			input(BilledAmount,best32.) as BilledAmount label="Claim Billed Amount",
			input(AllowedAmount,best32.) as AllowedAmount label="Claim Allowed Amount",
			input(CopayAmount,best32.) as CopayAmount label="Amount the Member is Responsible to Pay",
			input(PaidAmount,best32.) as PaidAmount label="Amount the Insurance Company Actually Paid",
			input(CostAmount,best32.) as CostAmount label="",
			RxProviderIndicator as RxProviderIndicator length=3 label="Claim Provider has Prescribing Privileges for the MCO Members",
			/*case when RxProviderIndicator=1 then "Y"
			     when RxProviderIndicator=0 then "N" end as RXProviderIndicator label="Claim Provider has Prescribing Privileges for the MCO Members",*/
			PCPProviderIndicator as PCPProviderIndicator length=3 label="Claim Provider serves as a PCP for the Health Plan",
			/*case when PCPProviderIndicator=1 then "Y",
			     when PCPProviderIndicator=0 then "N" end as PCPProviderIndicator length=1 label="Claim Provider serves as a PCP for the Health Plan",*/
			RoomBoardIndicator as RoomBoardIndicator length=3 label="Claim is for Room and Board Service",
			/*case when RoomBoardIndicator=1 then 'Y'
			     when RoomBoardIndicator=0 then 'N' end as RoomBoardIndicator label="Claim is for Room and Board Service",*/
			MajorSurgeryIndicator as MajorSurgeryIndicator length=3 label="Claim includes a Procedure Code considered as a Major Surgery",
			/* case when MajorSurgeryIndicator=1 then "Y"
			     when MajorSurgeryIndicator=0 then "N" end as MajorSurgeryIndicator label="Claim includes a Procedure Code considered as a Major Surgery",*/
			ExcludeFromDischargeIndicator as ExcludeFromDischargeIndicator length=3 label="Claim should be excluded from Discharge",
			/*case when ExcludeFromDischargeIndicator=1 then 'Y'
			     when ExcludeFromDischargeIndicator=0 then 'N' end as ExcludeFromDischargeIndicator label="Claim should be excluded from Discharge",*/
			ClaimFormTypeCode as ClaimFormTypeCode length=1 label="Indicates type of Claim Form",
			InstitutionalTypeCode as InstitutionalTypeCode length=1 label="Type of Institutional Service",
			ProfessionalTypeCode as ProfessionalTypeCode length=1 label="Yype of Professional Service",
			BillingProviderUID as BillingProviderUID label="Provider Billing Unique ID",
			RenderingProviderUID as RenderingProviderUID label="Provider Rendering Service Unique ID",
			RenderingProviderNPI as RenderingProviderNPI length=15 label="Provider Rendering Service Unique ID National Provider Identification Number",
			BillingProviderNPI as BillingProviderNPI length=15 label="Billing Provider National Provider Identification Number",
			input(sourcemodifieddate,yymmdd10.) as sourcemodifieddate format=yymmddd10. label="Source Last Modified Date",
			/*claimnumber as ClaimNumber length=64 label="Claim Header Number",
			claimlinenumber as ClaimLineNumber length=20 label="Claim Service Line Number",*/
			input(CreatedDate,yymmdd10.) as CreatedDate format=yymmddd10. label="Created Date" ,
			 batch_id as CJ_Batch_ID length=10,
					load_file_nm as CJ_Load_File_nm length=123	
			from CONNECTION TO snowcon(
				select a.*
				from DEV_STAGE_RAW.STAGE_HEALTHVERITY.MEDICAL_CLM as a
				where load_file_nm in(&filenames)
			/*inner join inovalon.ALL_MEMBERUIDS_A as b
						  on a.memberuid=b.memberuid
			               /*inner join (Select distinct claimuid  from DEV_STAGE_RAW.STAGE_HEALTHVERITY.MEDICAL_CCD as c
			               inner join inovalon.member_list&samplesuffix as b
						    on c.memberuid=b.memberuid) as d
							on a.claimuid=d.claimuid*/
						   );
	quit;
%end;

%mend medical_clm_batch;
*%medical_clm_batch(min_batch=14, max_batch=34);
%medical_clm_batch(min_batch=23, max_batch=23);
%medical_clm_batch(min_batch=25, max_batch=25);
