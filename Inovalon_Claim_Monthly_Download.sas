
libname IDs 't:\inovalon\IDs';
libname Full 't:\inovalon\full';

%include "d:\sasdata\dua_052882\users\jim_jones\snowflake_connection.sas";
%include "d:\sasdata\dua_052882\sndbx\Jim_Jones\JJ_Utils.sas";

%macro create_month_dates(year=, month=, month_list_var=);
	%local start_date end_date mlist dt;
	%global &month_list_var;
	%let start_date=%sysfunc(mdy(&month,1,&year));
	%let end_date=%sysfunc(intnx(month, &start_date,0,E));

	%do x= &start_date %to &end_date;
		%let dt=%str(%')%sysfunc(putn(&x, yymmdd10.))%str(%');
		%if &x=&start_date %then %let mlist=&dt;
		%else %let mlist=&mlist, &dt;
	%end;
	%let &month_list_var=&mlist;
%mend create_month_dates;

%macro split_medical_clm;
%local std_year std_month;

%do std_year = 2020 %to 2020;
	%do std_month =1 %to 2;
		
		%create_month_dates(year=&std_year, month=&std_month, month_list_var=date_list);

		proc sql;
	    %sql_sf_connection(env=RI, db=SAS_SERVER_DB, wh=, conn=snowcon);
		create table full.MEDICAL_CLM&std_year._&std_month. as 
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
			input(CreatedDate,yymmdd10.) as CreatedDate format=yymmddd10. label="Created Date" ,
			/* batch_id as CJ_Batch_ID length=10,*/
			load_file_nm as CJ_Load_File_nm length=123	
			from CONNECTION TO snowcon(
				select top 10 *
				from SAS_SERVER_DB.INOVALON.MEDICAL_CLM_&std_year._&std_month/*DEV_STAGE_RAW.STAGE_HEALTHVERITY.MEDICAL_CLM*/ as a
				where servicethrudate in (&date_list))

						   ;/*			execute(
				create or replace table SAS_SERVER_DB.INOVALON.MEDICAL_CLM_&std_year._&std_month AS 
				select *
				from stage_healthverity.medical_clm
				where servicethrudate in (&date_list)
			) by snowcon;*/
		quit;
	%end;
%end;
%mend split_medical_clm;
%split_medical_clm;
