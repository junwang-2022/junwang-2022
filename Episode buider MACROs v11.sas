

****************************************************;
************ macros for episode builder ************;
****************************************************;

%macro check_max;
%local code_max curr_max close;

proc sql; select distinct case when index(code, "MAX")>0 then code_type
	 					  when index(clm_wc1, "=MAX")>0 then tranwrd(clm_wc1,"=MAX","")
	 					  when index(clm_wc2, "=MAX")>0 then tranwrd(clm_wc2,"=MAX","") end
			  into:code_max separated by " " 
			  from out.def_to_run a, meta.def_spec b
			  where a.def_name=b.def_name and ((index(b.code_type, "_AMT")>0 and index(b.code, "MAX")>0)
		  										or (index(b.clm_wc1, "_AMT")>0 and index(b.clm_wc1, "=MAX")>0)
												or (index(b.clm_wc2, "_AMT")>0 and index(b.clm_wc2, "=MAX")>0)); quit;
%if &sqlobs.>0 %then %do;
	%do i=1 %to &sqlobs.;
		%let curr_max=%scan(&code_max.,&i.);
/*		%if not %sysfunc(varnum(%sysfunc(open(serviceline)), max_&curr_max.)) %then %do;*/
		proc sql; select count(*)>0 into:var_exist from sashelp.vcolumn 
			where libname='WORK' and memname='SERVICELINE' and upcase(name)=%upcase("max_&curr_max."); quit;
		%if not &var_exist. %then %do;
		proc sql;
		create table serviceline as
		select *, case when &curr_max.>0 and &curr_max.=max(&curr_max.) then 1 end as max_&curr_max.
		from serviceline group by syskey; quit;
		%end;
	%end;
%end;
%mend check_max;

%macro reset_global_macro_var;

%symdel task def_src def_type def_name base base_mod out out_mod exist_trig ch/nowarn;
%symdel src_medical src_serviceline src_diagnosis src_procedure src_pharmacy src_enroll src_esrd /nowarn;
%symdel prf_beg prf_end cenroll_type cmin_age cmax_age index_beg index_end /nowarn;
%symdel t_data t_var/nowarn;

%global task def_src def_type def_name base base_mod out out_mod exist_trig ch;
%global src_medical src_serviceline src_diagnosis src_procedure src_pharmacy src_enroll src_esrd ;
%global prf_beg prf_end cenroll_type cmin_age cmax_age index_beg index_end ;
%global t_data t_var;

%let src_medical		=medical;		
%let src_serviceline	=serviceline;	
%let src_diagnosis		=diagnosis;	
%let src_procedure		=procedure;
%let src_pharmacy		=pharmacy;	

%let src_enroll			=&enroll;
%let src_esrd			=&esrd;

%mend reset_global_macro_var;

%macro prep_def;

proc sql noprint; 
select def_src, def_type, def_name, enroll_type, min_age, max_age, 
	   input(put(prf_beg,mmddyy10.), mmddyy10.), input(put(prf_end,mmddyy10.), mmddyy10.)
into :def_src, :def_type, :def_name, :cenroll_type, :cmin_age, :cmax_age, :prf_beg, :prf_end								
from curr_def_to_run;
quit;

proc sql; create table def as 
select monotonic() as id, b.* 
	from curr_def_to_run a, meta.def_spec b
	where a.def_name=b.def_name 
	order by id; 
quit;

data def; 
	set def; 
	if missing(check_beg_offset) then check_beg_offset=0;
	if missing(check_end_offset) then check_end_offset=0;
	if missing(check_beg_offset_unit) then check_beg_offset_unit="DAY";
	if missing(check_end_offset_unit) then check_end_offset_unit="DAY";
	/*replace def default value with customized value*/
	%if %sysfunc(anyalnum("&cmin_age"))	%then %do; if code_type="AGE_MIN" then code="&cmin_age."; %end;
	%if %sysfunc(anyalnum("&cmax_age"))	%then %do; if code_type="AGE_MAX" then code="&cmax_age."; %end;
	%if %sysfunc(anyalnum("&cenroll_type"))	%then %do; if code_type="ENROLL_GAP" then setting="&cenroll_type."; %end;
run;

proc sql noprint; 
select 
case when sum(index(def_type,"MEASURE"))>0 then "DENOMINATOR" 
	 when sum(index(def_type,"EPISODE"))>0 then "TRIGGER" 
	 when sum(index(def_type,"COHORT"))>0 then "COHORT" end, 
case when sum(index(def_type,"MEASURE"))>0 then "DENOM" 
	 when sum(index(def_type,"EPISODE"))>0 then "TRIG" 
	 when sum(index(def_type,"COHORT"))>0 then "COH" end, 
case when sum(index(def_type,"MEASURE"))>0 then "NUMERATOR" 
	 when sum(index(def_type,"EPISODE"))>0 then "EPISODE" 
	 when sum(index(def_type,"COHORT"))>0 then "SERVICE" end, 
case when sum(index(def_type,"MEASURE"))>0 then "NUM" 
	 when sum(index(def_type,"EPISODE"))>0 then "EPI" 
	 when sum(index(def_type,"COHORT"))>0 then "SVC" end, 
sum(index(def_cat,"DENOM_IN"))>0 or sum(index(def_cat,"TRIG_IN"))>0 or sum(index(def_cat,"COH_IN"))>0
into :base, :base_mod, :out, :out_mod, :exist_trig
from def; quit;

%let base=%trim(&base.);
%let base_mod=%trim(&base_mod.);
%let out=%trim(&out.);
%let out_mod=%trim(&out_mod.);

%let def_src=%trim(&def_src.);
%let def_type=%trim(&def_type.);
%let def_name=%trim(&def_name.);
%let cenroll_type=%trim(&cenroll_type.);

%if &def_name.=PPI_CH_MED %then %let ch=1; %else %let ch=0;;

%mend prep_def;

%macro read_def(tbl,def_cat,code_type);
/*read in the def lines and put values into appropriate macro variables*/
/*run statement is left out on purpose due to scope of macro variable*/
%local x currname namelst;
proc sql noprint;
select name into:namelst separated by " " from sashelp.vcolumn
where libname="WORK" and memname="%upcase(&tbl.)"; quit;

data _null_; 
	set &tbl.;
	%if &def_cat. ne all %then where index(def_cat,"&def_cat.")>0; %if "&code_type" ne "" %then and code_type="&code_type.";;
	%do x=1 %to %sysfunc(countw(&namelst.));
		%let currname=%scan(&namelst.,&x.);
		call symputx("&currname"||put(_n_,5. -l),&currname.);
	%end;
	call symputx("cnt",_n_);
/*run;*/
%mend read_def;
%macro parse_code;
/*read in all related parameters from current definition and prepare the whole codelst*/
%local cnt var i j st ed m;

proc sql; 
create table sub as
select b.* from curr_def_to_run a, meta.def_list b
	where a.def_name=b.def_name; quit;

%read_def(def, all, ); run;
%do i=1 %to &cnt.;
	%if &&def_sub&i=Y or %index(&&def_sub&i, &def_name.) %then %do;
		%if %index(&&code&i,SUB_F) %then %do; %let var=code; %end;
		%else %if %index(&&code_list&i,SUB_F) %then %do; %let var=code_list; %end;
		%else %do; %let var= ; %end;;
		%if &var. ne %then %do;
			%if &&def_sub&i=Y %then %do;
			proc sql; 
			create table def_sub_&i. as
			select distinct a.*, b.def_sub, b.&&&&&var.&i. as &var.
				from def(where=(id=&i.) drop=def_sub &var.) a
				left join sub b on b.def_sub^="" and b.&&&&&var.&i.^=""; 
				quit;
			%end;
			%if %index(&&def_sub&i, &def_name.) %then %do; 
			proc sql; 
			create table def_sub_&i. as
			select distinct a.*, b.&&&&&var.&i. as &var.
				from def(where=(id=&i.) drop=&var.) a
				left join sub b on b.def_sub=a.def_sub and b.&&&&&var.&i.^=""; 
				quit;
			%end;
		%end;
		%else %do; 
			%if &&def_sub&i=Y %then %do;
			proc sql; 
			create table def_sub_&i. as
			select distinct a.*, b.def_sub
				from def(where=(id=&i.) drop=def_sub) a
				left join sub b on b.def_sub^=""; 
				quit;
			%end;
			%if %index(&&def_sub&i, &def_name.) %then %do; 
			proc sql; 
			create table def_sub_&i. as
			select distinct a.*
				from def(where=(id=&i.)) a
				left join sub b on b.def_sub=a.def_sub; 
				quit;
			%end;
		%end;
	%end;
	%else %do;
	data def_sub_&i; set def; where id=&i.; run;
	%end;
%end;

data def_sub; set def_sub_:; run;
proc sort data=def_sub; by id def_sub; run;
proc datasets lib=work nolist; delete def_sub_:; quit; 

%read_def(def_sub, all, ); run;

%do i=1 %to &cnt.;
	proc datasets lib=work nolist; delete temp; quit;

	%let t_data=&src_medical; %let t_var=&&code_type&i; 
	%if &&def_cat&i=LEVEL or %index(&&def_cat&i,&base.) or %index(&&def_cat&i,&out.)
		%then %do; %let t_data=; %let t_var=; %end;
	%else %if %quote(&&clm_type&i) = RX  %then %do; %let t_data=&src_pharmacy; %end;
	%else %do;
		%if %index(&&code_type&i,DX)    %then %do;	
			%if &&code_position&i = 1 	%then %do;  %let t_var=PDX_CD;	%end;
										%else %do; 	%let t_data=&src_diagnosis;   %let t_var=DX_CD;    %end;
		%end;
		%if %index(&&code_type&i,PX)    %then %do;	
			%if &&code_position&i = 1 	%then %do;  %let t_var=PPX_CD;	%end;
										%else %do; 	%let t_data=&src_procedure;   %let t_var=PX_CD;    %end;
		%end;
		%if &&code_type&i=PC     		%then %let t_var=PDX_CD;;
		%if &&code_type&i=LINE_DX_CD    %then %let t_var=LINE_DX_CD;;
		%if %index(&&code_type&i,LINE_) or %index(&&code_position&i,LINE) %then %let t_data=&src_serviceline;;
		%if &&code_type&i=CPT     		%then %let t_data=&src_serviceline;;
		%if &&code_type&i=CPT_MOD 		%then %do;	%let t_data=&src_serviceline; %let t_var=%str(cpt_mod_1 cpt_mod_2 cpt_mod_3 cpt_mod_4);	%end;
		%if &&code_type&i=REV_CD     	%then %let t_data=&src_serviceline;;
		%if &&code_type&i=REV_STUS_IND  %then %let t_data=&src_serviceline;;
		%if %index(&&code_type&i,AGE) 	%then %let t_var=AGE;;
		%if %index(&&code_type&i,DOB) 	%then %let t_var=bene_dob_dt;;
		%if %index(&&code_type&i,DOD) 	%then %let t_var=bene_dod_dt;;
		%if %quote(&&clm_type&i) = ENROLL  %then %let t_data=enroll;;
	%end;

	%if &&code_list&i eq %then %do; 
		data temp;
			id=input("&&id&i",6.); 
			length def_sub $30 code $100 t_data $50 t_var $50;
			def_sub="&&def_sub&i" ;
			t_data="&t_data";
			t_var="&t_var";
			%if %quote(&&code&i) ne %then %do;
				%do p=1 %to %sysfunc(countw(&&code&i,'/')); 
					code="%scan(&&code&i,&p,'/')"; 
					%if %index(%scan(&&code&i,&p,'/'),-) %then %do;
						%let st=%scan(%scan(&&code&i,&p,'/'),1,'-');
						%let st=%substr(&st.,%length(&st.));
						%let ed=%scan(%scan(&&code&i,&p,'/'),2,'-');
						%let ed=%substr(&ed.,%length(&ed.));
						%let m=%scan(%scan(&&code&i,&p,'/'),1,'-');
						%let m=%substr(&m.,1,%length(&m.)-1);
							%do j=&st. %to &ed.; 
								code="&m."||"&j."; output; 
							%end;
					%end;
					%else %do; output; %end;
				%end;
			%end;
			%else %do; code=""; %end;
		run;

	%if &&code_type&i=PC %then %do;
			proc sql; create table temp as
			select a.id, a.def_sub, b.dx7 as code, a.t_data, a.t_var
			from temp a, meta.&t_mdata. b
			where a.code=b.pac_rf;
			quit;
	%end;
		proc append base=whole_codelst data=temp force; run; 
	%end;

	%if &&code_list&i ne %then %do;
		%let t_mdata=; %let t_mvar=;
	/*		xwalk code_type to master code list tables and variables */
		%if &&code_type&i=DX 	 %then %do; 	%let t_mdata=icd_10_dx; %let t_mvar=dx7;  %end;
		%if &&code_type&i=LINE_DX_CD 	 %then %do; 	%let t_mdata=icd_10_dx; %let t_mvar=dx7;  %end;
		%if &&code_type&i=PX 	 %then %do; 	%let t_mdata=icd_10_px; %let t_mvar=px7;  %end;
		%if &&code_type&i=PC 	 %then %do; 	%let t_mdata=icd_10_dx; %let t_mvar=pac_rf;  %end;
		%if &&code_type&i=CPT 	 %then %do; 	%let t_mdata=cpt;		%let t_mvar=cpt;  %end;
		%if &&code_type&i=REV_CD %then %do; 	%let t_mdata=rev;	 	%let t_mvar=rev4; %end;
		%if &&code_type&i=MS_DRG %then %do; 	%let t_mdata=ms_drg;	%let t_mvar=ms_drg;  %end;
		%if &&code_type&i=NDC 	 %then %do; 	%let t_mdata=ndc;		%let t_mvar=ndc;	 %end;

		%if &&code_type&i ne and &t_mdata. ne and %length(&t_mvar.) ne 0 %then %do;
			proc sql; create table temp as
			select input("&&id&i",6.) as id, 
				   "&&def_sub&i" as def_sub length=30, 
				   &t_mvar. as code length=100 format=$100., 
				   "&t_data" as t_data length=50,
				   "&t_var" as t_var length=50 
			from meta.&t_mdata. 
			where &def_name. contains "&&code_list&i";
			quit;
			proc append base=whole_codelst data=temp force; run; 
		%end;
	%end;
%end;

proc sql;
create table whole_codelst as
select distinct a.*, b.*
from def_sub(drop=code) a, whole_codelst b
where a.id=b.id and a.def_sub=b.def_sub;
quit;

%mend parse_code;

%macro offset(p);
/*parse def check_beg/end check_beg/end_offset and check_beg/end_offset_unit */
/*it returns 2 macro variables check_beg&p and check_end&p*/
%global search_index_claim search_index_case;
%let search_index_claim=0; %let search_index_case=0;

%if %index(&&def_cat&p, &base_mod._IN) or &&check_beg&p=PRF_BEG or &&check_beg&p=PRF_END %then %do;
	%let check_beg&p =intnx("&&check_beg_offset_unit&p", &&&&&&check_beg&p, &&check_beg_offset&p, 'S');
	%let check_end&p =intnx("&&check_end_offset_unit&p", &&&&&&check_end&p, &&check_end_offset&p, 'S');
%end;

%else %do;
	%let search_index_claim=%index(&&check_beg&p, INDEX_CLAIM);
	%let search_index_case=%index(&&check_beg&p, INDEX_CASE);
	%if &search_index_claim. or &search_index_case. %then %return;

	%if %quote(&&check_beg&p) ne %then 
		%let check_beg&p =intnx("&&check_beg_offset_unit&p", &&check_beg&p, &&check_beg_offset&p, 'S');
	%else %put note: check_beg is empty!;
	%if %quote(&&check_end&p) ne %then 
		%let check_end&p =intnx("&&check_end_offset_unit&p", &&check_end&p, &&check_end_offset&p, 'S');
	%else %put note: check_end is empty!;
%end;
%mend offset;

%macro separatebar(in_var); 
/*to convert multiple codes with '/' (e.g. IP/OP) into separate codes (e.g. "IP", "OP")*/
%local p holder; %let p=2;
%if %quote(&&&in_var) ne  %then %let holder="%scan(&&&in_var,1)";
%do %while (%scan(&&&in_var,&p) ne);
	%let holder=&holder, "%scan(&&&in_var,&p)";
	%let p=%eval(&p+1);
%end;
%let &in_var=&holder;
%if &&&in_var ne %then %put note: separate &in_var into &&&in_var;
%else %put note: &in_var is empty!;
%mend separatebar;

%macro all_module(def_mod);
%local all_tbl tbl batchlst tbllst varlst wclst1 wclst2 namelst currtbl t i j x b exist_max max_var;

proc sort data=&def_mod._codelst;
by check_beg check_beg_offset check_beg_offset_unit check_end check_end_offset check_end_offset_unit;
quit;

data &def_mod._codelst;
set &def_mod._codelst;
by check_beg check_beg_offset check_beg_offset_unit check_end check_end_offset check_end_offset_unit;
retain batch;
if first.check_end_offset_unit then batch+1;
run;

proc sql noprint; select distinct batch into :batchlst separated by " " from &def_mod._codelst; quit;

%do b=1 %to %sysfunc(countw(&batchlst.));

proc sql noprint; select distinct t_data into :tbllst separated by " " from &def_mod._codelst 
	where batch=&b. and t_data^=""; quit;

proc datasets lib=work nolist; delete &def_mod._batch&b.; quit;

	%do i=1 %to %sysfunc(countw(&tbllst.)); 
		%let currtbl=%scan(&tbllst.,&i.," ");
		proc datasets lib=work nolist; delete matched_tbl_&currtbl. tbl_def; quit;

	proc sql noprint; select distinct t_var into :varlst separated by " " from &def_mod._codelst 
		where batch=&b. and t_data="&currtbl."; quit;
	proc sql noprint; select distinct scan(clm_wc1,1,"~^<=>"), scan(clm_wc2,1,"~^<=>") into :wclst1 separated by " ",  :wclst2 separated by " "
		from &def_mod._codelst where batch=&b. and t_data="&currtbl."; quit;

	proc sql; create table tbl_def as select distinct * from &def_mod._codelst(drop=code) 
		where batch=&b. and t_data="&currtbl."; quit;

	proc sql noprint; select name into:namelst separated by " " from sashelp.vcolumn
		where libname="WORK" and memname="TBL_DEF"; quit;

	proc sql; select distinct case when index(code, "MAX")>0 then "max_"||t_var
	 					  when index(clm_wc1, "=MAX")>0 then "max_"||tranwrd(clm_wc1,"=MAX","")
	 					  when index(clm_wc2, "=MAX")>0 then "max_"||tranwrd(clm_wc2,"=MAX","") end,
					 count(*)>0 into:max_var separated by " ", :exist_max 
			  from &def_mod._codelst
			  where batch=&b. and t_data="serviceline" and ((index(t_var, "_AMT")>0 and index(code, "MAX")>0)
		  											 or (index(clm_wc1, "_AMT")>0 and index(clm_wc1, "=MAX")>0)
												 	 or (index(clm_wc2, "_AMT")>0 and index(clm_wc2, "=MAX")>0)); quit;
	data _null_; 
		set tbl_def;
		%do x=1 %to %sysfunc(countw(&namelst.));
			%let currname=%scan(&namelst.,&x.);
			call symputx("&currname"||put(_n_,5. -l),&currname.);
		%end;
		call symputx("cnt",_n_);
	run;

	%do j=1 %to &cnt.;
		data codelst;
		set &def_mod._codelst;
		where batch=&b. and t_data="&currtbl." and id=&&id&j and def_sub="&&def_sub&j" and t_var="&&t_var&j";
		run;

		proc sql noprint; select ceil(count(*)/5000) into :grp&j from codelst; quit;

		%do x=1 %to &&grp&j.;
			%let st=%eval(&x*5000-4999);
			%let end=%eval(&x*5000);
			proc sql noprint; select distinct "'"||strip(code)||"'" into :codelst&j&x separated by " " 
			from codelst(firstobs=&st. obs=&end.); quit;
		%end;
		%separatebar(clm_type&j);	%separatebar(setting&j);	
	%end;

	%offset(1);	
		%if &index_dt1. eq  %then %let index_dt1=THRU_DT;;
		%if %index(&def_cat1, EPI_IN) %then %let index_dt1=FROM_DT;;
		%if %quote(&clm_type1)="RX"  %then %let index_dt1=FILL_DT;;

	%if &exist_trig. and &post_trig. %then %let src_&currtbl.=out.&def_name._&currtbl.;;

	%if &currtbl.=enroll %then %do;
	data matched_tbl_&currtbl.;
	set &src_enroll.;
	length def_sub $30 code_type $15 code $100 code_apc $5 code_int $1 link $10 module $20 table $15 rule $20;
	%do j=1 %to &cnt.;
		if	bene_id^=""
			%if "&&clm_wc1&j" ne ""  %then %do; %if %index(&&clm_wc1&j, MAX) %then %do; and &max_var.=1 %end; %else %do; and &&clm_wc1&j %end; %end;
			%if "&&clm_wc2&j" ne ""  %then %do; %if %index(&&clm_wc2&j, MAX) %then %do; and &max_var.=1 %end; %else %do; and &&clm_wc2&j %end; %end;
			%if not &ch. %then %do; %if &post_trig. and &&def_sub&j. ne and &&def_sub&j. ne ALL %then and def_sub="&&def_sub&j."; %end;
			%if %index(&&code_type&j, AGE) %then %do;
				and (&check_beg1.-bene_dob_dt)/365.25 %if %index(&&code_type&j,MIN) %then >=; %if %index(&&code_type&j,MAX) %then <=; input(&&codelst&j.1,6.) %end;
			%if &&code_type&j=DOD or &&code_type&j=DOB %then and &check_beg1. <= &&t_var&j. <= &check_end1.;
		then do;
			id=&&id&j;  
			%if &&def_sub&j. ne %then def_sub="&&def_sub&j";;
			code_type="&&code_type&j"; code_int="&&code_int&j";
			%if &&codelst&j.1 ne '' %then code=%scan(&&t_var&j,1); %else code="";;  
			link= %if &&link&j=LINE_NUM %then put(&&link&j,3.); %else %if &&link&j ne %then &&link&j; %else "";; 
			%if &currtbl.=diagnosis or &currtbl.=procedure %then allowed_amt=.;;
			%if &currtbl.=serviceline %then code_apc=apc_cd; %else code_apc="";;
			module="&&def_cat&j"; index_dt=&index_dt1.; format index_dt mmddyy10.;
			table="&currtbl."; rule="&&def_rule&j.";
			output;
		end;
	%end;
	run;
	%end;

	%else %do;
	data matched_tbl_&currtbl.;
	set &&src_&currtbl.
	(keep=bene_id syskey clm_type case_id &varlst. &wclst1. &wclst2.
		%if &currtbl.=pharmacy %then fill_dt days_suply allowed_amt; %else from_dt thru_dt setting dnl_flag; 
		%if &currtbl.=medical %then adm_dt dis_dt allowed_amt discharge; 
		%if &currtbl.=serviceline %then %do; svc_thru_dt svc_from_dt line_num allowed_amt apc_cd %if &exist_max. %then &max_var.; %end;
		%if &currtbl.=diagnosis or &currtbl.=procedure %then seq_num; 
		%if &post_trig. %then bene_dob_dt bene_dod_dt def_sub def_id index_beg index_end ana_beg ana_end index_claim index_case index_discharge;
	);
	where %if &search_index_claim. %then index_claim=1; 
		  %else %if &search_index_case. %then index_case=1;
		  %else &check_beg1<=&index_dt1<=&check_end1;;
	length def_sub $30 code_type $15 code $100 code_apc $5 code_int $1 link $10 module $20 table $15 rule $20;

	%do j=1 %to &cnt.;
		if	%if &&clm_type&j ne	  %then clm_type in (&&clm_type&j); %else clm_type^="";
			%if "&&t_var&j" ne "" %then %do;
			%if &&t_var&j=ER_FLAG or &&t_var&j=ICU_FLAG %then and &&t_var&j=1;
			%else %if %index(&&t_var&j,_AMT) %then %do;
					%if &currtbl.=serviceline and %index(&&codelst&j.1,MAX) %then and max_&&t_var&j.=1;
					%else and &&t_var&j %sysfunc(compress(&&codelst&j.1,%str(%')));
			%end;
			%else %if not %index(&&code_type&j, AGE) %then %do; 
					%if (%index(&&code_type&j, DX) or %index(&&code_type&j, PX) or %index(&&code_type&j, NDC)) %then %do; 
					and &&t_var&j in: (%do x=1 %to &&grp&j; &&codelst&j&x %end;) %end;
					%else %do;
					/*for cpt_mod_1-4 */
					 and (%scan(&&t_var&j,1) in (%do x=1 %to &&grp&j; &&codelst&j&x %end;)
						%do p=2 %to %sysfunc(countw(&&t_var&j)); or %scan(&&t_var&j,&p.) in (%do x=1 %to &&grp&j; &&codelst&j&x %end;) %end;)
					%end; 
				%end;
			%end;
			%if &&setting&j ne	  	  %then and setting in (&&setting&j);
			%if &&code_position&j eq 2 %then and seq_num ne 1;
			%if &&clm_los_min&j ne .  %then and thru_dt-from_dt+1 >= &&clm_los_min&j;
			%if &&clm_los_max&j ne .  %then and thru_dt-from_dt+1 <= &&clm_los_max&j;
			%if "&&clm_wc1&j" ne ""  %then %do; %if %index(&&clm_wc1&j, MAX) %then %do; and &max_var.=1 %end; %else %do; and &&clm_wc1&j %end; %end;
			%if "&&clm_wc2&j" ne ""  %then %do; %if %index(&&clm_wc2&j, MAX) %then %do; and &max_var.=1 %end; %else %do; and &&clm_wc2&j %end; %end;
			%if not &ch. %then %do; %if &post_trig. and &&def_sub&j. ne and &&def_sub&j. ne ALL %then and def_sub="&&def_sub&j."; %end;
			%if %index(&&code_type&j, AGE) %then %do;
				and (&check_beg1.-bene_dob_dt)/365.25 %if %index(&&code_type&j,MIN) %then >=; %if %index(&&code_type&j,MAX) %then <=; input(&&codelst&j.1,6.) %end;
			%if &&code_type&j=DOD or &&code_type&j=DOB %then and &&t_var&j. between &check_beg1. and &check_end1.;
		then do;
			id=&&id&j;  
			%if &&def_sub&j. ne %then def_sub="&&def_sub&j";;
			code_type="&&code_type&j"; code_int="&&code_int&j";
			%if &&codelst&j.1 ne '' %then code=%scan(&&t_var&j,1); %else code="";;  
			link= %if &&link&j=LINE_NUM %then put(&&link&j,3.); %else %if &&link&j ne %then &&link&j; %else "";; 
			%if &currtbl.=diagnosis or &currtbl.=procedure %then allowed_amt=.;;
			%if &currtbl.=serviceline %then code_apc=apc_cd; %else code_apc="";;
			module="&&def_cat&j"; index_dt=&index_dt1.; format index_dt mmddyy10.;
			table="&currtbl."; rule="&&def_rule&j.";
			output;
		end;
	%end;
	run;
	%end;

	%if %index(&def_cat1., &base_mod._IN) %then %do;
		data matched_tbl_&currtbl.;
			set matched_tbl_&currtbl.;
			format index_beg index_end mmddyy10.;
			index_end=&index_dt1.; index_beg=&index_dt1.;
			%if &index_dt1.=DIS_DT %then index_beg=coalesce(adm_dt,from_dt);;
			%if &index_dt1.=SVC_THRU_DT %then index_beg=coalesce(svc_from_dt, svc_thru_dt);;	
			%if &index_dt1.=THRU_DT %then index_beg=coalesce(from_dt, thru_dt);;
			%if &index_dt1.=ADM_DT %then index_end=coalesce(dis_dt,thru_dt);;
			%if &index_dt1.=SVC_FROM_DT %then index_end=coalesce(svc_thru_dt, svc_from_dt);;	
			%if &index_dt1.=FROM_DT %then index_end=coalesce(thru_dt, from_dt);;
			%if &index_dt1.=FILL_DT %then index_beg=fill_dt;;
			length def_id $30; 	%if &ch. %then def_id=bene_id; %else if syskey^=. then def_id=strip(put(syskey,15.));;
			if def_id="" then def_id=bene_id;
			%if &currtbl.=pharmacy %then %do; setting="    "; %end;
		run;
	%end;

	proc sort data=matched_tbl_&currtbl. nodupkey; by bene_id id def_sub %if &post_trig. %then def_id; syskey link code_int module; run;

	%end;
	data &def_mod._batch&b.; set matched_tbl_:; run;
	proc datasets lib=work nolist; delete matched_tbl_:; quit;
%end;
data &def_mod.; set &def_mod._batch:; run;
proc datasets lib=work nolist; delete &def_mod._batch:; quit;
%mend;

%macro proc_code_int(currmod,in_data);
/*process have/not have, and/or logic specified by code_int column*/
%local p n_incl incl_lst n_excl excl_lst lst curr_set all;

proc sql noprint; select count(*) into:int from def 
where def_cat="&currmod" and (anydigit(code_int) or anyalpha(code_int) or link='CASE_ID' or def_rule='COST'); quit; 
%if &int.>0 %then %do;

	proc sql noprint; select distinct code_int into :incl_lst separated by " " from def 
		where def_cat="&currmod" and anydigit(code_int); quit; 
	%let n_incl=&sqlobs; 
	proc sql noprint; select distinct code_int into :excl_lst separated by " " from def 
		where def_cat="&currmod" and anyalpha(code_int) and code_int^='X'; quit; 
	%let n_excl=&sqlobs;

	proc sql noprint; select distinct link into :link from def where def_cat="&currmod"; quit;
	proc datasets lib=work nolist; delete incl: excl:; quit;

	data all_incl all_incl_any all_excl all_excl_any incl incl_any excl excl_any;
	set &in_data.; 
	if def_sub="ALL" then do;
		if code_int='X' then output all_excl_any;
		else if anyalpha(code_int) then output all_excl;
		else if anydigit(code_int) then output all_incl;
		else output all_incl_any;
	end;
	else do;
		if code_int='X' then output excl_any;
		else if anyalpha(code_int) then output excl;
		else if anydigit(code_int) then output incl;
		else output incl_any;
	end;
	run;
	/*process inclusion (p=1) and exclusion (p=2) separately*/
%let lst=incl excl;
%do p=1 %to 2;
	%let curr_set=%scan(&lst, &p);
	%if &&n_&curr_set.>=2 %then %do; /*indicates that there are at least one "and" logic */
		%do q=1 %to 2;
			%if &q.=1 %then %do; %let set=&curr_set; %end; %else %do; %let set=all_&curr_set.; %end;
			proc sql noprint; select count(distinct code_int) into:n_in from &set.; quit; 
			%if &n_in.>=2 %then %do;
				proc sql; 
					create table &set._int(drop=code_int) as 
					select * from &set.
					group by bene_id, def_sub, def_id, syskey, link
					having count(distinct code_int)=&n_in.; 
				quit;
			%end;
			%else %do;
				data &set._int; set &set.; run;
			%end;
		%end;
		proc sql noprint; select count(*) into:cnt from &curr_set._int; quit;
		proc sql noprint; select count(*) into:all_cnt from all_&curr_set._int; quit;
		%if &cnt.>0 and &all_cnt.>0 %then %do;
			proc sql;
			create table &curr_set._int as
			select a.*
			from &curr_set._int a, all_&curr_set._int b
			where a.bene_id=b.bene_id and a.syskey=b.syskey and a.link=b.link;
			quit;
		%end;
		%else %do;
			data &curr_set._int; set &curr_set._int all_&curr_set._int; run;
		%end;
		data &curr_set._all; set &curr_set._any &curr_set._int; run;
		proc sort data=&curr_set._all nodupkey; by bene_id def_sub def_id syskey link; run;
	%end;

	%else %do; 
		data &curr_set._all; set &curr_set._any all_&curr_set._any; run;
		proc sort data=&curr_set._all; by bene_id def_sub def_id syskey link id; run;
		proc sort data=&curr_set._all nodupkey; by bene_id def_sub def_id syskey link; run;
	%end;

	data &curr_set._all; 
	set &curr_set._all; 
	if rule="COST" then excl_amt=allowed_amt; else excl_amt=.;
	run;

	proc sql noprint; select distinct link, def_rule into :link, :rule from def 
		where def_cat="&currmod" and %if &curr_set.=incl %then not; anyalpha(code_int); quit;
	proc sql noprint; select * from &curr_set._all where def_sub="ALL"; quit;
	%if &sqlobs.>0 %then %do; %let all=1; %end; %else %do; %let all=0; %end;

	%if &rule.=FIRST or &rule.=LAST %then %do;
	proc sort data=&curr_set._all; by bene_id def_sub def_id module %if &rule.=LAST %then descending; index_dt; run;
	proc sort data=&curr_set._all nodupkey; by bene_id def_sub def_id module; run;
	%end;

	%if &post_trig. %then %do;	

	proc sql; create table &curr_set._all as 
		select distinct %if &ch. %then i.*, e.* ; %else e.*, i.* ;
		from out.&def_name._medical e, 
			 (select bene_id, def_sub, def_id, module, link, rule, %if %index("&link.", CASE_ID) %then case_id; %else syskey;, sum(excl_amt) as excl_amt
			  from &curr_set._all group by bene_id, def_sub, def_id, module, link, rule, %if %index("&link.", CASE_ID) %then case_id; %else syskey;) i
		where i.bene_id=e.bene_id and %if %index("&link.", CASE_ID) %then i.case_id = e.case_id; %else i.syskey=e.syskey; and i.def_id=e.def_id 
			  %if not &all. and not &ch. %then %do; and i.def_sub=e.def_sub %end; 
	; quit;

	%end;
%end;
	
	proc sql; create table &in_data. as 
		select i.*, e.syskey is not null and e.rule="" as excl_flag, e.excl_amt 
		from incl_all(drop=excl_amt) i
		left join (select distinct bene_id, def_sub, def_id, syskey, link, rule, excl_amt from excl_all) e
		on i.bene_id=e.bene_id and %if not &all. and not &ch. %then %do; i.def_sub=e.def_sub and i.def_id=e.def_id and %end; i.syskey = e.syskey
		having excl_flag=0;
		quit;
%end;

%else %do;
	proc sort data=&in_data.; by bene_id def_sub %if &post_trig. %then def_id; syskey id; run;
	proc sort data=&in_data. nodupkey; by bene_id def_sub %if &post_trig. %then def_id; syskey; run;
	data &in_data.; set &in_data.; excl_flag=0; excl_amt=.; run;
%end;
proc datasets lib=work nolist; delete incl: excl:; quit;

%mend proc_code_int;

%macro proc_clm_n(currmod,in_data);
/*process minimum number of claims requirement specified by clm_n column*/
%local clm_n1 clm_gap_min1 clm_gap_max1;
proc sql noprint; select distinct clm_n into :clm_n separated by " " from def where def_cat="&currmod"; quit; 

%if &clm_n.>1 %then %do; /* if clm_n is 1 or missing, nothing will be done.*/
	%read_def(def, &currmod., ) run; %offset(1);
	%if &clm_gap_min1.=. %then %let clm_gap_min1=1;;
	%if &clm_gap_max1.=. %then %let clm_gap_max1=&check_end1.-&check_beg1.;
	proc sql; create table bene_clm_n as
		select *, count(distinct syskey) as n 
		from &in_data. 
		group by bene_id, def_sub
		having n>=&clm_n1. 
		order by bene_id, def_sub, index_dt;
	quit;
	data clm_n_lag;
		set bene_clm_n; 
		by bene_id def_sub index_dt;
		retain first_dt lag_day;
		lag_day=index_dt-lag(index_dt);
		if first.def_sub then do; first_dt=index_dt; lag_day=.; end;
		if last.def_sub then max_gap=index_dt-first_dt; 
	run;
	proc sql;
		create table clm_n_gap as 
		select bene_id, def_sub, min(lag_day) as min_gap, max(max_gap) as max_gap
		from clm_n_lag 
		where lag_day^=.
		group by bene_id, def_sub
		having max_gap>=&clm_gap_min1. and min_gap<=&clm_gap_max1.; 
	quit;
	proc sql; create table &in_data. as 
		select a.* from &in_data. a, clm_n_gap b
		where a.bene_id=b.bene_id and a.def_sub=b.def_sub;
	quit;
%end;
%mend;


%macro combine_modules(comb_mod, mod);
%local i j mod_n link seq cnt;
%read_def(def, &comb_mod., ) run; 

proc datasets lib=work nolist; delete &comb_mod.; quit;

%if &cnt.>0 %then %do;
	%do i=1 %to &cnt.;
		%let mod_n=%sysfunc(countw(%str(&&def_rule&i.),'<+>'));
		%put note: currently processing &&def_cat&i.: &&def_rule&i. ;

		proc sql noprint; select link into :link from def where def_cat="&&def_cat&i."; quit;

		%if &mod_n.=1 %then %do;
			data mod_n;
			set &&def_rule&i.; 
			&comb_mod._rank="&&def_cat&i";
			%if &&def_sub&i. ne %then %do;
				if def_sub="" then def_sub="&&def_sub&i.";
			%end;
			run;
		%end;
		
		%else %do;
			%if %index(&&def_rule&i.,+) %then %do;
			data mod;
			set %do j=1 %to &mod_n.; %scan(&&def_rule&i.,&j.)(in=a&j.) %end;; 
			if a1 then flag=1; else flag=0;
			run;

			proc sql;
			create table mod_n as
			select *, "&&def_cat&i." as &comb_mod._rank, count(distinct module) as mod_n
			from mod
			group by bene_id, def_sub %if &post_trig. %then, def_id; %if &link. ne %then , &link.;
			having mod_n=&mod_n. and flag=1
			order by bene_id, def_sub %if &post_trig. %then, def_id;, syskey, &comb_mod._rank, allowed_amt desc;
			quit;
			%end;

			%if %index(&&def_rule&i.,<) or %index(&&def_rule&i.,>) %then %do;
			%offset(&i.);
				%if &&clm_gap_min&i.=. %then %do; %let clm_gap_min=1; %end; %else %do; %let clm_gap_min=&&clm_gap_min&i.; %end;
				%if &&clm_gap_max&i.=. %then %do; %let clm_gap_max=&&check_end&i.-&&check_beg&i.; %end; %else %do; %let clm_gap_max=&&clm_gap_max&i.; %end;
			proc sql;
			create table mod_n as 
			select a.*, "&&def_cat&i." as &comb_mod._rank
			from  %scan(&&def_rule&i.,1,"<+>") a,  %scan(&&def_rule&i.,2,"<+>") b
			where a.bene_id=b.bene_id and a.def_sub=b.def_sub %if &post_trig. %then and a.def_id=b.def_id; 
				and %if %index(&&def_rule&i.,<) %then &clm_gap_min.<=b.index_dt-a.index_dt<=&clm_gap_max.;
					%if %index(&&def_rule&i.,>) %then &clm_gap_min.<=a.index_dt-b.index_dt<=&clm_gap_max.;
				%if &link. ne %then , and a.&link.=b.&link.;
			order by bene_id, def_sub %if &post_trig. %then, def_id;, syskey, &comb_mod._rank, allowed_amt desc;
			quit;
			%end;

			%if &&def_sub&i. ne %then %do;
			data mod_n; set mod_n; if def_sub="" then def_sub="&&def_sub&i."; run;
			%end;
		%end;

		proc sort data=mod_n nodupkey out=&comb_mod._&i; by bene_id def_sub %if &post_trig. %then def_id; syskey; run;
/*		proc datasets lib=work nolist; delete mod:; quit;*/
	%end;
%end;

%else %do;
/*	%read_def(def, &mod., ) run; */
	proc sql noprint;
	select distinct def_cat into:def_cat separated by ' '
	from def where index(def_cat,"&mod.")>0; quit;

	%do i=1 %to &sqlobs.;
		proc datasets lib=work nolist; delete mod_n; quit;
		%let curr_mod=%scan(&def_cat.,&i);
		%let seq=%sysfunc(compress(&curr_mod.,,kd));
		data mod_n;
			set &curr_mod.; 
			&comb_mod._rank="&comb_mod._&seq.";
/*			%if &&def_sub&i. ne %then %do;*/
/*			if def_sub="" then def_sub="&&def_sub&i.";*/
/*			%end;*/
		run;
		proc sort data=mod_n; by bene_id def_sub %if &post_trig. %then def_id; syskey &comb_mod._rank descending allowed_amt; run;
		proc sort data=mod_n nodupkey out=&comb_mod._&seq.; by bene_id def_sub %if &post_trig. %then def_id; syskey; run;
		proc datasets lib=work nolist; delete mod:; quit;
	%end;
%end;

data &comb_mod.; set &comb_mod._:; value=1; run;
/*proc datasets lib=work nolist; delete mod mod_n &comb_mod._:; quit;*/
%mend combine_modules;

%macro trigger_identification;
%if &exist_trig. %then %do;
%local triglst i currmod def_rule1 dedup_by curr_set lst p keep_last sort anchor1 anchor2 comp post_trig; 

proc datasets lib=work nolist; delete &base_mod._in_codelst &base_mod._in all; quit;
data &base_mod._in_codelst; set whole_codelst; where index(def_cat, "&base_mod._IN"); run;
%let post_trig=0;
%all_module(&base_mod._IN);

proc sql noprint; select distinct def_cat into :triglst separated by " " from &base_mod._in_codelst; quit;

%do i=1 %to %sysfunc(countw(&triglst.)); /*this step produces each trig modules in triglst*/
	%let currmod=%scan(&triglst.,&i.);
	data &currmod.; set &base_mod._in; where module="&currmod."; run;
	%proc_code_int(&currmod., &currmod.);
	%proc_clm_n(&currmod.,&currmod.);
%end;

%combine_modules(&base._IN, &base_mod._IN);		

proc sort data=&src_enroll. out=enroll; by bene_id descending start_dt; run;
proc sort data=enroll(keep=bene_id bene_dob_dt bene_dod_dt bene_race bene_gender) nodupkey; by bene_id; run;

proc sql; create table &base. as 
	select distinct i.*, e.*
	from &base._in i left join enroll e 
	on i.bene_id=e.bene_id; quit;

%read_def(def, LEVEL, ); run;		
%offset(1);

data &base.; 
	set &base.; 
	length def_name $ 15; 
	def_name="&def_name.";
	format prf_beg prf_end ana_beg ana_end mmddyy10.;
	prf_beg=&prf_beg.; prf_end=&prf_end.; 
	ana_beg=&check_beg1.; ana_end=&check_end1.;
	if bene_dod_dt ne . then do;
		if bene_dod_dt<ana_beg then delete;
		else ana_end=min(ana_end,bene_dod_dt);
	end;
run;

/*dedup overlapped definitions at the specified level*/

%if %index(&def_rule1.,SUB_) %then %do; %let dedup_by=def_sub; %end;
%else %do; %let dedup_by=def_name; %end;

proc sql noprint; select distinct "'"||strip(def_sub)||"'" into :keep_last separated by " " from sub 
	where keep="LAST"; quit;
%Let last_cnt=&sqlobs.;

data keep_first keep_last; 
	set &base.; 
	%if &last_cnt.>0 %then %do;
		if def_sub in (&keep_last.) then output keep_last; else output keep_first;
	%end;
	%else %do;
		output keep_first;
	%end;
run;

%let lst=first last;
%do p=1 %to 2;
	%let curr_set=%scan(&lst, &p);
	%if &curr_set.=last %then %do; %let ind1=index; %let ind2=ana; %let anchor1=beg; %let anchor2=end; %let comp= <; %let sort=descending; %end;
						%else %do; %let ind1=ana; %let ind2=index; %let anchor1=end; %let anchor2=beg; %let comp= >; %let sort=%str(); %end;
 
	proc sort data=keep_&curr_set.; 
		by &dedup_by. bene_id 
		%if %index(&def_rule1.,RANK) %then &base._in_rank;
		&sort. index_dt &base._in_rank descending allowed_amt; 
	run;

	proc sort data=keep_&curr_set. nodupkey; 
		by &dedup_by. bene_id %if not %index(&def_rule1.,BENE) and not %index(&def_rule1.,COHORT) %then &sort. index_dt; ; 
	run;

	%if %index(&def_rule1.,EPISODE) %then %do;
	data keep_&curr_set.(drop=epi_dt); 
		set keep_&curr_set.; 
		by &dedup_by. bene_id &sort. index_dt;
		retain epi_dt;
		if first.bene_id then do; epi_dt=&ind1._&anchor1.; overlap=0; end;
		else if &ind2._&anchor2. &comp. epi_dt then do; epi_dt=&ind1._&anchor1.; overlap=0; end;
		if overlap=0 then output;
	run;
	%end;
%end;

data out.&def_name._&base.; 
	set keep_:;
	exclusion=0; age=.; discharge=coalescec(discharge, " ");
	drop excl_flag overlap keep link code_int module table rule id value max_: excl_amt cpt apc_cd dnl_flag;
run;

proc datasets lib=work nolist; delete keep_: &base.: ; quit;
%end;
%mend trigger_identification;

%macro eligibility_check;
%local n_elig elig_lst curr_elig i j wclst1 wclst2; 

proc sql noprint; select distinct def_cat into :elig_lst separated by " " from def 
	where index(def_cat, "ELIG_"); quit;	
%let n_elig=&sqlobs; 
%if &n_elig.>0 %then %do;

%read_def(def, ELIG_, ENROLL_GAP); run;		
%offset(1); %separatebar(setting1);	

%if &exist_trig. %then %do;
proc sql; 
	create table elig as 
	select a.*, b.start_dt, b.end_dt
	from out.&def_name._&base. a 
	left join &src_enroll. b 
	on a.bene_id=b.bene_id and b.enroll_type in (&setting1.) 
	order by bene_id, def_sub, def_id, start_dt;
quit;
%end;

%else %do;
data elig; 
	set &src_enroll.; 
	where enroll_type in (&setting1.);
run;

%read_def(def, LEVEL, ); run;		
%offset(1);

data elig; 
	set elig; 
	format prf_beg prf_end ana_beg ana_end mmddyy10.;
	length def_name $15 def_sub $30 def_id $30; 
		def_name="&def_name."; def_sub=""; def_id=strip(bene_id); 
		prf_beg=&prf_beg.; prf_end=&prf_end.; 
		ana_beg=&check_beg1.; ana_end=&check_end1.;
		if bene_dod_dt ne . then do;
			if bene_dod_dt<ana_beg then delete;
			else ana_end=min(ana_end,bene_dod_dt);
		end;
	index_dt=.; index_beg=.; index_end=.; index_claim=.; index_case=.; age=.; syskey=.; case_id=.;
	clm_type=""; setting=""; discharge=""; code=""; code_apc="";
run;
%end;

proc datasets lib=work nolist; delete elig_:; quit;
%do i=1 %to &n_elig.;
	%let curr_elig=%scan(&elig_lst, &i);
		proc datasets lib=work nolist; delete &curr_elig.; quit;
	%read_def(def, &curr_elig., ) run;
	%offset(1);
	proc sql noprint; select distinct scan(clm_wc1,1,"<=>"), scan(clm_wc2,1,"<=>") into :wclst1 separated by " ",  :wclst2 separated by " "
		from def where def_cat="&curr_elig."; quit;	

	%let j=1;
	%if %index(&&code_type&j, AGE_M) %then %do;
		data &curr_elig.; set elig;
		%if &&clm_wc1&j ne  %then %do; where &&clm_wc1&j %if &&clm_wc2&j ne  %then %do; and &&clm_wc2&j %end;; %end;
		length elig_excl $ 50 excl_rank $ 20 parameter $ 6;
			age=(&&check_beg&j.-bene_dob_dt)/365.25;	
		%if %index(&&code_type&j., MIN) %then %do;
			if age < &&code&j. then do; 
				elig_excl="&&code_desc&j."; excl_rank="&curr_elig."; parameter=put(age,6.1); output; end;
		%end;
		%if %index(&&code_type&j, MAX) %then %do;
			if age > &&code&j. then do; 
				elig_excl="&&code_desc&j."; excl_rank="&curr_elig."; parameter=put(age,6.1); output; end;
		%end;
		run;
	%end;

	%if "&&code_type&j"="ENROLL_GAP" %then %do;
		data &curr_elig. (drop=d1 d2);
			set elig; 
			%if &&clm_wc1&j ne  %then %do; where &&clm_wc1&j %if &&clm_wc2&j ne  %then %do; and &&clm_wc2&j %end;; %end;
			by bene_id def_sub def_id start_dt;
			retain gap d1 d2; length elig_excl $ 50 excl_rank $ 20 parameter $ 6;
			if first.def_id then do; 
						gap=0; d1=&&check_beg&j.-1; d2=min(ana_end, &&check_end&j.); 
			end;
			if (.<start_dt<=d2 and end_dt>=d1) then do; 
				gap=gap+max(0,start_dt-d1-1); d1=min(d2,end_dt); 
			end;
			if last.def_id then do; 
				if d2>d1 then gap=gap+d2-d1; 
				if gap>&&code&j. then do; 
					elig_excl="&&code_desc&j"; excl_rank="&curr_elig."; parameter=put(gap,6.);
					output;
				end;
			end;
		run;
	%end;

	%if "&&code_type&j"="GENDER" %then %do;
		data &curr_elig.; set elig; 
		%if &&clm_wc1&j ne  %then %do; where &&clm_wc1&j %if &&clm_wc2&j ne  %then %do; and &&clm_wc2&j %end;; %end;
			length  elig_excl $ 50 excl_rank $ 20 parameter $ 6;
			if bene_gender=0 then gender='U';
			if bene_gender=1 then gender='M';
			if bene_gender=2 then gender='F';
			if gender ne "&&code&j"; 
			elig_excl="&&code_desc&j"; excl_rank="&curr_elig."; parameter=gender;
		run;
	%end;

	%if "&&code_type&j"="ESRD" %then %do; 
		%if &&code&j = 0 %then %do;
		proc sql; create table esrd as select i.* from elig i, &src_esrd. e 
		where %if &&clm_wc1&j ne  %then %do; &&clm_wc1&j %if &&clm_wc2&j ne  %then %do; and &&clm_wc2&j %end; and %end;
				i.bene_id=e.bene_id and e.mdcr_status_code in ('11' '21' '31') 
				and &&check_beg&j. <= e.end_dt and &&check_end&j.>= e.start_dt 
		order by bene_id, def_sub, def_id; quit;

		data &curr_elig.; set esrd; length elig_excl $ 50 excl_rank $ 20 parameter $ 6;
			elig_excl="&&code_desc&j"; excl_rank="&curr_elig."; parameter='Y';
		run;
		%end;
	%end;

	%if "&&code_type&j"="DOD" or "&&code_type&j"="DOB" %then %do; 
		%if "&&code_type&j"="DOD" %then %do; %let var=bene_dod_dt; %end; %if "&&code_type&j"="DOB" %then %do; %let var=bene_dob_dt; %end;
		data &curr_elig.; set elig; 
		%if &&clm_wc1&j ne  %then %do; where &&clm_wc1&j %if &&clm_wc2&j ne  %then %do; and &&clm_wc2&j %end;; %end;
			length  elig_excl $ 50 excl_rank $ 20 parameter $ 2;
		%if &&code&j = 0 %then %do; if &check_beg1. <= &var. <= &check_end1.; %end;
		%if &&code&j = 1 %then %do; if &var.<&check_beg1. or &var. >&check_end1.; %end;
			elig_excl="&&code_desc&j"; excl_rank="&curr_elig."; parameter="&&code_type&j";
		run;
	%end;
%end;

data elig_exclusion; set elig_: ; run;
proc sort data=elig_exclusion; by bene_id def_sub def_id excl_rank; run;
proc sort data=elig_exclusion nodupkey; by bene_id def_sub def_id; run;

%if &exist_trig. %then %do;
	%if &ch. %then %do;
		data elig2; set elig; where start_dt<=prf_beg and end_dt>=prf_beg; run;
		proc sort data=elig2(keep=bene_id def_sub def_id end_dt); by bene_id def_sub def_id; run;
	%end;
proc sort data=out.&def_name._&base.; by bene_id def_sub def_id; run;
data out.&def_name._&base.; 
	merge out.&def_name._&base. elig_exclusion(keep=bene_id def_sub def_id elig_excl excl_rank parameter)
		%if &ch. %then elig2;; 
	by bene_id def_sub def_id; 
	length excl $ 50; age=.;
	if not missing(elig_excl) then do; exclusion=1; excl=strip(elig_excl)||": "||strip(parameter); end;
	drop elig_excl parameter;
	%if &ch. %then %do; if not missing(end_dt) then ana_end=min(ana_end, end_dt); drop end_dt; %end;
run;
%end;
%else %do;
proc sort data=elig; by bene_id def_sub def_id; run;
data out.&def_name._&base.; 
	merge elig elig_exclusion(keep=bene_id def_sub def_id elig_excl excl_rank parameter); 
	by bene_id def_sub def_id; 
	length excl $ 50 excl_rank $ 20;
	exclusion=0; excl=""; excl_rank=""; syskey=.;
	if elig_excl^="" then delete;
	drop elig_excl;
run;
%end;

proc datasets lib=work nolist; delete elig:; quit;
%end;
%mend eligibility_check;
%macro claim_subset;
%local tbl_lst curr_tbl i hcc; 

proc sql noprint; select count(*) into :hcc from def where def_cat="RF_HCC"; quit;
%if &hcc. %then %do;
	proc datasets lib=work nolist; delete hcc all; quit;
	data hcc; set whole_codelst; where def_cat="RF_HCC"; run;

	%read_def(def, RF_HCC, ); run;		
	%offset(1);	
	%if &index_dt1. eq  %then %let index_dt1=THRU_DT;;

	proc sql;
	create table hcc_base as
	select distinct bene_id, exclusion, def_name, 
		%if not &ch. %then %do; def_sub, def_id, index_beg, index_end %end;
		%else %do; "" as def_sub length=30, bene_id as def_id length=30, prf_beg, prf_end %end;
	from out.&def_name._&base.
	where exclusion^=1;
	quit;

	proc sql;
	create table hcc_med as 
	select distinct a.bene_id, a.syskey, a.clm_type, b.def_name, b.def_sub, b.def_id, 
			case when a.bill_type in ('11' '41') then 'IP' else 'OP' end as type
	from &src_medical. a, hcc_base b
	where a.bene_id=b.bene_id and b.exclusion^=1 and &check_beg1<=a.&index_dt1<=&check_end1
		and (a.clm_type='PHYS' or (a.bill_type in ('11' '41' '12' '13' '43' '71' '73' '76' '77' '85') and a.freq_cd^='8'));
	quit;

	proc sql;
	create table hcc_cpt as 
	select distinct a.syskey, b.def_sub, b.def_id
	from &src_serviceline. a, hcc_base b, meta.cpt_hcc c
	where a.bene_id=b.bene_id and b.exclusion^=1 and &check_beg1<=a.&index_dt1<=&check_end1
		and a.cpt=c.proc_code and c.year=year(&prf_beg.);
	quit;

	proc sql;
	create table hcc_post_cpt as
	select a.*, b.syskey is not null as cpt_flag
	from hcc_med a
	left join hcc_cpt b
	on a.type='OP' and a.def_sub=b.def_sub and a.def_id=b.def_id and a.syskey=b.syskey
	having clm_type='IP' or cpt_flag=1;
	quit;

	proc sql;
	create table out.&def_name._hcc_dx as
	select distinct a.bene_id, a.def_name, a.def_sub, a.def_id, b.dx_cd
	from hcc_post_cpt a, &src_diagnosis. b
	where a.bene_id=b.bene_id and a.syskey=b.syskey
	order by bene_id, def_sub, def_id, dx_cd;
	quit;

	proc datasets lib=work nolist; delete hcc:; quit;
%end;

proc sql noprint; select distinct t_data into:tbl_lst separated by " " from whole_codelst
	where index(def_cat, "_EX_")>0 or index(def_cat, "&out_mod._")>0 or (index(def_cat, "RF")>0 and def_cat^="RF_HCC"); quit;

%if &sqlobs.>0 %then %do; 
%if %index(&tbl_lst., medical)=0 %then %let tbl_lst=&tbl_lst. medical;;

%if &ch. %then %do;
	%do i=1 %to %sysfunc(countw(&tbl_lst.));
		%let curr_tbl=%scan(&tbl_lst.,&i.);
		proc sql;
		create table out.&def_name._&curr_tbl. as 
		select a.*, b.bene_dob_dt, b.bene_dod_dt, b.age, b.def_name, "" as def_sub length=30, a.bene_id as def_id length=30, . as index_dt, . as index_beg, . as index_end, 
				. as index_claim, . as index_case, b.ana_beg, b.ana_end, "" as index_discharge length=6
		from &curr_tbl. a, (select distinct bene_id, bene_dob_dt, bene_dod_dt, age, def_name, ana_beg, ana_end, exclusion from out.&def_name._&base. where exclusion^=1) b
		where a.bene_id=b.bene_id and b.exclusion^=1 and 
			(%if %index(&curr_tbl., pharmacy) %then %do; a.fill_dt between b.ana_beg and b.ana_end %end;
			%else %do; a.thru_dt between b.ana_beg and b.ana_end %end;)
		order by bene_id, syskey;
		quit;
		%let src_&curr_tbl.=out.&def_name._&curr_tbl.;
	%end;
%end;
%else %do;
	%do i=1 %to %sysfunc(countw(&tbl_lst.));
		%let curr_tbl=%scan(&tbl_lst.,&i.);
		proc sql;
		create table out.&def_name._&curr_tbl. as 
		select a.*, b.bene_dob_dt, b.bene_dod_dt, b.age, b.def_name, b.def_sub, b.def_id, b.index_dt, b.index_beg, b.index_end, b.ana_beg, b.ana_end,
			b.clm_type as index_type length=4, b.setting as index_setting, b.discharge as index_discharge, b.code as trig_code, b.code_apc as trig_apc,
			a.syskey=b.syskey as index_claim, a.case_id=b.case_id as index_case
		from &curr_tbl. a, out.&def_name._&base. b
		where a.bene_id=b.bene_id and b.exclusion^=1 and 
			(%if %index(&curr_tbl., pharmacy) %then %do; a.fill_dt between b.ana_beg and b.ana_end %end;
			%else %do; a.from_dt between b.ana_beg and b.ana_end or a.thru_dt between b.ana_beg and b.ana_end %end;)
		order by bene_id, def_sub, def_id, syskey;
		quit;
		%let src_&curr_tbl.=out.&def_name._&curr_tbl.;
	%end;
%end;
%end;

%mend claim_subset;

%macro pt_exclusion;
%local pt_excl_lst i currmod post_trig; 

proc sql noprint; select distinct def_cat into :pt_excl_lst separated by " " from def 
	where index(def_cat, "&base_mod._EX"); quit;

%if &sqlobs.>0 %then %do;

	proc datasets lib=work nolist; delete &base_mod._ex_codelst &base_mod. all; quit;

	data &base_mod._ex_codelst; set whole_codelst; where index(def_cat, "&base_mod._EX"); run;
	%let post_trig=1;
	%all_module(&base_mod._EX);

	proc sql noprint; select distinct def_cat into :exlst separated by " " from &base_mod._ex_codelst; quit;

	%do i=1 %to %sysfunc(countw(&exlst.));
		%let currmod=%scan(&exlst.,&i.);
		data &currmod.; set &base_mod._ex; where module="&currmod."; run;
		%proc_code_int(&currmod., &currmod.);
		%proc_clm_n(&currmod.,&currmod.);
	%end;

	%combine_modules(&base._EX, &base_mod._EX);;		

	proc sort data=&base._ex; by bene_id def_sub def_id &base._ex_rank; run;
	proc sort data=&base._ex nodupkey; by bene_id def_sub def_id;; run;

	proc sql;
	create table &base._ex_update as
		select a.*, b.module as ex_module, b.&base._ex_rank
		from out.&def_name._&base. a
		left join &base._ex b
		on a.bene_id=b.bene_id %if not &ch. %then and a.def_sub=b.def_sub and a.def_id=b.def_id;;
	quit;

	data out.&def_name._&base.;
		set &base._ex_update;
		excl=coalescec(excl, ex_module);
		excl_rank=coalescec(excl_rank, &base._ex_rank);
		if not missing(excl_rank) then exclusion=1;
		drop ex_module &base._ex_rank;
	run;

	proc datasets lib=work nolist; delete &base._ex:; quit;
%end;
%mend pt_exclusion;

%macro service_inclusion;
%local i outlst currmod ranklst post_trig;
proc sql noprint; select distinct def_type, def_cat into :def_type, :svclst separated by " " from def 
	where index(def_cat, "&out_mod._IN"); quit;

%if &sqlobs.>0 %then %do;
	proc datasets lib=work nolist; delete &out_mod._in_codelst &out_mod. all; quit;
	data &out_mod._in_codelst; set whole_codelst; where index(def_cat, "&out_mod._IN"); run;
	%let post_trig=1;

	%all_module(&out_mod._IN);

	proc sql noprint; select distinct def_cat into :outlst separated by " " from &out_mod._in_codelst; quit;

	%do i=1 %to %sysfunc(countw(&outlst.));
		%let currmod=%scan(&outlst.,&i.);
		data &currmod.; set &out_mod._in; where module="&currmod."; run;
		%proc_code_int(&currmod., &currmod.);
		%proc_clm_n(&currmod.,&currmod.);
	%end;

	%combine_modules(&out._IN, &out_mod._IN);;		

/*	data &out._in; set &out._in; value=1; run;*/
	proc sort data=&out._in nodupkey; by bene_id def_sub def_id syskey &out._in_rank; run;
	proc transpose data=&out._in out=&out._in_trans; by bene_id def_sub def_id syskey; var value; id &out._in_rank; run;

	proc sort data=&out._in out=&out._in_dedup nodupkey; by bene_id def_sub def_id syskey; run;
	proc sort data=&out._in out=&out._in_cost nodupkey; where excl_amt^=.; by bene_id def_sub def_id syskey; run;


	%if &def_type.=MEASURE %then %do;
		proc sql noprint; select distinct &out._in_rank into :ranklst separated by " " from &out._in; quit;

		proc sort data=&out._in_trans; 
			by bene_id def_sub def_id %do i=1 %to %sysfunc(countw(&ranklst.)); descending %scan(&ranklst.,&i.) %end;;
		run;
		proc sort data=&out._in_trans nodupkey; by bene_id def_sub def_id; run;

		data out.&def_name._&out.; 
			merge out.&def_name._&base. &out._in_trans(keep=bene_id def_sub def_id &out.:); 
			by bene_id def_sub def_id; 
		run;
	%end;

	%if %sysfunc(exist(out.&def_name._medical)) %then %do;
		data out.&def_name._&out._medical; 
			merge %if &ch. %then &out._in; %else out.&def_name._medical;(in=a) 
				  &out._in_dedup(in=b keep=bene_id def_sub def_id syskey &out._in_rank) 
				  &out._in_trans(keep=bene_id def_sub def_id syskey &out._in:)
				  &out._in_cost(keep=bene_id def_sub def_id syskey excl_amt);
			by bene_id def_sub def_id syskey; 
			if a and b;
		run;
	%if &ch. %then %do;
	proc sql;
		create table out.&def_name._&out._medical as 
		select b.def_name, b.def_sub, b.def_id, a.*
		from out.&def_name._&out._medical a, out.&def_name._&base. b 
		where a.bene_id=b.bene_id and a.def_sub=b.def_sub
		order by bene_id, def_sub, def_id, syskey, &out._in_rank;
	quit;
	%end;

	%end;

	%if %sysfunc(exist(out.&def_name._pharmacy)) %then %do;
		data out.&def_name._&out._pharmacy; 
			merge out.&def_name._pharmacy(in=a) 
				  &out._in_dedup(in=b keep=bene_id def_sub def_id syskey &out._in_rank)
				  &out._in_trans(keep=bene_id def_sub def_id syskey &out._in:)
				  &out._in_cost(keep=bene_id def_sub def_id syskey excl_amt);
			by bene_id def_sub def_id syskey; 
			if a and b;
		run;
	%end;
%end;
proc datasets lib=work nolist; delete &out._:; quit;
%mend service_inclusion;

%macro rf;
%local i rflst currmod ranklst post_trig hcc;

proc sql noprint; select distinct def_type, def_cat into :def_type, :rflst separated by " " from def 
	where index(def_cat, "RF") and def_cat^="RF_HCC"; quit;

%if &sqlobs.>0 %then %do;
	proc datasets lib=work nolist; delete rf_codelst all; quit;
	data rf_in_codelst; set whole_codelst; where index(def_cat, "RF") and def_cat^="RF_HCC"; run;
	%let post_trig=1;

	%all_module(RF_IN);

	proc sql noprint; select distinct def_cat into :rflst separated by " " from rf_in_codelst; quit;

	%do i=1 %to %sysfunc(countw(&rflst.));
		%let currmod=%scan(&rflst.,&i.);
		data &currmod.; set rf_in; where module="&currmod."; run;
		%proc_code_int(&currmod., &currmod.);
		%proc_clm_n(&currmod.,&currmod.);
	%end;

	%combine_modules(RISK_IN, RF_IN);;		

	proc sort data=risk_in out=out.&def_name._rf nodupkey; by bene_id def_sub def_id syskey risk_in_rank; run;
%end;

proc datasets lib=work nolist; delete risk_in; quit;
%mend rf;


