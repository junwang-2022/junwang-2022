proc datasets lib=out;

    modify medical_clm;

    index create memberuid;
    index create claimuid;
    index create provideruid;
run;
proc datasets lib=out;

    modify medical_ccd;

    index create memberuid;
    index create claimuid;

run;
proc datasets lib=out;

    modify enrollment_records;

    index create memberuid;
run;

proc datasets lib=out;

    modify medical_prv;

    index create provideruid;
	index create npi1;
	index create npi2;
run;
proc datasets lib=out;

    modify medical_ipps;

    index create provideruid;
	index create dischargeclaimuid;
	index create memberuid;
run;
proc datasets lib=out;

    modify medical_member;

	index create memberuid;
run;

proc datasets lib=out;

    modify medical_nonipps;

    index create provideruid;
	index create claimuid;
	index create memberuid;
run;
proc datasets lib=out;

    modify medical_xref;

    index create dischargeuid;
	index create claimuid;
	index create memberuid;
run;
proc datasets lib=out;

    modify enrollment_member;

    index create memberuid;
run;
proc datasets lib=out;

    modify medical_psp;

    index create provideruid;
run;

proc datasets lib=out;

    modify pharmacy_member;

    index create memberuid;
run;

proc datasets lib=out;

    modify pharmacy_prv;

    index create provideruid;
run;

proc datasets lib=out;

    modify pharmacy_psp;

    index create provideruid;
run;

proc datasets lib=out;

    modify pharmacy_rxc;

	index create memberuid;
    index create provideruid;
run;
proc datasets lib=out;

    modify pharmacy_rxcc;

	index create memberuid;
    index create rxfilluid;
run;
proc datasets lib=out;

    modify pharmacy_rxcw;

	index create memberuid;
    index create rxclaimuid;
run;
