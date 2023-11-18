/*This script will create the NDC data set, which is input 3 of 4 into the HHS-HCC
	program. Components are:

	IDVAR
	NDC

	Pharmacy data is not expected to be loaded into CCM or used for any client deliverables.
	Thus, this is expected to be an empty table.

	Created by: Michelle Vergara
	Last Updated: June 1, 2023
*/

%macro ndc(year);
/*Define file paths*/
LIBNAME  data  "D:\SASData\dua_052882\Sndbx\Michelle_V\HHS-HCC Inputs\&year.";

data data.ndc;
	length bene_id 8 ndc $11;
	stop;
run;

%mend;

