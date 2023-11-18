 %macro V03127M2(ICD9_10=);
 %**********************************************************************
 MACRO NAME:      V03127M2
 PURPOSE:         Assign additional CC for some ICD codes

 Note: Future releases will differentiate between ICD-9 and -10 herein.

 **********************************************************************;

 IF      &ICD9_10 IN("74100","74101","74102","74103") THEN CC121  = 1;
 ELSE IF &ICD9_10 IN("40403","40413","40493" )        THEN CC187  = 1;

 %mend V03127M2;
