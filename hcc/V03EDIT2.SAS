 %macro V03EDIT2(AGERTI=, AGEMCE=, SEX=, ICD9_10= );
 %**********************************************************************
 1 MACRO NAME:  V03EDIT2
 2 PURPOSE:     age/sex edits on &ICD9_10 - are based on:
                1. RTI age/sex splits
                2. MCE list to check if age or sex
                 is within the range of acceptable age/sex, if not- CC 
                 is set to -1.0 - invalid
 
 3 PARAMETERS:  AGERTI- age for CC splits (=AGE_LAST) 
                AGEMCE- age for MCE edits (=AGE_AT_DIAGNOSIS) 
                SEX   - SEX
                &ICD9_10  - diagnosis variable in a diagnosis file
 4 COMMENTS:    1. Age format AGEFMT and sex format SEXFMT are
                   parameters in the main macro. They must correspond
                   to the years of data
                2. If &ICD9_10 code does not have any restriction on age
                   or sex then the corresponding format puts it in "-1"
                3. AGEL format sets lower limits for age
                   AGEU format sets upper limit for age
                   for specific edit categories:
                   "0"= "0 newborn (age 0)      "
                   "1"= "1 pediatric (age 0 -17)"
                   "2"= "2 maternity (age 12-55)"
                   "3"= "3 adult (age 14+)      "

 Note: Future releases will differentiate between ICD-9 and -10 herein.

 **********************************************************************;
 * RTI age/sex splits;
 * codes are not cross-walked to CC if age condition is not met;
 IF &AGERTI < 2  and &ICD9_10 IN ("4920","4928","496","5181","5182" )
 THEN CC="-1.0" ; /*exclude diagnoses of emphysema for enrollees age <2*/
 ELSE
 IF &AGERTI >= 2  and
    &ICD9_10 IN ("5571","5579","7503","7535","7566","75671","75672","75673",
             "76070","76072","76073","76074","76075","76076","76079" )
 THEN CC="-1.0" ; /*infant diagnoses in HCC42 Peritonitis/Gastrointestinal
 Perforation/Necrotizing Enterocolitis, HCC 64 Major Congenital Anomalies of
 Diaphragm, Abdominal Wall, and Esophagus, Age < 2, and HCC81 Drug Psychosis*/
 ELSE
 IF &AGERTI NE 0 and &ICD9_10 IN ("V2130","V2131","V2132","V2133","V2134","V2135" )
 THEN CC="-1.0" ; /*edit for newborn low birthweight status V-codes*/

 * CC is changed from originally assigned by format depending on age;
 IF &AGERTI >= 18  and &ICD9_10 IN ("1940" )
 THEN CC="10" ; /*split adrenal gland cancer diagnosis to age 18+ and age <18*/
 ELSE
 IF &AGERTI >= 50  and
    &ICD9_10 IN ("1740","1741","1742","1743","1744","1745","1746","1748",
             "1749","1750","1759" )
 THEN CC="12" ; /*split breast cancer diagnoses to age 50+ and age <50*/
 ELSE
 IF &AGERTI >= 2  and &ICD9_10 IN ("5570" )
 THEN CC="154" ; /*split acute vascular insufficiency of intestine diagnosis
 to age 2+ and age <2*/
 ELSE
 IF &AGERTI >= 18  and
    &ICD9_10 IN ("4910","4911","49120","49121","49122","4918","4919","49320",
             "49321","49322" )
 THEN CC="160" ; /*split chronic obstructive asthma and chronic bronchitis 
 diagnoses to age 18+ and age <18*/
 ELSE
 IF &AGERTI >= 2  and &ICD9_10 IN ("7707" )
 THEN CC="162" ; /*split chronic respiratory disease arising in the perinatal
 period diagnosis to age 2+ and age <2*/
 IF &AGERTI >= 18  and
    &ICD9_10 IN ("20400","20401","20402","20600","20601","20602","20700",
             "20701","20702","20800","20801","20802" )
 THEN CC="8" ; /*split acute lymphoid and other acute leukemias, except
 myeloid diagnoses to age 18+ and age <18*/
 ELSE
 IF &AGERTI >= 1  and &ICD9_10 IN ("7594" )
 THEN CC="97" ; /*split conjoined twins diagnosis to age 1+ and age <1*/

 * CC is changed from originally assigned by format depending on sex; 
 IF &sex='1'  and &ICD9_10 IN ("2860","2861" ) THEN CC="66" ; /*split hemophilia
 diagnosis to male and female*/

 * MCE edits;
 * check if age is within acceptable range;
 _tage=put(&ICD9_10, $&AGEFMT..);
 if _tage ne "-1" and
  (&AGEMCE < input(put(_tage, $AGEL.),8.) or
   &AGEMCE > input(put(_tage, $AGEU.),8.)) then cc='-1.0';
 * check if sex for a person is the one in the MCE file;
 _tsex=put(&ICD9_10, $&SEXFMT..);
 if _tsex ne "-1"  and _tsex ne &SEX then cc='-1.0';
 %mend V03EDIT2;
