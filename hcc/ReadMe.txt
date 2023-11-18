(1) Each program with a .SAS extension has a corresponding .TXT file.  

The .SAS and .TXT files are identical. The .TXT files are provided to make 
the SAS programs easier to view with some text editors. 

File names are case sensitive on some computing platforms, and software 
modules assume that file names are upper case (e.g., V03EDIT2.SAS).

---------------------


(2) The two transport files (each with extension .TRN) contain the format 
library and model coefficients dataset. 

The transport files may be used on any SAS® version 9 platform after 
uploading them and converting them using SAS® PROC CIMPORT. Program 
IMPORT.SAS is provided as an example.

If your computing platform is z/OS, both transport files should be uploaded 
using the following parameters:  RECFM(F or FB) LRECL(80) BLKSIZE(8000).

