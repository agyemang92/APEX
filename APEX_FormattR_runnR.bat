@ECHO OFF
Title: Let's get your AQS data ready for APEX!

SETLOCAL ENABLEDELAYEDEXPANSION

echo Input the file path to R.exe on your computer. If you're unable to use ctrl+v to paste, right click and select 'paste'.
set /p r_loc="Where is your R program located?  "
set r_loc_use="%r_loc%\R.exe"

IF exist %r_loc_use% (
	REM file exists
) ELSE (
	echo That directory cannot be found. Please try again.
	set /p r_loc="Where is your R program located?  "
	set r_loc_use="!r_loc!\R.exe"
	IF not exist !r_loc_use! echo You must provide a readable directory. Please verify your directory and retry the tool.& timeout /t 45 /nobreak > NUL & EXIT
)

set /p script_loc="Next, where is the script? " 
set script_loc_use="%script_loc%\Apex_Util_ControlStart.R"
cd /d %script_loc%
echo %script_loc%>user_cd.txt

IF exist %script_loc% (
	REM file exists
) ELSE (
	echo That directory cannot be found. Please try again.
	set /p script_loc="Where is the script? " 
	set script_loc_use="!script_loc!\Apex_Util_ControlStart.R"
	cd /d !script_loc!
	echo !script_loc!>user_cd.txt
	IF not exist !script_loc! echo You must provide a readable directory. Please verify your directory and retry the tool.& timeout /t 45 /nobreak > NUL & EXIT
)



set /p log_loc="Finally, where should the record of the R log be printed?  "
set log_loc_use="%log_loc%\Rsession_run_log.Rout"

IF exist %log_loc% (
	REM file exists
) ELSE (
	echo That directory cannot be found. Please try again.
	set /p log_loc="Where should the record of the R log be printed?  "
	set log_loc_use="!log_loc!\Rsession_run_log.Rout"
	IF not exist !log_loc! echo You must provide a readable directory. Please verify your directory and retry the tool.& timeout /t 45 /nobreak > NUL & EXIT
)

echo Now, we'll open a text file for you to input the script parameters. Be sure to save it with your desired inputs.

timeout /t 4 /nobreak > NUL

start notepad "User_inputs.txt"

PAUSE

Echo Fetching and formatting the data.

%r_loc_use% CMD BATCH %script_loc_use% %log_loc_use%

echo Run complete! Please review the meta data file to be sure the data is acceptable before using in the APEX model. If the output folder is empty please consult the Rsession_run_log to determine the error.

CMD /k