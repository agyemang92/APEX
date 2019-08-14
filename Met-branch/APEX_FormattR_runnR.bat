@ECHO OFF
Title: Welcome to the APEX FormattR!

echo "Input the file path to R.exe on your computer. To paste into the line right click and select 'paste'."
set /p r_loc="Where is your R program located?"
set r_loc_use="%r_loc%\R.exe"

set /p script_loc="Next, where is the script?"
set script_loc_use="%script_loc%\Apex_Util_ControlStart.R"
cd /d %script_loc%
echo %script_loc%>user_cd.txt

REM set /p user_cd="Where should we print the output files?"
REM cd /d %user_cd%

set /p log_loc="Finally, where should the record of the R log be printed?"
set log_loc_use="%log_loc%\Rsession_run_log.Rout"

echo "Now, we'll open a text file for you to input the script parameters. Be sure to save it with your desired inputs."

start notepad "User_inputs.txt"

PAUSE

Echo "Fetching and formatting the data."

%r_loc_use% CMD BATCH %script_loc_use% %log_loc_use%

echo "Run complete! Please review the meta data file to be sure the data is acceptable before using in the APEX model."

CMD /k

