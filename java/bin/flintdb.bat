@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

SET SCRIPT=%0
SET SCRIPTDIR=%SCRIPT%\..
cd %SCRIPTDIR%
cd ..
SET CURRENTDIR="%cd%"
REM ECHO %cd%


SET JLIB_PATH=lib
SET JMAIN=lite.db.CLI
SET JVMOPT=-Dfile.encoding=UTF-8 -Xmx16g
REM -Duser.timezone=GMT
SET CP=.;build\classes

SET CP="
 FOR /R %JLIB_PATH% %%a IN (*.jar) DO (
   SET CP=!CP!;%%a
 )
 SET CP=!CP!"
 
SET CP=.;build/classes;%CP%
REM ECHO %CP%

if defined JAVA_HOME (
    echo %JAVA_HOME%
) else (
    SET JAVA_HOME=%USERPROFILE%\jdk-17\
)

SET PATH=%JAVA_HOME%\bin;%PATH%

java -cp %CP% %JVMOPT% %JMAIN% %*

ENDLOCAL
