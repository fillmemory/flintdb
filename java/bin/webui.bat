@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

REM Resolve repository root (this file is in bin\)
SET "SCRIPTDIR=%~dp0"
cd /d "%SCRIPTDIR%"
cd ..
REM echo Now in: %cd%

REM Main class and JVM options (match bin/webui)
SET JMAIN=flint.db.WebUI
SET JVMOPT=-Dfile.encoding=UTF-8 -Xmx4g -DTSVFILE.TYPE.INFERENCE=0

REM Build classpath from jars under lib and build-gradle
SET CP=.
IF EXIST build\classes SET CP=%CP%;build\classes

IF EXIST lib (
  FOR /R "lib" %%a IN (*.jar) DO (
    SET CP=!CP!;%%a
  )
)

IF EXIST build (
  FOR /R "build" %%a IN (*.jar) DO (
    SET CP=!CP!;%%a
  )
)

REM Ensure JAVA_HOME if not set (best-effort fallback)
IF NOT DEFINED JAVA_HOME (
  SET "JAVA_HOME=%USERPROFILE%\jdk-17"
)
SET "PATH=%JAVA_HOME%\bin;%PATH%"

java -cp "%CP%" %JVMOPT% %JMAIN% %*

ENDLOCAL
