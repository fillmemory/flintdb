@echo off

REM Simple script to compile and generate the tutorial files.
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"
echo PWD: %cd%

set LIB_DIRS=..\..\lib ..\..\build\classes\java\main

REM Create classes directory if it doesn't exist
if not exist ".\classes" mkdir ".\classes"

set CP=.\classes
for %%d in (%LIB_DIRS%) do (
  if exist "%%d" (
    for /r "%%d" %%i in (*.jar) do (
      set CP=%CP%;%%i
    )
  )
)

echo CP: %CP%

javac -cp "%CP%" Tutorial.java -d .\classes
REM java -cp "%CP%" Tutorial --help
java -cp "%CP%" Tutorial --customers=10000 --avg-orders=10 --avg-items=30
