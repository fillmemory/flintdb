@echo off

REM Simple script to compile and generate the tutorial files.
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"
echo PWD: %cd%

set LIB_DIRS=./lib

set CP=./tutorial/classes
for %%d in (%LIB_DIRS%) do (
  if exist "%%d" (
    for /r "%%d" %%i in (*.jar) do (
      set CP=%CP%;%%i
    )
  )
)

echo CP: %CP%

javac -cp "%CP%" tutorial/src/Tutorial.java -d tutorial/classes
REM java -cp "%CP%" Tutorial --help
java -cp "%CP%" Tutorial --customers=10000 --avg-orders=10 --avg-items=30
