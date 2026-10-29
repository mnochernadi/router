@echo off
rem ocx -- shorthand for the anonymous lane.
rem   ocx "say OK"   |   ocx status   |   ocx refresh
setlocal
set "PS=powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ocx.ps1""
if /i "%~1"=="status"  ( %PS% status & goto :eof )
if /i "%~1"=="refresh" ( %PS% refresh & goto :eof )
%PS% run "%~1"
endlocal
