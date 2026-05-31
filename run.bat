@echo off
REM =============================================================================
REM GenAI RCA POC -- Command Prompt wrapper (calls PowerShell script)
REM =============================================================================
REM Usage:
REM     run.bat help
REM     run.bat up
REM     run.bat install
REM     ... etc.
REM =============================================================================

powershell.exe -ExecutionPolicy Bypass -File "%~dp0run.ps1" %*
