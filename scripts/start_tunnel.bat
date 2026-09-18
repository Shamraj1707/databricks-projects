@echo off
title Wanderbricks TCP Tunnel
echo ===========================================================================
echo    WANDERBRICKS TCP TUNNEL (bore.pub -^> localhost:3306)
echo ===========================================================================
echo.
echo Connecting to public relay bore.pub...
"%~dp0bore.exe" local 3306 --to bore.pub
pause
