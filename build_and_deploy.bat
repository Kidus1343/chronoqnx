@echo off
rem ============================================================================
rem  ChronoQNX - build_and_deploy.bat
rem  Cross-compiles ipc_bench + chrono_agent for QNX 8.0 x86_64, deploys them to
rem  /data/home/root on the QEMU target (ssh alias "qnx-qemu") and verifies them.
rem
rem  usage:  build_and_deploy.bat [start-agent]
rem            start-agent : also (re)start chrono_agent in the background
rem ============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "QNX_ENV=C:\qnx800\qnxsdp-env.bat"
set "TARGET=qnx-qemu"
set "RDIR=/data/home/root"
set "SSHOPT=-o BatchMode=yes -o ConnectTimeout=10 -q"

echo [1/5] Initializing QNX SDP 8.0 environment
if not exist "%QNX_ENV%" (
    echo ERROR: %QNX_ENV% not found. Install QNX SDP 8.0 to C:\qnx800.
    exit /b 1
)
call "%QNX_ENV%" >nul 2>&1
@echo off
where qcc >nul 2>&1
if errorlevel 1 (
    echo ERROR: qcc not on PATH after sourcing %QNX_ENV%
    exit /b 1
)

rem NOTE: -lsocket is required on QNX for BSD socket calls (socket, sendto, ...).
echo [2/5] Compiling workload-sim\ipc_bench.c
qcc -Vgcc_ntox86_64 -O2 workload-sim/ipc_bench.c -o workload-sim/ipc_bench -lsocket
if errorlevel 1 ( echo ERROR: ipc_bench build failed & exit /b 1 )

echo [3/5] Compiling target-probe\chrono_agent.c
qcc -Vgcc_ntox86_64 -O2 target-probe/chrono_agent.c -o target-probe/chrono_agent -lsocket
if errorlevel 1 ( echo ERROR: chrono_agent build failed & exit /b 1 )

echo [4/5] Deploying to %TARGET%:%RDIR%/
ssh %SSHOPT% %TARGET% "slay -f chrono_agent >/dev/null 2>&1; slay -f ipc_bench >/dev/null 2>&1; sleep 2; exit 0"
scp %SSHOPT% workload-sim/ipc_bench target-probe/chrono_agent %TARGET%:%RDIR%/
if errorlevel 1 ( echo ERROR: scp failed - is the QEMU VM up and ssh alias %TARGET% configured? & exit /b 1 )
ssh %SSHOPT% %TARGET% "chmod +x %RDIR%/ipc_bench %RDIR%/chrono_agent"
if errorlevel 1 ( echo ERROR: chmod failed & exit /b 1 )

echo [5/5] Verifying execution on target
ssh %SSHOPT% %TARGET% "cd %RDIR% && ./chrono_agent --help >/dev/null && ./ipc_bench --mode normal --duration 2 --no-telemetry --quiet"
if errorlevel 1 ( echo ERROR: verification run failed on target & exit /b 1 )

if /i "%~1"=="start-agent" (
    echo Starting chrono_agent on %TARGET% ^(tcp 9000^)
    ssh %SSHOPT% %TARGET% "cd %RDIR% && (nohup ./chrono_agent > agent.log 2>&1 </dev/null &) ; sleep 1; cat agent.log"
)

echo.
echo ChronoQNX deployed OK.
echo   target : ssh %TARGET% "cd %RDIR% && ./chrono_agent"        ^(telemetry on :9000^)
echo   target : ssh %TARGET% "cd %RDIR% && ./ipc_bench --mode invert"
echo   host   : python host-analyzer\server.py     -^> http://127.0.0.1:8080
echo   host   : python host-analyzer\ci_gate.py --launch normal ^| invert
endlocal
exit /b 0
