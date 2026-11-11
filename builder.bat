@echo off
setlocal
cd /d "%~dp0"

echo [1/2] Building frontend (web/dist) ...
cd web
if not exist node_modules (
  echo node_modules not found, running "pnpm install" first ...
  call pnpm install
  if errorlevel 1 goto :fail
)
call pnpm build
if errorlevel 1 goto :fail
cd ..

echo [2/2] Compiling backend (cross-platform, CGO_ENABLED=0) ...
set CGO_ENABLED=0

echo   - windows/amd64 -> ipcnotice.exe
set GOOS=windows
set GOARCH=amd64
go build -o ipcnotice.exe .
if errorlevel 1 goto :fail

echo   - linux/amd64   -> ipcnotice-linux-amd64
set GOOS=linux
set GOARCH=amd64
go build -o ipcnotice-linux-amd64 .
if errorlevel 1 goto :fail

echo   - linux/arm64   -> ipcnotice-linux-arm64
set GOOS=linux
set GOARCH=arm64
go build -o ipcnotice-linux-arm64 .
if errorlevel 1 goto :fail

echo.
echo Done. Binaries + web/dist are up to date:
echo   ipcnotice.exe            (windows/amd64)
echo   ipcnotice-linux-amd64    (linux/amd64)
echo   ipcnotice-linux-arm64    (linux/arm64)
goto :eof

:fail
echo.
echo Build failed. Check the errors above.
exit /b 1
