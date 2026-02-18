@echo off
setlocal

if "%~1"=="" (
  echo Usage: %~nx0 "company query" [search_items] [filing_items] [output_dir]
  echo Example: %~nx0 "tesco" 5 25 output
  exit /b 1
)

if "%CH_API_KEY%"=="" (
  echo CH_API_KEY is not set.
  echo Set it first, e.g.:
  echo   set CH_API_KEY=your_companies_house_api_key
  exit /b 1
)

set "QUERY=%~1"
set "SEARCH_ITEMS=%~2"
set "FILING_ITEMS=%~3"
set "OUTPUT_DIR=%~4"

if "%SEARCH_ITEMS%"=="" set "SEARCH_ITEMS=5"
if "%FILING_ITEMS%"=="" set "FILING_ITEMS=25"
if "%OUTPUT_DIR%"=="" set "OUTPUT_DIR=output"

powershell -NoProfile -ExecutionPolicy Bypass -File ".\companieshouse_fetch.ps1" -CompanyQuery "%QUERY%" -SearchItems %SEARCH_ITEMS% -FilingItems %FILING_ITEMS% -OutputDir ".\%OUTPUT_DIR%"
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo Script failed with exit code %RC%.
  exit /b %RC%
)

echo Completed successfully.
endlocal
