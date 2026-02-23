param(
    [Parameter(Mandatory = $true)]
    [string]$CompanyQuery,

    [int]$SearchItems = 5,
    [int]$FilingItems = 25,
    [string]$OutputDir = ".\\output"
)

$ErrorActionPreference = "Stop"

if (-not $env:CH_API_KEY) {
    throw "Set CH_API_KEY in your environment before running this script."
}

$publicBase = "https://api.company-information.service.gov.uk"
$docBase = "https://document-api.company-information.service.gov.uk"

$authPair = "{0}:" -f $env:CH_API_KEY
$authB64 = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($authPair))
$headers = @{
    Authorization = "Basic $authB64"
}

function Invoke-CHJsonGet {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )
    return Invoke-RestMethod -Method GET -Uri $Url -Headers $headers
}

if (-not (Test-Path -Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

Write-Host "Searching companies for query: $CompanyQuery"
$searchUrl = "$publicBase/search/companies?q=$([Uri]::EscapeDataString($CompanyQuery))&items_per_page=$SearchItems"
$search = Invoke-CHJsonGet -Url $searchUrl

if (-not $search.items -or $search.items.Count -eq 0) {
    throw "No companies found for query '$CompanyQuery'."
}

$company = $search.items[0]
$companyNumber = $company.company_number
Write-Host "Using first match: $($company.title) ($companyNumber)"

$profile = Invoke-CHJsonGet -Url "$publicBase/company/$companyNumber"
$filings = Invoke-CHJsonGet -Url "$publicBase/company/$companyNumber/filing-history?items_per_page=$FilingItems"

$profilePath = Join-Path $OutputDir "$companyNumber-profile.json"
$filingsPath = Join-Path $OutputDir "$companyNumber-filings.json"
$searchPath = Join-Path $OutputDir "$companyNumber-search.json"

$search | ConvertTo-Json -Depth 20 | Set-Content -Path $searchPath -Encoding UTF8
$profile | ConvertTo-Json -Depth 20 | Set-Content -Path $profilePath -Encoding UTF8
$filings | ConvertTo-Json -Depth 20 | Set-Content -Path $filingsPath -Encoding UTF8

Write-Host "Saved search/profile/filings JSON to $OutputDir"

$downloaded = @()
if ($filings.items) {
    foreach ($item in $filings.items) {
        if (-not $item.links -or -not $item.links.document_metadata) {
            continue
        }

        $metadataPath = $item.links.document_metadata
        if (-not $metadataPath.StartsWith("/document/")) {
            continue
        }

        $docId = ($metadataPath -split "/")[-1]
        $metaUrl = "$docBase$metadataPath"
        $meta = Invoke-CHJsonGet -Url $metaUrl

        $contentType = "application/pdf"
        if ($meta.resources -and $meta.resources.PSObject.Properties.Name -contains "application/pdf") {
            $contentType = "application/pdf"
        } elseif ($meta.resources) {
            $firstType = $meta.resources.PSObject.Properties.Name | Select-Object -First 1
            if ($firstType) {
                $contentType = $firstType
            }
        }

        $targetExt = if ($contentType -eq "application/pdf") { "pdf" } else { "bin" }
        $targetPath = Join-Path $OutputDir "$companyNumber-$docId.$targetExt"
        $contentUrl = "$docBase/document/$docId/content"

        try {
            Invoke-WebRequest -Method GET -Uri $contentUrl -Headers ($headers + @{ Accept = $contentType }) -MaximumRedirection 10 -OutFile $targetPath | Out-Null
            $downloaded += $targetPath
            Write-Host "Downloaded document $docId -> $targetPath"
        } catch {
            Write-Warning "Failed to download document $docId ($contentType): $($_.Exception.Message)"
        }
    }
}

Write-Host "Done. Downloaded $($downloaded.Count) document(s)."
