# =================================================================================================
# MainOrchestrator.ps1 - Unified Power BI PBIP Deployment Script
# =================================================================================================
param(
    [Parameter(Mandatory = $true)]
    [string]$Workspace,

    [Parameter(Mandatory = $true)]
    [string]$ConfigFile
)

Write-Host "🚀 Starting Power BI PBIP Deployment..." -ForegroundColor Cyan

# -------------------------------------------------------------------------------------------------
# Load Config
# -------------------------------------------------------------------------------------------------
if (-Not (Test-Path $ConfigFile)) {
    Write-Error "❌ Config file not found at path $ConfigFile"
    exit 1
}

$config = Get-Content -Path $ConfigFile | ConvertFrom-Json

# -------------------------------------------------------------------------------------------------
# Authentication (Service Principal)
# -------------------------------------------------------------------------------------------------
function Get-AccessToken {
    param (
        [string]$TenantId,
        [string]$ClientId,
        [string]$ClientSecret,
        [string]$Resource = "https://analysis.windows.net/powerbi/api"
    )

    $body = @{
        grant_type    = "client_credentials"
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = "$Resource/.default"
    }

    $response = Invoke-RestMethod -Method Post `
        -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" `
        -Body $body

    return $response.access_token
}

$global:AccessToken = Get-AccessToken `
    -TenantId $config.tenantId `
    -ClientId $config.clientId `
    -ClientSecret $config.clientSecret

if (-not $global:AccessToken) {
    Write-Error "❌ Failed to acquire access token."
    exit 1
}

Write-Host "✅ Authentication successful." -ForegroundColor Green

# -------------------------------------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------------------------------------
function Invoke-PowerBIRestMethod {
    param (
        [string]$Method,
        [string]$Uri,
        [object]$Body = $null
    )

    $headers = @{ Authorization = "Bearer $global:AccessToken" }

    if ($Body) {
        $jsonBody = $Body | ConvertTo-Json -Depth 10 -Compress
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -Body $jsonBody -ContentType "application/json"
    }
    else {
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers
    }
}

# -------------------------------------------------------------------------------------------------
# Get Workspace ID
# -------------------------------------------------------------------------------------------------
$workspaceUri = "https://api.powerbi.com/v1.0/myorg/groups"
$workspaces = Invoke-PowerBIRestMethod -Method Get -Uri $workspaceUri

$workspaceObj = $workspaces.value | Where-Object { $_.name -eq $Workspace }
if (-not $workspaceObj) {
    Write-Error "❌ Workspace '$Workspace' not found."
    exit 1
}

$workspaceId = $workspaceObj.id
Write-Host "📂 Target Workspace: $Workspace ($workspaceId)" -ForegroundColor Cyan

# -------------------------------------------------------------------------------------------------
# Deploy Semantic Model (Dataset)
# -------------------------------------------------------------------------------------------------
Write-Host "📦 Deploying Semantic Model..." -ForegroundColor Yellow

$pbipFolder = $config.pbipSemanticModelPath
if (-not (Test-Path $pbipFolder)) {
    Write-Error "❌ PBIP Semantic Model folder not found: $pbipFolder"
    exit 1
}

# Publish dataset (semantic model)
$datasetName = $config.datasetName
$importUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/imports?datasetDisplayName=$datasetName&nameConflict=Overwrite"

$zipFile = "$env:TEMP\semanticModel.zip"
if (Test-Path $zipFile) { Remove-Item $zipFile -Force }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory($pbipFolder, $zipFile)

$headers = @{
    Authorization = "Bearer $global:AccessToken"
}
$importResponse = Invoke-RestMethod -Method Post -Uri $importUri -Headers $headers -InFile $zipFile -ContentType "application/zip"

$datasetId = $importResponse.datasets[0].id
Write-Host "✅ Semantic Model deployed. DatasetId: $datasetId" -ForegroundColor Green

# -------------------------------------------------------------------------------------------------
# Switch Connections
# -------------------------------------------------------------------------------------------------
Write-Host "🔄 Switching Connections..." -ForegroundColor Yellow

$newConnectionDetails = $config.connectionDetails

$updateConnectionUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/Default.UpdateDatasources"

Invoke-PowerBIRestMethod -Method Post -Uri $updateConnectionUri -Body @{
    updateDetails = @(
        @{
            datasourceSelector = @{
                datasourceType = "Sql"
                connectionDetails = $newConnectionDetails.old
            }
            connectionDetails = $newConnectionDetails.new
        }
    )
}

Write-Host "✅ Connection switched successfully." -ForegroundColor Green

# -------------------------------------------------------------------------------------------------
# Deploy Report and Bind to Dataset
# -------------------------------------------------------------------------------------------------
Write-Host "📊 Deploying Report..." -ForegroundColor Yellow

$reportPath = $config.pbipReportPath
if (-not (Test-Path $reportPath)) {
    Write-Error "❌ Report path not found: $reportPath"
    exit 1
}

$reportZip = "$env:TEMP\report.zip"
if (Test-Path $reportZip) { Remove-Item $reportZip -Force }
[System.IO.Compression.ZipFile]::CreateFromDirectory($reportPath, $reportZip)

$reportImportUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/imports?nameConflict=Overwrite"

$reportImportResponse = Invoke-RestMethod -Method Post -Uri $reportImportUri -Headers $headers -InFile $reportZip -ContentType "application/zip"

$reportId = $reportImportResponse.reports[0].id

Write-Host "✅ Report deployed. ReportId: $reportId" -ForegroundColor Green

# Bind report to dataset
$bindUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/reports/$reportId/Rebind"
Invoke-PowerBIRestMethod -Method Post -Uri $bindUri -Body @{
    datasetId = $datasetId
}

Write-Host "✅ Report successfully bound to Semantic Model." -ForegroundColor Green

# -------------------------------------------------------------------------------------------------
Write-Host "🎉 Power BI Deployment Completed Successfully!" -ForegroundColor Cyan
# -------------------------------------------------------------------------------------------------
