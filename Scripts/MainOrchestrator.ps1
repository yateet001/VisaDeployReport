# ================================
# MainOrchestrator.ps1
# ================================

# Import utility scripts
. "$PSScriptRoot\Token-Utility.ps1"
. "$PSScriptRoot\PBI-Deployment-Utility.ps1"

# ----------------------------
# Deployment Configuration
# ----------------------------
$TenantId       = "<Your-Tenant-Id>"
$ClientId       = "<Your-Client-Id>"
$ClientSecret   = "<Your-Client-Secret>"

$WorkspaceName  = "<Target-Workspace-Name>"

$SemanticModelPath = "$PSScriptRoot\SemanticModels\SalesDataset.pbism"
$ReportPath        = "$PSScriptRoot\Reports\SalesReport.pbix"

# ----------------------------
# 1. Authenticate
# ----------------------------
Write-Host "Authenticating..." -ForegroundColor Cyan
$AccessToken = Get-AccessToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
if (-not $AccessToken) {
    Write-Error "❌ Failed to acquire access token. Stopping."
    exit 1
}
Write-Host "✔ Authentication successful." -ForegroundColor Green

# ----------------------------
# 2. Get Workspace Id
# ----------------------------
Write-Host "Fetching WorkspaceId for '$WorkspaceName'..." -ForegroundColor Cyan
$WorkspaceId = Get-WorkspaceIdByName -AccessToken $AccessToken -WorkspaceName $WorkspaceName
if (-not $WorkspaceId) {
    Write-Error "❌ Workspace '$WorkspaceName' not found. Stopping."
    exit 1
}
Write-Host "✔ WorkspaceId: $WorkspaceId" -ForegroundColor Green

# ----------------------------
# 3. Deploy Semantic Model
# ----------------------------
Write-Host "Deploying Semantic Model from $SemanticModelPath ..." -ForegroundColor Cyan
$SemanticModelId = Deploy-SemanticModel -AccessToken $AccessToken -WorkspaceId $WorkspaceId -SemanticModelPath $SemanticModelPath
if ($SemanticModelId) {
    Write-Host "✔ Semantic Model deployed successfully. Id: $SemanticModelId" -ForegroundColor Green
} else {
    Write-Error "❌ Semantic Model deployment failed."
    exit 1
}

# ----------------------------
# 4. Deploy Report
# ----------------------------
Write-Host "Deploying Report from $ReportPath ..." -ForegroundColor Cyan
$ReportId = Deploy-Report -AccessToken $AccessToken -WorkspaceId $WorkspaceId -ReportPath $ReportPath -SemanticModelId $SemanticModelId
if ($ReportId) {
    Write-Host "✔ Report deployed successfully. Id: $ReportId" -ForegroundColor Green
} else {
    Write-Error "❌ Report deployment failed."
    exit 1
}

# ----------------------------
# 5. Final Status
# ----------------------------
Write-Host "🎉 Deployment completed successfully!" -ForegroundColor Green
