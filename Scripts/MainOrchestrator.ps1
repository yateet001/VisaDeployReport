# ================================
# MainOrchestrator.ps1 (Final)
# ================================
[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [ValidateSet("Dev","Prod")]
    [string]$Environment,

    [Parameter(Mandatory = $true)]
    [string]$SemanticModelName,

    [Parameter(Mandatory = $true)]
    [string]$ModelDefinitionPath,   # path to model.bim (or JSON definition)

    [Parameter(Mandatory = $true)]
    [string]$ReportName,

    [Parameter(Mandatory = $true)]
    [string]$ReportDefinitionPath   # path to report definition JSON
)

Write-Host "========== Starting Orchestrator ($Environment) ==========" -ForegroundColor Cyan

# --- Paths ---
$scriptRoot = $PSScriptRoot
$configPath = Join-Path $scriptRoot "Config\config.json"
$tokenUtil  = Join-Path $scriptRoot "Scripts\Token-Utilities.ps1"
$pbiUtil    = Join-Path $scriptRoot "Scripts\PBI-Deployment-Utilities.ps1"

try {
    # ----------------------------
    # 0) Imports & Config
    # ----------------------------
    if (-not (Test-Path $tokenUtil)) { throw "Token utilities not found at: $tokenUtil" }
    if (-not (Test-Path $pbiUtil))   { throw "PBI deployment utilities not found at: $pbiUtil" }
    if (-not (Test-Path $configPath)){ throw "config.json not found at: $configPath" }

    . $tokenUtil
    . $pbiUtil

    $config = Get-Content -Raw -Path $configPath | ConvertFrom-Json

    # Resolve env-specific values from config.json
    switch ($Environment) {
        "Dev" {
            $WorkspaceId = $config.DevWorkspaceID
            $Server      = $config.DevWarehouseConnection
            $Database    = $config.DevWarehouseName
        }
        "Prod" {
            $WorkspaceId = $config.ProdWorkspaceID
            $Server      = $config.ProdWarehouseConnection
            $Database    = $config.ProdWarehouseName
        }
    }

    if (-not $WorkspaceId) { throw "WorkspaceId for '$Environment' not found in config.json" }
    if (-not $Server)      { throw "Warehouse connection endpoint for '$Environment' not found in config.json" }
    if (-not $Database)    { throw "Warehouse name for '$Environment' not found in config.json" }

    if (-not (Test-Path $ModelDefinitionPath))  { throw "Model definition file not found: $ModelDefinitionPath" }
    if (-not (Test-Path $ReportDefinitionPath)) { throw "Report definition file not found: $ReportDefinitionPath" }

    Write-Host "Workspace: $WorkspaceId"
    Write-Host "Warehouse endpoint: $Server"
    Write-Host "Warehouse name: $Database"
    Write-Host "Semantic model: $SemanticModelName"
    Write-Host "Report: $ReportName"

    # ----------------------------
    # 1) Auth
    # ----------------------------
    Write-Host "Fetching access token..." -ForegroundColor Yellow
    $AccessToken = Get-PBIAccessToken -TenantId $config.TenantID -ClientId $config.ClientID -ClientSecret $config.ClientSecret
    if (-not $AccessToken) { throw "Failed to acquire access token." }
    $Headers = @{ Authorization = "Bearer $AccessToken" }
    Write-Host "✔ Access token acquired." -ForegroundColor Green

    # ----------------------------
    # 2) Deploy Semantic Model
    # ----------------------------
    Write-Host "`n--- STEP 4: SEMANTIC MODEL DEPLOYMENT ---"
    $semanticModelId = Deploy-PBISemanticModel -WorkspaceId $WorkspaceId `
                                               -SemanticModelName $SemanticModelName `
                                               -ModelDefinitionPath $ModelDefinitionPath `
                                               -AccessToken $AccessToken

    if (-not $semanticModelId) { throw "Semantic model deployment returned no ID." }
    Write-Host "Semantic model result - ID: $semanticModelId"

    # ----------------------------
    # 3) Ensure Fabric Connection & Bind
    # ----------------------------
    Write-Host "`n--- STEP 5: CONNECTION (GATEWAY) BINDING ---"
    $connectionId = Ensure-FabricConnection -WorkspaceId $WorkspaceId -Server $Server -Database $Database -Headers $Headers
    if (-not $connectionId) { throw "No connectionId available to bind to semantic model." }

    Bind-Connection-ToSemanticModel -WorkspaceId $WorkspaceId -SemanticModelId $semanticModelId -ConnectionId $connectionId -Headers $Headers
    Write-Host "✔ Gateway connection bound to semantic model." -ForegroundColor Green

    # ----------------------------
    # 4) Deploy Report
    # ----------------------------
    Write-Host "`n--- STEP 6: REPORT DEPLOYMENT ---"
    $reportId = Deploy-PBIReport -WorkspaceId $WorkspaceId `
                                 -ReportName $ReportName `
                                 -ReportPath $ReportDefinitionPath `
                                 -SemanticModelId $semanticModelId `
                                 -AccessToken $AccessToken

    if (-not $reportId) { throw "Report deployment failed or returned no ID." }
    Write-Host "✔ Report deployed successfully. ReportId = $reportId" -ForegroundColor Green

    Write-Host "`n========== Orchestration Completed Successfully ==========" -ForegroundColor Cyan
}
catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
