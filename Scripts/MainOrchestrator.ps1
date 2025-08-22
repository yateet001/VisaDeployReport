# ================================
# MainOrchestrator.ps1 (Fixed with Validation)
# ================================
[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [ValidateSet("Dev","Prod")]
    [string]$Environment,

    [Parameter(Mandatory = $true)]
    [string]$ConfigFile,

    [Parameter(Mandatory = $false)]
    [string]$DeploymentProfilePath,

    [Parameter(Mandatory = $false)]
    [switch]$SkipValidation
)

Write-Host "========== Starting Orchestrator ($Environment) ==========" -ForegroundColor Cyan

# --- Paths ---
$scriptRoot = $PSScriptRoot
$configPath = if ([System.IO.Path]::IsPathRooted($ConfigFile)) { $ConfigFile } else { Join-Path (Split-Path $scriptRoot -Parent) $ConfigFile }
$tokenUtil  = Join-Path $scriptRoot "Token-Utilities.ps1"
$pbiUtil    = Join-Path $scriptRoot "PBI-Deployment-Utilities.ps1"

# Set deployment profile path based on environment if not provided
if (-not $DeploymentProfilePath) {
    $DeploymentProfilePath = Join-Path (Split-Path $scriptRoot -Parent) "Configuration\$Environment\DEPLOYMENT_PROFILE.csv"
}

# ================================
# VALIDATION FUNCTIONS
# ================================
function Test-Configuration {
    param(
        [Parameter(Mandatory=$true)][string]$ConfigPath,
        [Parameter(Mandatory=$true)][string]$Environment
    )
    
    Write-Host "=== Configuration Validation ===" -ForegroundColor Yellow
    
    # Test config file exists
    if (-not (Test-Path $ConfigPath)) {
        throw "Configuration file not found: $ConfigPath"
    }
    
    # Load and validate config
    try {
        $config = Get-Content -Raw -Path $ConfigPath | ConvertFrom-Json
        Write-Host "✓ Configuration file loaded successfully" -ForegroundColor Green
    }
    catch {
        throw "Failed to parse configuration file: $_"
    }
    
    # Validate required properties
    $requiredProps = @(
        "DevWorkspaceID", "ProdWorkspaceID",
        "DevWarehouseConnection", "ProdWarehouseConnection", 
        "DevWarehouseName", "ProdWarehouseName",
        "TenantID", "ClientID", "ClientSecret",
        "FabricAPIEndpoint"
    )
    
    $missingProps = @()
    foreach ($prop in $requiredProps) {
        if (-not $config.PSObject.Properties.Name.Contains($prop) -or [string]::IsNullOrWhiteSpace($config.$prop)) {
            $missingProps += $prop
        }
    }
    
    if ($missingProps.Count -gt 0) {
        throw "Missing or empty configuration properties: $($missingProps -join ', ')"
    }
    
    Write-Host "✓ All required configuration properties present" -ForegroundColor Green
    
    # Validate environment-specific properties
    switch ($Environment) {
        "Dev" {
            if ([string]::IsNullOrWhiteSpace($config.DevWorkspaceID)) {
                throw "DevWorkspaceID is required for Dev environment"
            }
            if ([string]::IsNullOrWhiteSpace($config.DevWarehouseConnection)) {
                throw "DevWarehouseConnection is required for Dev environment"
            }
            Write-Host "✓ Dev environment configuration valid" -ForegroundColor Green
        }
        "Prod" {
            if ([string]::IsNullOrWhiteSpace($config.ProdWorkspaceID)) {
                throw "ProdWorkspaceID is required for Prod environment"
            }
            if ([string]::IsNullOrWhiteSpace($config.ProdWarehouseConnection)) {
                throw "ProdWarehouseConnection is required for Prod environment"
            }
            Write-Host "✓ Prod environment configuration valid" -ForegroundColor Green
        }
    }
    
    return $config
}

function Test-DeploymentProfile {
    param(
        [Parameter(Mandatory=$true)][string]$ProfilePath
    )
    
    Write-Host "=== Deployment Profile Validation ===" -ForegroundColor Yellow
    
    if (-not (Test-Path $ProfilePath)) {
        throw "Deployment profile not found: $ProfilePath"
    }
    
    try {
        $profile = Import-Csv -Path $ProfilePath
        Write-Host "✓ Deployment profile loaded successfully" -ForegroundColor Green
    }
    catch {
        throw "Failed to parse deployment profile: $_"
    }
    
    if (-not $profile -or $profile.Count -eq 0) {
        throw "Deployment profile is empty or contains no valid rows"
    }
    
    # Validate required columns
    $requiredColumns = @("workspace_name", "report_name", "report_path", "warehouse_name", "environment_type", "transformation_layer")
    $actualColumns = $profile[0].PSObject.Properties.Name
    
    $missingColumns = @()
    foreach ($col in $requiredColumns) {
        if ($col -notin $actualColumns) {
            $missingColumns += $col
        }
    }
    
    if ($missingColumns.Count -gt 0) {
        throw "Missing columns in deployment profile: $($missingColumns -join ', ')"
    }
    
    # Validate each row has required data
    for ($i = 0; $i -lt $profile.Count; $i++) {
        $row = $profile[$i]
        if ([string]::IsNullOrWhiteSpace($row.report_name)) {
            throw "Row $($i + 1): report_name is required"
        }
        if ([string]::IsNullOrWhiteSpace($row.report_path)) {
            throw "Row $($i + 1): report_path is required"
        }
    }
    
    Write-Host "✓ Deployment profile structure valid" -ForegroundColor Green
    Write-Host "✓ Found $($profile.Count) deployment profile(s) to process" -ForegroundColor Green
    
    return $profile
}

function Test-ReportPaths {
    param(
        [Parameter(Mandatory=$true)][array]$DeploymentProfile,
        [Parameter(Mandatory=$true)][string]$BasePath
    )
    
    Write-Host "=== Report Path Validation ===" -ForegroundColor Yellow
    
    $pathIssues = @()
    
    foreach ($profile in $DeploymentProfile) {
        Write-Host "Validating: $($profile.report_name)" -ForegroundColor Cyan
        
        # Check report path
        $reportPath = Join-Path $BasePath $profile.report_path
        $reportDir = Split-Path $reportPath -Parent
        
        if (Test-Path $reportPath) {
            Write-Host "  ✓ Report definition found: $reportPath" -ForegroundColor Green
        } elseif (Test-Path $reportDir) {
            Write-Host "  ✓ Report directory found: $reportDir" -ForegroundColor Green
        } else {
            Write-Host "  ✗ Report path missing: $reportPath" -ForegroundColor Red
            $pathIssues += "Report path missing: $reportPath"
        }
        
        # Check for model.bim in semantic model folder
        $modelPath = Join-Path $reportDir "$($profile.report_name).SemanticModel\model.bim"
        if (Test-Path $modelPath) {
            Write-Host "  ✓ Model definition found: $modelPath" -ForegroundColor Green
        } else {
            # Try alternative paths
            $altModelPath = Join-Path $reportDir "*.SemanticModel\model.bim"
            $foundModel = Get-ChildItem -Path $altModelPath -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($foundModel) {
                Write-Host "  ✓ Model definition found: $($foundModel.FullName)" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ Model definition not found: $modelPath" -ForegroundColor Yellow
                Write-Host "    Will attempt to create semantic model from report definition" -ForegroundColor Yellow
            }
        }
        
        # Check for .pbip file
        $pbipPath = Join-Path $reportDir "$($profile.report_name).pbip"
        if (Test-Path $pbipPath) {
            Write-Host "  ✓ PBIP file found: $pbipPath" -ForegroundColor Green
        } else {
            $foundPbip = Get-ChildItem -Path $reportDir -Filter "*.pbip" -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($foundPbip) {
                Write-Host "  ✓ PBIP file found: $($foundPbip.FullName)" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ PBIP file not found in: $reportDir" -ForegroundColor Yellow
            }
        }
    }
    
    if ($pathIssues.Count -gt 0) {
        Write-Host "Path validation issues found:" -ForegroundColor Red
        $pathIssues | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
        throw "Critical path validation failures. Please check file structure."
    }
    
    Write-Host "✓ Path validation completed successfully" -ForegroundColor Green
}

function Test-Prerequisites {
    param(
        [Parameter(Mandatory=$true)][string]$ConfigPath,
        [Parameter(Mandatory=$true)][string]$Environment,
        [Parameter(Mandatory=$true)][string]$DeploymentProfilePath,
        [Parameter(Mandatory=$true)][string]$ScriptRoot
    )
    
    Write-Host "`n========== VALIDATION PHASE ==========" -ForegroundColor Magenta
    
    # Test script dependencies
    $tokenUtil = Join-Path $ScriptRoot "Token-Utilities.ps1"
    $pbiUtil = Join-Path $ScriptRoot "PBI-Deployment-Utilities.ps1"
    
    if (-not (Test-Path $tokenUtil)) { throw "Token utilities not found at: $tokenUtil" }
    if (-not (Test-Path $pbiUtil)) { throw "PBI deployment utilities not found at: $pbiUtil" }
    Write-Host "✓ Script dependencies found" -ForegroundColor Green
    
    # Validate configuration
    $config = Test-Configuration -ConfigPath $ConfigPath -Environment $Environment
    
    # Validate deployment profile
    $deploymentProfile = Test-DeploymentProfile -ProfilePath $DeploymentProfilePath
    
    # Validate report paths
    $basePath = Split-Path $ScriptRoot -Parent
    Test-ReportPaths -DeploymentProfile $deploymentProfile -BasePath $basePath
    
    Write-Host "✓ All validations passed successfully" -ForegroundColor Green
    Write-Host "========== VALIDATION COMPLETE ==========`n" -ForegroundColor Magenta
    
    return @{
        Config = $config
        DeploymentProfile = $deploymentProfile
    }
}

# ================================
# MAIN ORCHESTRATION LOGIC
# ================================
try {
    # Run validation unless skipped
    if (-not $SkipValidation) {
        $validationResult = Test-Prerequisites -ConfigPath $configPath -Environment $Environment -DeploymentProfilePath $DeploymentProfilePath -ScriptRoot $scriptRoot
        $config = $validationResult.Config
        $deploymentProfile = $validationResult.DeploymentProfile
    } else {
        Write-Host "⚠ Skipping validation as requested" -ForegroundColor Yellow
        # Load config and profile without validation
        $config = Get-Content -Raw -Path $configPath | ConvertFrom-Json
        $deploymentProfile = Import-Csv -Path $DeploymentProfilePath
    }

    # Import utilities
    . $tokenUtil
    . $pbiUtil

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

    Write-Host "Environment Configuration:" -ForegroundColor Cyan
    Write-Host "  Workspace ID: $WorkspaceId"
    Write-Host "  Warehouse Server: $Server"
    Write-Host "  Warehouse Database: $Database"
    Write-Host "  Reports to deploy: $($deploymentProfile.Count)"

    # ----------------------------
    # AUTHENTICATION
    # ----------------------------
    Write-Host "`n========== AUTHENTICATION ==========" -ForegroundColor Magenta
    Write-Host "Acquiring access token..." -ForegroundColor Yellow
    $AccessToken = Get-SPNToken -TenantId $config.TenantID -ClientId $config.ClientID -ClientSecret $config.ClientSecret
    if (-not $AccessToken) { throw "Failed to acquire access token." }
    $Headers = @{ Authorization = "Bearer $AccessToken" }
    Write-Host "✓ Access token acquired successfully" -ForegroundColor Green

    # Test token validity
    try {
        $testUrl = "$($config.FabricAPIEndpoint)/workspaces"
        Invoke-RestMethod -Uri $testUrl -Headers $Headers -Method Get -TimeoutSec 30 | Out-Null
        Write-Host "✓ Access token validated against Fabric API" -ForegroundColor Green
    }
    catch {
        Write-Warning "Token validation failed, but continuing: $_"
    }

    # ----------------------------
    # DEPLOYMENT PHASE
    # ----------------------------
    Write-Host "`n========== DEPLOYMENT PHASE ==========" -ForegroundColor Magenta
    
    $successCount = 0
    $failureCount = 0
    $deploymentResults = @()

    foreach ($profile in $deploymentProfile) {
        $deploymentStart = Get-Date
        Write-Host "`n--- Processing: $($profile.report_name) ---" -ForegroundColor Cyan
        
        try {
            $ReportName = $profile.report_name
            $ReportPath = $profile.report_path
            $SemanticModelName = $ReportName + "_Model"
            
            # Construct paths
            $ReportDefinitionPath = Join-Path (Split-Path $scriptRoot -Parent) $ReportPath
            $ModelDefinitionPath = Join-Path (Split-Path $ReportDefinitionPath -Parent) "$($profile.report_name).SemanticModel\model.bim"
            
            Write-Host "Report: $ReportName" -ForegroundColor White
            Write-Host "Semantic Model: $SemanticModelName" -ForegroundColor White

            # Deploy Semantic Model
            Write-Host "Deploying semantic model..." -ForegroundColor Yellow
            $semanticResult = Deploy-PBISemanticModel -WorkspaceId $WorkspaceId `
                                                      -SemanticModelName $SemanticModelName `
                                                      -ModelDefinitionPath $ModelDefinitionPath `
                                                      -AccessToken $AccessToken
            if (-not $semanticResult.Success) { 
                throw "Semantic model deployment failed: $($semanticResult.Error)"
            }
            $semanticModelId = $semanticResult.ModelId
            Write-Host "✓ Semantic model deployed successfully" -ForegroundColor Green

            # Connection binding (non-critical)
            Write-Host "Configuring data connections..." -ForegroundColor Yellow
            try {
                $connectionId = Ensure-FabricConnection -WorkspaceId $WorkspaceId -Server $Server -Database $Database -Headers $Headers
                if ($connectionId) {
                    Bind-Connection-ToSemanticModel -WorkspaceId $WorkspaceId -SemanticModelId $semanticModelId -ConnectionId $connectionId -Headers $Headers
                    Write-Host "✓ Data connection configured successfully" -ForegroundColor Green
                }
            }
            catch {
                Write-Warning "Connection binding failed (non-critical): $_"
            }

            # Deploy Report
            Write-Host "Deploying report..." -ForegroundColor Yellow
            $reportResult = Deploy-PBIReport `
                            -WorkspaceId $WorkspaceId `
                            -ReportName $ReportName `
                            -ReportPath $ReportDefinitionPath `
                            -SemanticModelId $semanticModelId `
                            -AccessToken $AccessToken

            # Get report ID
            $reportId = $null
            if ($null -ne $reportResult) {
                if ($reportResult -is [string]) {
                    $reportId = $reportResult
                } elseif ($reportResult.PSObject.Properties.Match('id').Count -gt 0) {
                    $reportId = $reportResult.id
                }
            }

            if (-not $reportId) {
                Write-Host "Looking up report by name..." -ForegroundColor Yellow
                $listUri = "$($config.FabricAPIEndpoint)/workspaces/$WorkspaceId/reports"
                $listResp = Invoke-RestMethod -Method Get -Uri $listUri -Headers $Headers
                $reportHit = @($listResp.value) | Where-Object { $_.displayName -eq $ReportName -or $_.name -eq $ReportName } | Select-Object -First 1
                if ($reportHit) { $reportId = $reportHit.id }
            }

            if (-not $reportId) {
                throw "Report deployment failed - could not obtain report ID"
            }

            Write-Host "✓ Report deployed successfully" -ForegroundColor Green

            # Rebind report to semantic model
            Write-Host "Binding report to semantic model..." -ForegroundColor Yellow
            try {
                $rebindResult = Rebind-ReportToDataset -AccessToken $AccessToken -WorkspaceId $WorkspaceId -ReportId $reportId -DatasetId $semanticModelId -FabricApiEndpoint $config.FabricAPIEndpoint
                Write-Host "✓ Report bound to semantic model successfully" -ForegroundColor Green
            }
            catch {
                Write-Warning "Report rebinding failed (non-critical): $_"
            }

            $deploymentEnd = Get-Date
            $duration = $deploymentEnd - $deploymentStart
            
            Write-Host "✓ Successfully deployed: $ReportName (Duration: $($duration.ToString('mm\:ss')))" -ForegroundColor Green
            $successCount++
            
            $deploymentResults += @{
                ReportName = $ReportName
                Status = "Success"
                Duration = $duration
                Error = $null
            }
        }
        catch {
            $deploymentEnd = Get-Date
            $duration = $deploymentEnd - $deploymentStart
            
            Write-Host "✗ Failed to deploy: $($profile.report_name)" -ForegroundColor Red
            Write-Host "  Error: $_" -ForegroundColor Red
            $failureCount++
            
            $deploymentResults += @{
                ReportName = $profile.report_name
                Status = "Failed"
                Duration = $duration
                Error = $_.Exception.Message
            }
        }
    }

    # ----------------------------
    # DEPLOYMENT SUMMARY
    # ----------------------------
    Write-Host "`n========== DEPLOYMENT SUMMARY ==========" -ForegroundColor Magenta
    Write-Host "Environment: $Environment" -ForegroundColor Cyan
    Write-Host "Total Reports: $($deploymentProfile.Count)" -ForegroundColor Cyan
    Write-Host "Successful: $successCount" -ForegroundColor Green
    Write-Host "Failed: $failureCount" -ForegroundColor Red
    
    if ($deploymentResults.Count -gt 0) {
        Write-Host "`nDetailed Results:" -ForegroundColor Cyan
        foreach ($result in $deploymentResults) {
            $statusColor = if ($result.Status -eq "Success") { "Green" } else { "Red" }
            Write-Host "  $($result.ReportName): $($result.Status) ($($result.Duration.ToString('mm\:ss')))" -ForegroundColor $statusColor
            if ($result.Error) {
                Write-Host "    Error: $($result.Error)" -ForegroundColor Red
            }
        }
    }

    if ($failureCount -eq 0) {
        Write-Host "`n✓ All deployments completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "`n⚠ Some deployments failed. Check the logs above for details." -ForegroundColor Yellow
        if ($failureCount -eq $deploymentProfile.Count) {
            throw "All deployments failed"
        }
    }

    Write-Host "========== ORCHESTRATION COMPLETE ==========`n" -ForegroundColor Magenta
}
catch {
    Write-Host "`n❌ ORCHESTRATION FAILED ❌" -ForegroundColor Red
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host "Stack Trace: $($_.ScriptStackTrace)" -ForegroundColor Red
    exit 1
}