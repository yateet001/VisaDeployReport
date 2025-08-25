# MainOrchestrator.ps1 for PBIP file deployment (Complete Fixed Version)
param(
    [Parameter(Mandatory=$true)]
    [string]$Workspace,
    
    [Parameter(Mandatory=$true)]
    [string]$ConfigFile
)

Write-Host "Starting Power BI PBIP Deployment..."
Write-Host "Workspace: $Workspace"
Write-Host "Config File: $ConfigFile"

# ===============================
# UTILITY FUNCTIONS
# ===============================

function Get-SPNToken {
    param (
        [Parameter(Mandatory=$true)]
        [string]$TenantId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientSecret
    )
    
    try {
        Write-Host "Acquiring access token for Fabric API..."
        
        $body = @{
            grant_type    = "client_credentials"
            client_id     = $ClientId
            client_secret = $ClientSecret
            scope         = "https://api.fabric.microsoft.com/.default"
        }
        
        $tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Method Post -Body $body
        $accessToken = $tokenResponse.access_token
        
        Write-Host "✓ Successfully acquired Fabric API access token"
        return $accessToken
    }
    catch {
        Write-Error "Failed to acquire access token for Fabric API: $_"
        
        # Fallback to Power BI API scope
        try {
            Write-Host "Trying Power BI API scope as fallback..."
            
            $body = @{
                grant_type    = "client_credentials"
                client_id     = $ClientId
                client_secret = $ClientSecret
                resource      = "https://analysis.windows.net/powerbi/api"
            }
            
            $tokenResponse = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$TenantId/oauth2/token" -Method Post -Body $body
            $accessToken = $tokenResponse.access_token
            
            Write-Host "✓ Successfully acquired Power BI API access token as fallback"
            return $accessToken
        }
        catch {
            Write-Error "Failed to acquire Power BI API access token: $_"
            throw "Could not acquire any access token"
        }
    }
}

function Get-PBIPFiles {
    param(
        $ArtifactPath,
        $Folder
    )

    if ($Folder) {
        Write-Host "Folder path : $Folder"
        $target = Join-Path $ArtifactPath $Folder
    } else {
        $target = $ArtifactPath
    }

    if (-not (Test-Path $target)) {
        Write-Warning "Path not found: $target"
        return @()
    }

    $files = Get-ChildItem -Path $target -Recurse -File -Filter '*.pbip'
    Write-Host "Found $($files.Count) PBIP files in $target"
    
    foreach ($file in $files) {
        Write-Host "  Found PBIP: $($file.FullName)"
        
        $parentDir = $file.Directory.FullName
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        $reportFolder = Join-Path $parentDir "$baseName.Report"
        $semanticModelFolder = Join-Path $parentDir "$baseName.SemanticModel"
        
        Write-Host "    Report folder: $(Test-Path $reportFolder)"
        Write-Host "    SemanticModel folder: $(Test-Path $semanticModelFolder)"
    }

    return $files
}

function Validate-PBIPStructure {
    param(
        [Parameter(Mandatory=$true)]
        [string]$PBIPFilePath
    )
    
    $pbipDir = Split-Path $PBIPFilePath -Parent
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($PBIPFilePath)
    $reportFolder = Join-Path $pbipDir "$baseName.Report"
    $semanticModelFolder = Join-Path $pbipDir "$baseName.SemanticModel"
    
    $isValid = (Test-Path $reportFolder) -and (Test-Path $semanticModelFolder)
    
    if ($isValid) {
        Write-Host "✓ PBIP structure validated for: $baseName"
        
        $reportDefFile = Join-Path $reportFolder "report.json"
        $modelBimFile = Get-ChildItem -Path $semanticModelFolder -Filter "model.bim" -Recurse | Select-Object -First 1
        
        Write-Host "    Report definition: $(Test-Path $reportDefFile)"
        Write-Host "    Model BIM file: $($modelBimFile -ne $null)"
        
        return @{
            IsValid = $true
            ReportFolder = $reportFolder
            SemanticModelFolder = $semanticModelFolder
            ReportDefFile = $reportDefFile
            ModelBimFile = if ($modelBimFile) { $modelBimFile.FullName } else { $null }
        }
    } else {
        Write-Warning "Invalid PBIP structure for: $baseName"
        Write-Warning "  Missing Report folder: $(-not (Test-Path $reportFolder))"
        Write-Warning "  Missing SemanticModel folder: $(-not (Test-Path $semanticModelFolder))"
        
        return @{
            IsValid = $false
        }
    }
}

function Verify-WorkspaceAccess {
    param(
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken
    )
    
    try {
        Write-Host "Verifying access to workspace: $WorkspaceId"
        
        $headers = @{
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
        $response = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers
        
        Write-Host "✓ Workspace access verified: $($response.displayName)"
        return $true
    }
    catch {
        Write-Error "Failed to access workspace: $_"
        return $false
    }
}

function Wait-FabricOperationCompletion {
    param(
        [Parameter(Mandatory=$true)]
        [string]$OperationStatusUrl,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [int]$MaxWaitSeconds = 180
    )

    $headers = @{
        "Authorization" = "Bearer $AccessToken"
        "Content-Type" = "application/json"
    }

    $elapsed = 0
    $interval = 5
    while ($elapsed -lt $MaxWaitSeconds) {
        try {
            $resp = Invoke-RestMethod -Uri $OperationStatusUrl -Method Get -Headers $headers -ErrorAction Stop
            $status = $resp.status
            if (-not $status) { $status = $resp.state }
            if ($status -and ($status -in @('Succeeded','Completed'))) { return $true }
            if ($status -and ($status -in @('Failed','Error'))) {
                Write-Error "Fabric operation failed: $($resp | ConvertTo-Json -Depth 10)"
                return $false
            }
        } catch {
            Write-Warning "Failed to poll operation status: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }
    Write-Warning "Operation did not complete within $MaxWaitSeconds seconds"
    return $false
}

function List-WorkspaceItems {
    param(
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken
    )
    
    try {
        $headers = @{
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
        $response = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers
        
        Write-Host "Workspace items found: $($response.value.Count)"
        foreach ($item in $response.value) {
            Write-Host "  - $($item.displayName) ($($item.type))"
        }
        
        return $response.value
    }
    catch {
        Write-Warning "Failed to list workspace items: $_"
        return @()
    }
}

function Debug-PBIPContent {
    param(
        [Parameter(Mandatory=$true)]
        [string]$PBIPFilePath
    )
    
    try {
        Write-Host "Analyzing PBIP content..."
        
        # Basic file analysis
        $fileInfo = Get-Item $PBIPFilePath
        Write-Host "PBIP file info:"
        Write-Host "  - File size: $([math]::Round($fileInfo.Length / 1KB, 2)) KB"
        Write-Host "  - Last modified: $($fileInfo.LastWriteTime)"
        
        $pbipDir = Split-Path $PBIPFilePath -Parent
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($PBIPFilePath)
        
        # Check Report folder
        $reportFolder = Join-Path $pbipDir "$baseName.Report"
        if (Test-Path $reportFolder) {
            $reportFiles = Get-ChildItem $reportFolder -Recurse
            Write-Host "  - Report files: $($reportFiles.Count)"
        }
        
        # Check SemanticModel folder
        $semanticModelFolder = Join-Path $pbipDir "$baseName.SemanticModel"
        if (Test-Path $semanticModelFolder) {
            $modelFiles = Get-ChildItem $semanticModelFolder -Recurse
            Write-Host "  - Semantic model files: $($modelFiles.Count)"
            
            $modelBim = Get-ChildItem $semanticModelFolder -Filter "model.bim" -Recurse
            if ($modelBim) {
                $modelSize = [math]::Round($modelBim.Length / 1KB, 2)
                Write-Host "  - Model.bim size: $modelSize KB"
            }
        }
    }
    catch {
        Write-Warning "Could not analyze PBIP content: $_"
    }
}

function Wait-ForDeploymentCompletion {
    param(
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ItemName,
        [Parameter(Mandatory=$true)]
        [string]$ItemType,
        [int]$MaxWaitMinutes = 5
    )
    
    $maxWaitTime = $MaxWaitMinutes * 60 # Convert to seconds
    $waitTime = 0
    $checkInterval = 15 # Check every 15 seconds
    
    Write-Host "Waiting for $ItemType '$ItemName' to appear in workspace..."
    
    do {
        Start-Sleep -Seconds $checkInterval
        $waitTime += $checkInterval
        
        try {
            $headers = @{
                "Authorization" = "Bearer $AccessToken"
                "Content-Type" = "application/json"
            }
            
            $item = $null
            if ($ItemType -eq "SemanticModel") {
                $uriSm = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
                $responseSm = Invoke-RestMethod -Uri $uriSm -Method Get -Headers $headers
                $item = $responseSm.value | Where-Object { $_.displayName -eq $ItemName }
            } elseif ($ItemType -eq "Report") {
                $uriRpt = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
                $responseRpt = Invoke-RestMethod -Uri $uriRpt -Method Get -Headers $headers
                $item = $responseRpt.value | Where-Object { $_.displayName -eq $ItemName }
            }
            
            if (-not $item) {
                # Fallback to aggregated items API
                $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
                $response = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers
                $item = $response.value | Where-Object { $_.displayName -eq $ItemName }
            }
            
            if ($item) {
                Write-Host "✓ $ItemType '$ItemName' found in workspace"
                return $true
            }
            
            Write-Host "⏳ Waiting... ($waitTime/$maxWaitTime seconds)"
        }
        catch {
            Write-Warning "Error checking for item: $_"
        }
        
    } while ($waitTime -lt $maxWaitTime)
    
    Write-Warning "⚠️ $ItemType '$ItemName' not found after $MaxWaitMinutes minutes"
    return $false
}

function Verify-DeploymentResult {
    param(
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ReportName,
        [Parameter(Mandatory=$true)]
        [string]$SemanticModelName
    )
    
    try {
        $headers = @{
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }
        
        # Get all workspace items
        $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
        $response = Invoke-RestMethod -Uri $uri -Method Get -Headers $headers
        
        # Check for semantic model
        $semanticModel = $response.value | Where-Object { 
            $_.displayName -eq $SemanticModelName -and $_.type -eq "SemanticModel" 
        }
        
        # Check for report
        $report = $response.value | Where-Object { 
            $_.displayName -eq $ReportName -and $_.type -eq "Report" 
        }
        
        return @{
            SemanticModelFound = ($semanticModel -ne $null)
            ReportFound = ($report -ne $null)
            SemanticModelId = if ($semanticModel) { $semanticModel.id } else { $null }
            ReportId = if ($report) { $report.id } else { $null }
        }
    }
    catch {
        Write-Warning "Failed to verify deployment result: $_"
        return @{
            SemanticModelFound = $false
            ReportFound = $false
            SemanticModelId = $null
            ReportId = $null
        }
    }
}

function Deploy-SemanticModel {
    param (
        [string]$WorkspaceId,
        [string]$SemanticModelName,
        [string]$SemanticModelPath,
        [string]$AccessToken
    )

    $headers = @{
        "Authorization" = "Bearer $AccessToken"
        "Content-Type"  = "application/json"
    }

    # 1. Check if semantic model already exists
    $semanticModelsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
    $existingModels = Invoke-RestMethod -Method Get -Uri $semanticModelsUrl -Headers $headers

    $model = $existingModels.value | Where-Object { $_.displayName -eq $SemanticModelName }

    if ($null -ne $model) {
        Write-Host "✅ Semantic Model '$SemanticModelName' already exists. Updating..."

        $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$($model.id)/updateDefinition"

        # Read all parts
        $parts = @(
            @{ path = "definition/Model.bim";        payload = Get-Content "$SemanticModelPath/definition/Model.bim" -Raw; mimeType = "application/json" },
            @{ path = "definition/diagramLayout.json"; payload = Get-Content "$SemanticModelPath/definition/diagramLayout.json" -Raw; mimeType = "application/json" },
            @{ path = "definition/perspectives.perspective"; payload = Get-Content "$SemanticModelPath/definition/perspectives.perspective" -Raw; mimeType = "application/json" }
        )

        $body = @{
            parts = $parts
        } | ConvertTo-Json -Depth 10 -Compress

        try {
            Invoke-RestMethod -Method Post -Uri $updateUrl -Headers $headers -Body $body
            Write-Host "✅ Semantic Model '$SemanticModelName' updated successfully."
        }
        catch {
            Write-Error "❌ Failed to update Semantic Model: $_"
        }
    }
    else {
        Write-Host "ℹ️ Semantic Model '$SemanticModelName' not found. Creating new one..."

        $createUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"

        $parts = @(
            @{ path = "definition/Model.bim";        payload = Get-Content "$SemanticModelPath/definition/Model.bim" -Raw; mimeType = "application/json" },
            @{ path = "definition/diagramLayout.json"; payload = Get-Content "$SemanticModelPath/definition/diagramLayout.json" -Raw; mimeType = "application/json" },
            @{ path = "definition/perspectives.perspective"; payload = Get-Content "$SemanticModelPath/definition/perspectives.perspective" -Raw; mimeType = "application/json" }
        )

        $body = @{
            displayName = $SemanticModelName
            parts       = $parts
        } | ConvertTo-Json -Depth 10 -Compress

        try {
            Invoke-RestMethod -Method Post -Uri $createUrl -Headers $headers -Body $body
            Write-Host "✅ Semantic Model '$SemanticModelName' created successfully."
        }
        catch {
            Write-Error "❌ Failed to create Semantic Model: $_"
        }
    }
}

function Deploy-Report {
    param(
        [Parameter(Mandatory=$true)]
        [string]$ReportFolder,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ReportName,
        [string]$SemanticModelId
    )

    try {
        Write-Host "Deploying report: $ReportName"

        $reportJsonFile = Get-ChildItem -Path $ReportFolder -Filter "report.json" -Recurse | Select-Object -First 1
        if (-not $reportJsonFile) {
            throw "report.json not found in report folder"
        }

        $reportDefinitionRaw = Get-Content $reportJsonFile.FullName -Raw
        try {
            $reportJson = $reportDefinitionRaw | ConvertFrom-Json
        } catch {
            throw "Failed to parse report.json: $_"
        }

        # Bind datasetId if provided
        if ($SemanticModelId) {
            $reportJson.datasetId = $SemanticModelId
            Write-Host "✓ Bound report to semantic model ID: $SemanticModelId"
        }

        $reportDefinition = $reportJson | ConvertTo-Json -Depth 50

        # Build parts (report.json mainly)
        $rptParts = @()
        $rptParts += @{
            path        = "report.json"
            payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($reportDefinition))
            payloadType = "InlineBase64"
        }

        $deploymentPayload = @{
            displayName = $ReportName
            description = "Report deployed from PBIP: $ReportName"
            definition  = @{ parts = $rptParts }
        } | ConvertTo-Json -Depth 30

        $deployUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
        $headers = @{
            "Authorization" = "Bearer $AccessToken"
            "Content-Type"  = "application/json"
        }

        try {
            $createResp = Invoke-WebRequest -Uri $deployUrl -Method Post -Body $deploymentPayload -Headers $headers -ContentType "application/json"
            $reportId = $null
            $content = $null
            try { $content = $createResp.Content | ConvertFrom-Json } catch {}
            if ($content -and $content.id) { $reportId = $content.id }

            if (-not $reportId) {
                # Resolve by name if no id in response
                $listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
                $listResponse = Invoke-RestMethod -Uri $listUrl -Method Get -Headers $headers
                $existing = $listResponse.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
                if ($existing) { $reportId = $existing.id }
            }

            if (-not $reportId) { throw "Report id could not be determined after creation" }

            Write-Host "✓ Report deployed successfully"
            Write-Host "Report ID: $reportId"
            return $true
        } catch {
            $statusCode = $null
            $errBody = $null
            try { $statusCode = $_.Exception.Response.StatusCode } catch {}
            try {
                $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errBody = $reader.ReadToEnd()
            } catch {}

            if ($statusCode -eq 409) {
                Write-Host "Report already exists, attempting to update..."
                try {
                    $listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
                    $listResponse = Invoke-RestMethod -Uri $listUrl -Method Get -Headers $headers
                    $existing = $listResponse.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1

                    if ($existing) {
                        $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$($existing.id)/updateDefinition"
                        $updatePayload = @{
                            definition = @{
                                parts = @(
                                    @{
                                        path        = "report.json"
                                        payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($reportDefinition))
                                        payloadType = "InlineBase64"
                                    }
                                )
                            }
                        } | ConvertTo-Json -Depth 10

                        $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updatePayload -Headers $headers
                        Write-Host "✓ Report updated successfully"
                        return $true
                    } else {
                        Write-Warning "Could not find existing report to update"
                        return $false
                    }
                } catch {
                    Write-Error "Failed to update existing report: $_"
                    return $false
                }
            } else {
                Write-Error "Report creation failed. Status: $statusCode Body: $errBody"
                throw $_
            }
        }
    } catch {
        Write-Error "Failed to deploy report: $_"
        return $false
    }
}


function Deploy-PBIP {
    param (
        [string]$PBIPPath,
        [string]$WorkspaceId,
        [string]$ModelName,     # semantic model name
        [string]$ReportName     # report name
    )

    Write-Host "🚀 Starting deployment for: $PBIPPath to workspace: $WorkspaceId"

    # ---- Pre-deployment checks ----
    if (-not (Verify-WorkspaceAccess -WorkspaceId $WorkspaceId)) {
        throw "❌ Workspace access verification failed"
    }

    if (-not (Validate-PBIPStructure -PBIPPath $PBIPPath)) {
        throw "❌ PBIP structure validation failed"
    }

    Debug-PBIPContent -PBIPPath $PBIPPath
    $preDeploymentItems = List-WorkspaceItems -WorkspaceId $WorkspaceId

    $warnings = @()
    $overallSuccess = $true

    try {
        # ---- Deploy Semantic Model ----
        Write-Host "`n📦 Deploying Semantic Model: $ModelName"
        if (-not (Deploy-SemanticModel -PBIPPath $PBIPPath -WorkspaceId $WorkspaceId -ModelName $ModelName)) {
            throw "Semantic model deployment failed"
        }

        if (-not (Wait-ForDeploymentCompletion -WorkspaceId $WorkspaceId -ItemName $ModelName -MaxWaitMinutes 3)) {
            throw "Semantic model deployment did not complete in time"
        }

        if (-not (Verify-ItemDeployment -WorkspaceId $WorkspaceId -ItemName $ModelName)) {
            throw "Semantic model verification failed"
        }

        # ---- Deploy Report ----
        Write-Host "`n📊 Deploying Report: $ReportName"
        if (-not (Deploy-Report -PBIPPath $PBIPPath -WorkspaceId $WorkspaceId -ReportName $ReportName)) {
            throw "Report deployment failed"
        }

        if (-not (Wait-ForDeploymentCompletion -WorkspaceId $WorkspaceId -ItemName $ReportName -MaxWaitMinutes 3)) {
            throw "Report deployment did not complete in time"
        }

        if (-not (Verify-ItemDeployment -WorkspaceId $WorkspaceId -ItemName $ReportName)) {
            throw "Report verification failed"
        }

        # ---- Post-deployment verification ----
        $postDeploymentItems = List-WorkspaceItems -WorkspaceId $WorkspaceId
        $newItemsCount = $postDeploymentItems.Count - $preDeploymentItems.Count

        if ($newItemsCount -gt 0) {
            Write-Host "✅ Deployment successful - $newItemsCount new items detected"
        }
        else {
            $msg = "⚠️ No new items detected - deployment may have been an update"
            Write-Warning $msg
            $warnings += $msg
        }

        if (-not (Verify-DeploymentResult -WorkspaceId $WorkspaceId -PreItems $preDeploymentItems -PostItems $postDeploymentItems)) {
            throw "Final deployment verification failed"
        }
    }
    catch {
        Write-Host "`n❌ Deployment failed: $($_.Exception.Message)"
        $overallSuccess = $false
        # show workspace items for analysis
        List-WorkspaceItems -WorkspaceId $WorkspaceId
    }

    # ---- Return result ----
    return [PSCustomObject]@{
        Success      = $overallSuccess
        ModelName    = $ModelName
        ReportName   = $ReportName
        Warnings     = $warnings
    }
}


# ===============================
# MAIN EXECUTION LOGIC
# ===============================

try {
    Write-Host "Starting Power BI PBIP Report Deployment..."
    Write-Host "Environment: $Workspace"
    Write-Host "Config File: $ConfigFile"

    # Ensure TLS 1.2 is enabled
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    # Read configuration file
    if (-not (Test-Path $ConfigFile)) {
        throw "Configuration file not found: $ConfigFile"
    }

    $config = Get-Content -Raw $ConfigFile | ConvertFrom-Json
    Write-Host "Configuration loaded successfully"

    # Get SPN credentials from config
    $tenantId = $config.TenantID
    $clientId = $config.ClientID
    $clientSecret = $config.ClientSecret

    Write-Host "Using Tenant ID: $tenantId"
    Write-Host "Using Client ID: $clientId"

    # Map workspace based on environment
    $targetWorkspaceId = $null
    
    switch ($Workspace.ToUpper()) {
        "DEV" {
            $targetWorkspaceId = $config.DevWorkspaceID
        }
        "PROD" {
            $targetWorkspaceId = $config.ProdWorkspaceID
        }
        default {
            throw "Unsupported environment: $Workspace. Only DEV and PROD are supported."
        }
    }

    if (-not $targetWorkspaceId) {
        throw "Workspace ID not found for environment: $Workspace"
    }

    Write-Host "Target Workspace ID: $targetWorkspaceId"

    # Get Access Token
    $accessToken = Get-SPNToken -TenantId $tenantId -ClientId $clientId -ClientSecret $clientSecret
    if (-not $accessToken) {
        throw "Failed to obtain access token. Check TenantID/ClientID/ClientSecret and app permissions."
    }

    # Set artifact path
    $artifactPath = $env:BUILD_SOURCESDIRECTORY
    if (-not $artifactPath) {
        $artifactPath = $env:artifact_path
    }
    if (-not $artifactPath) {
        $artifactPath = (Get-Location).Path
    }
    Write-Host "Using artifact path: $artifactPath"

    # Search for PBIP files
    Write-Host "Searching for PBIP files..."
    $reportFolders = @("Demo Report", "Reporting", "Reports", "PowerBI", "BI")
    $allPbipFiles = @()
    
    foreach ($folder in $reportFolders) {
        $folderPath = Join-Path $artifactPath $folder
        if (Test-Path $folderPath) {
            $pbipFiles = Get-PBIPFiles -ArtifactPath $artifactPath -Folder $folder
            $allPbipFiles += $pbipFiles
            Write-Host "Found $($pbipFiles.Count) PBIP files in $folder folder"
        }
    }

    # If no PBIP files found in specific folders, search entire repository
    if ($allPbipFiles.Count -eq 0) {
        Write-Host "No PBIP files found in expected folders, searching entire repository..."
        $allPbipFiles = Get-ChildItem -Path $artifactPath -Recurse -Filter "*.pbip" -ErrorAction SilentlyContinue
        Write-Host "Found $($allPbipFiles.Count) PBIP files total"
    }

    if ($allPbipFiles.Count -eq 0) {
        throw "No PBIP files found in the repository"
    }

    Write-Host "`n=== PBIP DEPLOYMENT ==="
    $deploymentResults = @()
    
    foreach ($pbipFile in $allPbipFiles) {
        $reportName = [System.IO.Path]::GetFileNameWithoutExtension($pbipFile.Name)
        Write-Host "`nProcessing PBIP: $reportName"
        Write-Host "File path: $($pbipFile.FullName)"

        # Determine connection settings based on target environment
        if ($Workspace.ToUpper() -eq 'DEV') {
            $serverName = $config.DevWarehouseConnection
            $databaseName = $config.DevWarehouseName
        } else {
            $serverName = $config.ProdWarehouseConnection
            $databaseName = $config.ProdWarehouseName
        }
        Write-Host "Using connection -> Server: $serverName | Database: $databaseName"

        $deploymentSuccess = Deploy-PBIP -PBIPFilePath $pbipFile.FullName -ReportName $reportName -WorkspaceId $targetWorkspaceId -AccessToken $accessToken -ServerName $serverName -DatabaseName $databaseName
        
        $result = [PSCustomObject]@{
            ReportName = $reportName
            FilePath = $pbipFile.FullName
            DeploymentSuccess = $deploymentSuccess
            Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            Environment = $Workspace
            WorkspaceId = $targetWorkspaceId
        }
        
        $deploymentResults += $result
        
        if ($deploymentSuccess) {
            Write-Host "✓ Successfully deployed: $reportName"
        } else {
            Write-Warning "❌ Failed to deploy: $reportName"
        }
    }

    # Summary
    Write-Host "`n=== DEPLOYMENT SUMMARY ==="
    $successCount = ($deploymentResults | Where-Object { $_.DeploymentSuccess }).Count
    $totalCount = $deploymentResults.Count
    
    Write-Host "Total PBIP files processed: $totalCount"
    Write-Host "Successful deployments: $successCount"
    Write-Host "Failed deployments: $($totalCount - $successCount)"

    # Display detailed results
    Write-Host "`n=== DETAILED RESULTS ==="
    foreach ($result in $deploymentResults) {
        $status = if ($result.DeploymentSuccess) { "✓ SUCCESS" } else { "❌ FAILED" }
        Write-Host "$status - $($result.ReportName) [$($result.Environment)] - $($result.Timestamp)"
    }

    # Fail the deployment if any PBIP file failed to deploy
    if ($successCount -lt $totalCount) {
        $failedReports = $deploymentResults | Where-Object { -not $_.DeploymentSuccess } | Select-Object -ExpandProperty ReportName
        Write-Error "The following reports failed to deploy: $($failedReports -join ', ')"
        throw "One or more PBIP deployments failed"
    }

    Write-Host "`n✓ Power BI PBIP Report Deployment completed successfully!"
    Write-Host "========================================="
    Write-Host "FINAL SUMMARY"
    Write-Host "========================================="
    Write-Host "Environment: $Workspace"
    Write-Host "Workspace ID: $targetWorkspaceId"
    Write-Host "Total Reports: $totalCount"
    Write-Host "Successful Deployments: $successCount"
    Write-Host "Failed Deployments: $($totalCount - $successCount)"
    Write-Host "Success Rate: $([math]::Round(($successCount / $totalCount) * 100, 2))%"
    Write-Host "Deployment Completed: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host "========================================="
}
catch {
    Write-Error "Power BI PBIP Report Deployment failed: $_"
    Write-Host "`n=== ERROR DETAILS ==="
    Write-Host "Error Message: $($_.Exception.Message)"
    Write-Host "Error Location: $($_.InvocationInfo.ScriptName):$($_.InvocationInfo.ScriptLineNumber)"
    Write-Host "Failed Command: $($_.InvocationInfo.Line.Trim())"
    Write-Host "Stack Trace: $($_.ScriptStackTrace)"
    
    # If we have deployment results, show what we accomplished
    if ($deploymentResults -and $deploymentResults.Count -gt 0) {
        Write-Host "`n=== PARTIAL RESULTS BEFORE FAILURE ==="
        foreach ($result in $deploymentResults) {
            $status = if ($result.DeploymentSuccess) { "✓ SUCCESS" } else { "❌ FAILED" }
            Write-Host "$status - $($result.ReportName)"
        }
    }
    
    exit 1
}