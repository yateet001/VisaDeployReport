# 2MainOrchestrator.ps1 for PBIP file deployment (Complete Fixed Version)
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
    param(
        [Parameter(Mandatory=$true)]
        [string]$SemanticModelFolder,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [Parameter(Mandatory=$true)]
        [string]$ModelName,
        [Parameter(Mandatory=$true)]
        [string]$ServerName,
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName
    )
    $deployedModelId   = $null
    $deployedModelName = $null

    try {
        Write-Host "Deploying semantic model: $ModelName"

        $modelBimFile = Get-ChildItem -Path $SemanticModelFolder -Filter "model.bim" -Recurse | Select-Object -First 1
        if (-not $modelBimFile) { throw "model.bim file not found in semantic model folder" }

        $modelDefinitionRaw = Get-Content $modelBimFile.FullName -Raw
        $modelJson = $modelDefinitionRaw | ConvertFrom-Json

        # Connection switching
        $pattern = 'Sql\.Database\(".*?"\s*,\s*".*?"(?:\s*,\s*\[.*?\])?\)'
        $replacement = 'Sql.Database("' + $ServerName + '", "' + $DatabaseName + '")'
        $updatesApplied = 0

        foreach ($table in $modelJson.model.tables) {
            foreach ($partition in $table.partitions) {
                if ($partition.source -and $partition.source.type -eq 'm' -and $partition.source.expression) {
                    if ($partition.source.expression -is [System.Array]) {
                        $partition.source.expression = $partition.source.expression | ForEach-Object { $_ -replace $pattern, $replacement }
                        $updatesApplied++
                    } elseif ($partition.source.expression -is [string]) {
                        $partition.source.expression = $partition.source.expression -replace $pattern, $replacement
                        $updatesApplied++
                    }
                }
            }
        }

        if ($updatesApplied -gt 0) {
            Write-Host "✓ Connection switching applied to $updatesApplied partition(s)"
        }

        $modelDefinition = $modelJson | ConvertTo-Json -Depth 100

        # Build parts
        $smParts = @()
        $smDir = Split-Path $modelBimFile.FullName -Parent
        $smParts += @{
            path = 'model.bim'
            payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modelDefinition))
            payloadType = 'InlineBase64'
        }
        foreach ($optional in @('diagramLayout.json','definition.pbism')) {
            $optPath = Join-Path $smDir $optional
            if (Test-Path $optPath) {
                $bytes = [System.IO.File]::ReadAllBytes($optPath)
                $smParts += @{ path = $optional; payload = [Convert]::ToBase64String($bytes); payloadType = 'InlineBase64' }
            }
        }

        $headers = @{ "Authorization" = "Bearer $AccessToken"; "Content-Type" = "application/json" }

        # 🔑 Step 1: Check if model exists already
        $listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
        $listResponse = Invoke-RestMethod -Uri $listUrl -Method Get -Headers $headers
        $existingModel = $listResponse.value | Where-Object { $_.displayName -eq $ModelName } | Select-Object -First 1

        if ($existingModel) {
            Write-Host "Semantic model already exists (ID: $($existingModel.id)) → updating definition..."
            $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$($existingModel.id)/updateDefinition"
            $updatePayload = @{ definition = @{ parts = $smParts } } | ConvertTo-Json -Depth 50
            Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updatePayload -Headers $headers
            Write-Host "✓ Semantic model updated successfully"
            $deployedModelId = $existingModel.id
            $deployedModelName = $existingModel.displayName
            return @{ Success = $true; ModelId = $existingModel.id }
        }
        else {
            Write-Host "No existing model found → creating new semantic model..."
            $deploymentPayload = @{
                displayName = $ModelName
                description = "Semantic model deployed from PBIP: $ModelName"
                definition = @{ parts = $smParts }
            } | ConvertTo-Json -Depth 50

            $deployUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
            $createResp = Invoke-RestMethod -Uri $deployUrl -Method Post -Body $deploymentPayload -Headers $headers
            Write-Host "✓ Semantic model created successfully (ID: $($createResp.id))"
            $deployedModelId =  $createResp.id 
            $deployedModelName = $createResp.displayName 
            return @{ Success = $true; ModelId = $createResp.id }
        }

    } catch {
        Write-Error "Failed to deploy semantic model: $($_)"
        return @{ Success = $false; deployedModelId = $null; Error = "$($_)" }
    }
    # # 🔄 Step 2: Trigger refresh (Fabric API)
    #     $refreshUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$deployedModelId/refreshes"
    #     Write-Host "Triggering refresh for semantic model (ID: $deployedModelId)..."
    #     Write-Host "Refresh URL: $refreshUrl"
    #     $refreshPayload = "{}" | ConvertTo-Json
    #     $refreshHeaders = @{
    #          "Authorization" = "Bearer $AccessToken"
    #         #  "Content-Type" = "application/json"
    #     }
    #     Invoke-RestMethod -Uri $refreshUrl -Method Post -Headers $refreshHeaders -Body $refreshPayload
    #     Invoke-RestMethod -Uri $refreshUrl -Method Post -Headers $headers
    #     Write-Host "✓ Refresh triggered (Fabric PBIP model)"
        
    #      try {
    #         Invoke-RestMethod `
    #             -Uri $refreshUrl `
    #             -Method Post `
    #             -Headers @{ "Authorization" = "Bearer $AccessToken" }  # ⚡ No Content-Type, No Body
    #         Write-Host "✓ Refresh triggered successfully"
    #     }
    #     catch {
    #         Write-Host "❌ Refresh failed: $($_.Exception.Message)"
    #     }

    #     # ✅ Return JSON with id + name
    #     $output = @{
    #         Success       = $true
    #         ModelId       = $deployedModelId
    #         Name          = $deployedModelName
    #         RefreshStatus = "Triggered"
    #     }| ConvertTo-Json -Depth 5
    #     Write-Output $output

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
        [string]$SemanticModelId = $null
    )
    
    try {
        Write-Host "Deploying report: $ReportName"
        
        $reportJsonFile = Join-Path $ReportFolder "report.json"
        if (-not (Test-Path $reportJsonFile)) {
            throw "report.json file not found in report folder"
        }

        $headers = @{ 
            "Authorization" = "Bearer $AccessToken"
            "Content-Type" = "application/json"
        }

        # Resolve SemanticModelId if not provided
        if (-not $SemanticModelId) {
            Write-Host "Resolving SemanticModelId by name..."
            $listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
            $listResponse = Invoke-RestMethod -Uri $listUrl -Method Get -Headers $headers
            $existingModel = $listResponse.value | Where-Object { $_.displayName -eq $ReportName } | Select-Object -First 1
            if ($existingModel) { 
                $SemanticModelId = $existingModel.id 
                Write-Host "Found SemanticModelId: $SemanticModelId"
            }
        }

        if (-not $SemanticModelId) {
            throw "Cannot deploy report without SemanticModelId"
        }

        # Check if report already exists first
        $listReportsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
        $existingReports = Invoke-RestMethod -Uri $listReportsUrl -Method Get -Headers $headers
        $existingReport = $existingReports.value | Where-Object { $_.displayName -eq $ReportName }
        
        if ($existingReport) {
            Write-Host "Report already exists (ID: $($existingReport.id)) - updating..."
            
            # Build parts for update
            $allFiles = Get-ChildItem -Path $ReportFolder -Recurse -File
            $parts = @()
            foreach ($file in $allFiles) {
                $relativePath = ($file.FullName.Substring($ReportFolder.Length) -replace '^[\\/]+','')
                $relativePath = $relativePath -replace '\\','/'
                $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
                $b64 = [Convert]::ToBase64String($bytes)
                $parts += @{
                    path = $relativePath
                    payload = $b64
                    payloadType = 'InlineBase64'
                }
            }
            
            $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$($existingReport.id)/updateDefinition"
            $updatePayload = @{
                definition = @{ parts = $parts }
            } | ConvertTo-Json -Depth 50

            try {
                $updateResponse = Invoke-RestMethod -Uri $updateUrl -Method Post -Body $updatePayload -Headers $headers
                Write-Host "✓ Report updated successfully"
                Write-Host "Update response: $($updateResponse | ConvertTo-Json -Depth 3)"
                return $true
            } catch {
                Write-Warning "Report update failed: $($_.Exception.Message)"
                if ($_.Exception.Response) {
                    $errorStream = $_.Exception.Response.GetResponseStream()
                    $reader = New-Object System.IO.StreamReader($errorStream)
                    $errorBody = $reader.ReadToEnd()
                    Write-Warning "Error details: $errorBody"
                }
                return $false
            }
        } else {
            Write-Host "Creating new report..."
            
            # Build parts for creation
            $allFiles = Get-ChildItem -Path $ReportFolder -Recurse -File
            $parts = @()
            foreach ($file in $allFiles) {
                $relativePath = ($file.FullName.Substring($ReportFolder.Length) -replace '^[\\/]+','')
                $relativePath = $relativePath -replace '\\','/'
                $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
                $b64 = [Convert]::ToBase64String($bytes)
                $parts += @{
                    path = $relativePath
                    payload = $b64
                    payloadType = 'InlineBase64'
                }
            }

            # Try Fabric Items API first
            try {
                Write-Host "Attempting creation via Fabric Items API..."
                $createItemUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
                
                # Test the endpoint first
                Write-Host "Testing workspace access..."
                $testUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
                try {
                    $workspaceInfo = Invoke-RestMethod -Uri $testUrl -Method Get -Headers $headers
                    Write-Host "Workspace found: $($workspaceInfo.displayName)"
                } catch {
                    Write-Error "Cannot access workspace $WorkspaceId - Error: $($_.Exception.Message)"
                    return $false
                }
                
                $itemPayload = @{
                    displayName = $ReportName
                    type = "Report"
                    definition = @{
                        format = "PBIR"
                        parts = $parts
                    }
                }
                
                # Make the API call with enhanced error handling
                try {
                    $response = Invoke-WebRequest -Uri $createItemUrl -Method Post -Body ($itemPayload | ConvertTo-Json -Depth 50) -Headers $headers -UseBasicParsing
                    
                    Write-Host "HTTP Status: $($response.StatusCode)"
                    Write-Host "Response Headers: $($response.Headers | ConvertTo-Json)"
                    Write-Host "Raw Response Content: $($response.Content)"
                    
                    # Handle async operations - check for Location header
                    if ($response.Headers["Location"]) {
                        $operationUrl = $response.Headers["Location"]
                        Write-Host "Operation started, polling status at: $operationUrl"
                        
                        # Wait for the operation to complete
                        $operationResult = Wait-ForOperation -OperationUrl $operationUrl -Headers $headers -MaxWaitTimeMinutes 10
                        
                        if ($operationResult -and $operationResult.result -and $operationResult.result.id) {
                            Write-Host "✓ Report created successfully via async operation (ID: $($operationResult.result.id))"
                            return $true
                        }
                    }
                    
                    # Try to parse JSON response
                    if ($response.Content) {
                        $jsonResponse = $response.Content | ConvertFrom-Json
                        Write-Host "Parsed JSON Response: $($jsonResponse | ConvertTo-Json -Depth 5)"
                        
                        # Check for various possible ID fields
                        $reportId = $null
                        if ($jsonResponse.id) {
                            $reportId = $jsonResponse.id
                        } elseif ($jsonResponse.objectId) {
                            $reportId = $jsonResponse.objectId
                        } elseif ($jsonResponse.reportId) {
                            $reportId = $jsonResponse.reportId
                        }
                        
                        if ($reportId) {
                            Write-Host "✓ Report created successfully via Fabric Items API (ID: $reportId)"
                            return $true
                        }
                    }
                    
                    # If we get here, the API call succeeded but we can't find an ID
                    Write-Warning "Fabric Items API succeeded but no ID found in response"
                    
                } catch {
                    Write-Error "Fabric Items API request failed: $($_.Exception.Message)"
                    if ($_.Exception.Response) {
                        $errorResponse = $_.Exception.Response
                        Write-Error "Status Code: $($errorResponse.StatusCode)"
                        Write-Error "Status Description: $($errorResponse.StatusDescription)"
                        
                        try {
                            $errorStream = $errorResponse.GetResponseStream()
                            $reader = New-Object System.IO.StreamReader($errorStream)
                            $errorBody = $reader.ReadToEnd()
                            Write-Error "Error Body: $errorBody"
                        } catch {
                            Write-Error "Could not read error response body"
                        }
                    }
                    throw
                }
                
            } catch {
                Write-Warning "Fabric Items API failed: $($_.Exception.Message)"
                Write-Host "Trying alternative approach with createReport endpoint..."
                
                # Try the createReport endpoint directly
                try {
                    $createReportUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
                    
                    # Simplified payload for report creation
                    $reportPayload = @{
                        displayName = $ReportName
                        definition = @{
                            format = "PBIR"
                            parts = $parts
                        }
                    }
                    
                    Write-Host "Attempting direct report creation..."
                    $reportResponse = Invoke-RestMethod -Uri $createReportUrl -Method Post -Body ($reportPayload | ConvertTo-Json -Depth 50) -Headers $headers
                    
                    Write-Host "Direct report API Response: $($reportResponse | ConvertTo-Json -Depth 5)"
                    
                    if ($reportResponse.id) {
                        Write-Host "✓ Report created successfully via direct report API (ID: $($reportResponse.id))"
                        return $true
                    }
                    
                } catch {
                    Write-Warning "Direct report API also failed: $($_.Exception.Message)"
                }
                
                # Final fallback: Try Power BI Import API with proper multipart form
                Write-Host "Falling back to Power BI Import API..."
                try {
                    # First, check if we can access the workspace via Power BI API
                    $powerBIHeaders = @{ 
                        "Authorization" = "Bearer $AccessToken"
                    }
                    
                    $workspaceCheckUrl = "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId"
                    try {
                        $workspaceCheck = Invoke-RestMethod -Uri $workspaceCheckUrl -Method Get -Headers $powerBIHeaders
                        Write-Host "Power BI workspace accessible: $($workspaceCheck.name)"
                    } catch {
                        Write-Error "Cannot access workspace via Power BI API: $($_.Exception.Message)"
                        return $false
                    }
                    
                    # For PBIR files, we need to use the import endpoint differently
                    # This is a complex process that might require converting PBIR to PBIX first
                    Write-Warning "PBIR to Power BI conversion not yet implemented in this script"
                    Write-Host "Consider using Power BI Desktop to publish the report, or implement PBIR to PBIX conversion"
                    
                    return $false
                    
                } catch {
                    Write-Error "Power BI fallback also failed: $($_.Exception.Message)"
                    return $false
                }
            }
        }
        
    } catch {
        Write-Error "Failed to deploy report: $_"
        if ($_.Exception.Response) {
            $errorStream = $_.Exception.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($errorStream)
            $errorBody = $reader.ReadToEnd()
            Write-Error "Detailed error: $errorBody"
        }
        return $false
    }
}

# Helper function to wait for long-running operations
function Wait-ForOperation {
    param(
        [string]$OperationUrl,
        [hashtable]$Headers,
        [int]$MaxWaitTimeMinutes = 10
    )
    
    Write-Host "Waiting for long-running operation to complete..."
    $maxWaitTime = (Get-Date).AddMinutes($MaxWaitTimeMinutes)
    $attempt = 1
    
    do {
        Write-Host "Checking operation status (attempt $attempt)..."
        Start-Sleep -Seconds 15
        
        try {
            $operationResponse = Invoke-RestMethod -Uri $OperationUrl -Method Get -Headers $Headers
            Write-Host "Operation response: $($operationResponse | ConvertTo-Json -Depth 3)"
            
            if ($operationResponse.status) {
                Write-Host "Operation status: $($operationResponse.status)"
                
                if ($operationResponse.status -eq "Succeeded" -or $operationResponse.status -eq "Completed") {
                    Write-Host "✓ Operation completed successfully"
                    return $operationResponse
                } elseif ($operationResponse.status -eq "Failed" -or $operationResponse.status -eq "Error") {
                    $errorMsg = if ($operationResponse.error) { $operationResponse.error } else { "Unknown error" }
                    throw "Operation failed: $errorMsg"
                } elseif ($operationResponse.status -eq "Running" -or $operationResponse.status -eq "InProgress") {
                    Write-Host "Operation still running..."
                } else {
                    Write-Host "Unknown status: $($operationResponse.status)"
                }
            } else {
                # Some APIs don't return a status field, check for completion differently
                if ($operationResponse.result -or $operationResponse.id) {
                    Write-Host "✓ Operation appears to be completed (no status field but has result/id)"
                    return $operationResponse
                }
            }
        } catch {
            Write-Warning "Error checking operation status: $($_.Exception.Message)"
            # Continue trying unless it's a 404 (operation not found)
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode -eq 404) {
                throw "Operation not found (404) - may have completed or expired"
            }
        }
        
        $attempt++
    } while ((Get-Date) -lt $maxWaitTime)
    
    throw "Operation timed out after $MaxWaitTimeMinutes minutes"
}

# Additional helper function to verify workspace access
function Test-WorkspaceAccess {
    param(
        [string]$WorkspaceId,
        [hashtable]$Headers
    )
    
    try {
        $workspaceUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
        $workspace = Invoke-RestMethod -Uri $workspaceUrl -Method Get -Headers $Headers
        Write-Host "✓ Workspace access verified: $($workspace.displayName)"
        return $true
    } catch {
        Write-Error "✗ Cannot access workspace $WorkspaceId`: $($_.Exception.Message)"
        return $false
    }
}

function Deploy-PBIPUsingFabricAPI {
    param(
        [Parameter(Mandatory=$true)]
        [string]$PBIPFilePath,
        [Parameter(Mandatory=$true)]
        [string]$ReportName,
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId,
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        [string]$Takeover = "True",
        [Parameter(Mandatory=$true)]
        [string]$ServerName,
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName
    )
    
    try {
        Write-Host "`n========================================="
        Write-Host "Starting Enhanced PBIP Deployment"
        Write-Host "Report: $ReportName"
        Write-Host "========================================="
        
        # Step 1: Verify workspace access
        Write-Host "`n--- STEP 1: WORKSPACE VERIFICATION ---"
        $workspaceAccessible = Verify-WorkspaceAccess -WorkspaceId $WorkspaceId -AccessToken $AccessToken
        if (-not $workspaceAccessible) {
            throw "Cannot access target workspace"
        }
        
        # Step 2: List current workspace items (before deployment)
        Write-Host "`n--- STEP 2: PRE-DEPLOYMENT INVENTORY ---"
        $preDeploymentItems = List-WorkspaceItems -WorkspaceId $WorkspaceId -AccessToken $AccessToken
        Write-Host "Pre-deployment: Found $($preDeploymentItems.Count) items in workspace"
        
        # Step 3: Validate PBIP structure and content
        Write-Host "`n--- STEP 3: PBIP VALIDATION ---"
        $validation = Validate-PBIPStructure -PBIPFilePath $PBIPFilePath
        if (-not $validation.IsValid) {
            throw "Invalid PBIP structure for: $ReportName"
        }
        
        Debug-PBIPContent -PBIPFilePath $PBIPFilePath
        
        # Step 4: Deploy Semantic Model
        Write-Host "`n--- STEP 4: SEMANTIC MODEL DEPLOYMENT ---"
        $semanticModelResult = Deploy-SemanticModel -SemanticModelFolder $validation.SemanticModelFolder -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ModelName $ReportName -ServerName $ServerName -DatabaseName $DatabaseName
        
        if (-not $semanticModelResult.Success) {
            throw "Semantic model deployment failed: $($semanticModelResult.Error)"
        }
        
        if ($semanticModelResult.Warning) {
            Write-Warning "Semantic model warning: $($semanticModelResult.Warning)"
        }
        
        $semanticModelId = $semanticModelResult.ModelId
        Write-Host "Semantic model result - ID: $semanticModelId"
        
        # Step 5: Wait for semantic model to appear
        Write-Host "`n--- STEP 5: SEMANTIC MODEL VERIFICATION ---"
        $semanticModelReady = Wait-ForDeploymentCompletion -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ItemName $ReportName -ItemType "SemanticModel" -MaxWaitMinutes 3
        
        if (-not $semanticModelReady) {
            Write-Warning "Semantic model not found after deployment, but continuing..."
        }
        
        # Step 6: Deploy Report
        Write-Host "`n--- STEP 6: REPORT DEPLOYMENT ---"
        $reportSuccess = Deploy-Report -ReportFolder $validation.ReportFolder -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ReportName $ReportName -SemanticModelId $semanticModelId
        
        if (-not $reportSuccess) {
            throw "Report deployment failed"
        }
        
        # Step 7: Wait for report to appear
        Write-Host "`n--- STEP 7: REPORT VERIFICATION ---"
        $reportReady = Wait-ForDeploymentCompletion -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ItemName $ReportName -ItemType "Report" -MaxWaitMinutes 3
        
        # Step 8: Final verification
        Write-Host "`n--- STEP 8: FINAL VERIFICATION ---"
        $verificationResult = Verify-DeploymentResult -WorkspaceId $WorkspaceId -AccessToken $AccessToken -ReportName $ReportName -SemanticModelName $ReportName
        
        # Step 9: Post-deployment inventory
        Write-Host "`n--- STEP 9: POST-DEPLOYMENT INVENTORY ---"
        $postDeploymentItems = List-WorkspaceItems -WorkspaceId $WorkspaceId -AccessToken $AccessToken
        Write-Host "Post-deployment: Found $($postDeploymentItems.Count) items in workspace"
        
        $newItems = $postDeploymentItems.Count - $preDeploymentItems.Count
        if ($newItems -gt 0) {
            Write-Host "✓ Added $newItems new item(s) to workspace"
        } else {
            Write-Warning "⚠️ No new items detected in workspace"
        }
        
        # Final assessment
        Write-Host "`n========================================="
        Write-Host "DEPLOYMENT SUMMARY"
        Write-Host "========================================="
        
        $overallSuccess = $verificationResult.SemanticModelFound -and $verificationResult.ReportFound
        
        if ($overallSuccess) {
            Write-Host "✓ DEPLOYMENT SUCCESSFUL"
            Write-Host "  - Semantic Model: ✓ Found"
            Write-Host "  - Report: ✓ Found"
            Write-Host "  - Semantic Model ID: $($verificationResult.SemanticModelId)"
            Write-Host "  - Report ID: $($verificationResult.ReportId)"
        } else {
            Write-Warning "⚠️ DEPLOYMENT ISSUES DETECTED"
            Write-Host "  - Semantic Model: $(if ($verificationResult.SemanticModelFound) { '✓ Found' } else { '❌ Missing' })"
            Write-Host "  - Report: $(if ($verificationResult.ReportFound) { '✓ Found' } else { '❌ Missing' })"
            
            # Provide troubleshooting guidance
            Write-Host "`n--- TROUBLESHOOTING GUIDANCE ---"
            if (-not $verificationResult.SemanticModelFound) {
                Write-Host "• Semantic model missing - check permissions and API scope"
            }
            if (-not $verificationResult.ReportFound) {
                Write-Host "• Report missing - may be deployment timing or API sync issue"
                Write-Host "• Try checking the workspace manually in a few minutes"
            }
        }
        
        Write-Host "========================================="
        
        return $overallSuccess
        
    } catch {
        Write-Error "Enhanced PBIP deployment failed for $ReportName : $_"
        
        # Additional debugging on failure
        Write-Host "`n--- FAILURE ANALYSIS ---"
        try {
            List-WorkspaceItems -WorkspaceId $WorkspaceId -AccessToken $AccessToken
        } catch {
            Write-Warning "Could not list workspace items for failure analysis"
        }
        
        return $false
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

        $deploymentSuccess = Deploy-PBIPUsingFabricAPI -PBIPFilePath $pbipFile.FullName -ReportName $reportName -WorkspaceId $targetWorkspaceId -AccessToken $accessToken -ServerName $serverName -DatabaseName $databaseName
        
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