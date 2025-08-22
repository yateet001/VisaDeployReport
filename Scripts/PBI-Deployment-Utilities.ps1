# PBI-Deployment-Utilities.ps1
# Power BI Deployment Utilities - Handles deployment of semantic models and reports

function Deploy-PBISemanticModel {
    param(
        [Parameter(Mandatory=$true)][string]$WorkspaceId,
        [Parameter(Mandatory=$true)][string]$SemanticModelName,
        [Parameter(Mandatory=$true)][string]$ModelDefinitionPath,
        [Parameter(Mandatory=$true)][string]$AccessToken
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken"; "Content-Type" = "application/json" }

    try {
        Write-Host "Deploying semantic model: $SemanticModelName" -ForegroundColor Cyan
        
        # Check existing semantic models using Fabric API
        $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
        
        try {
            $existingModels = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
            $existingModel = $existingModels.value | Where-Object { $_.displayName -eq $SemanticModelName }
        }
        catch {
            Write-Host "Could not retrieve existing models (may be expected for new workspace): $($_.Exception.Message)" -ForegroundColor Yellow
            $existingModel = $null
        }

        # Read model definition
        if (-not (Test-Path $ModelDefinitionPath)) {
            throw "Model definition file not found: $ModelDefinitionPath"
        }
        
        $modelContent = Get-Content -Raw -Path $ModelDefinitionPath -Encoding UTF8
        
        if ($existingModel) {
            Write-Host "Semantic model '$SemanticModelName' exists. Updating..." -ForegroundColor Yellow
            $semanticModelId = $existingModel.id

            # Update existing model
            $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$semanticModelId/import"
            try {
                $updateResponse = Invoke-RestMethod -Uri $updateUrl -Headers $headers -Method Post -Body $modelContent
                Write-Host "✓ Model updated successfully" -ForegroundColor Green
            }
            catch {
                Write-Host "Update failed, attempting to create new model..." -ForegroundColor Yellow
                throw
            }
        }
        else {
            Write-Host "Creating new semantic model '$SemanticModelName'..." -ForegroundColor Yellow
            
            # Create new model using import
            $createUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/import"
            
            # Prepare the body for import
            $importBody = @{
                displayName = $SemanticModelName
                definition = @{
                    parts = @(
                        @{
                            path = "model.bim"
                            payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modelContent))
                            payloadType = "InlineBase64"
                        }
                    )
                }
            } | ConvertTo-Json -Depth 10

            $createResponse = Invoke-RestMethod -Uri $createUrl -Headers $headers -Method Post -Body $importBody
            $semanticModelId = $createResponse.id
            Write-Host "✓ Model created successfully with ID: $semanticModelId" -ForegroundColor Green
        }

        return @{
            Success = $true
            Error   = $null
            Warning = $null
            ModelId = $semanticModelId
        }
    }
    catch {
        Write-Host "✗ Failed to deploy semantic model: $($_.Exception.Message)" -ForegroundColor Red
        return @{
            Success = $false
            Error   = $_.Exception.Message
            Warning = $null
            ModelId = $null
        }
    }
}

function Deploy-PBIReport {
    param(
        [Parameter(Mandatory=$true)][string]$WorkspaceId,
        [Parameter(Mandatory=$true)][string]$ReportName,
        [Parameter(Mandatory=$true)][string]$ReportPath,
        [Parameter(Mandatory=$true)][string]$SemanticModelId,
        [Parameter(Mandatory=$true)][string]$AccessToken
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken"; "Content-Type" = "application/json" }

    try {
        Write-Host "Deploying report: $ReportName" -ForegroundColor Cyan

        if (-not $SemanticModelId) {
            throw "No SemanticModelId provided for report deployment."
        }

        # Check if report exists
        $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
        
        try {
            $existingReports = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
            $existingReport = $existingReports.value | Where-Object { $_.displayName -eq $ReportName -or $_.name -eq $ReportName }
        }
        catch {
            Write-Host "Could not retrieve existing reports: $($_.Exception.Message)" -ForegroundColor Yellow
            $existingReport = $null
        }

        # For PBIP reports, we need to handle the entire folder structure
        $reportFolderPath = Split-Path $ReportPath -Parent
        $pbipFilePath = Join-Path $reportFolderPath "$ReportName.pbip"
        
        if (-not (Test-Path $pbipFilePath)) {
            # Try to find the .pbip file
            $pbipFiles = Get-ChildItem -Path $reportFolderPath -Filter "*.pbip" -Recurse
            if ($pbipFiles.Count -gt 0) {
                $pbipFilePath = $pbipFiles[0].FullName
                Write-Host "Found PBIP file: $pbipFilePath" -ForegroundColor Yellow
            } else {
                throw "No .pbip file found in $reportFolderPath"
            }
        }

        if ($existingReport) {
            Write-Host "Report '$ReportName' exists. Updating..." -ForegroundColor Yellow
            $reportId = $existingReport.id

            # For updates, we may need to use the import API with replacement
            $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$reportId/import"
            
            # Create import body
            $importBody = @{
                displayName = $ReportName
                type = "PBIP"
                datasetDisplayName = $SemanticModelId
            } | ConvertTo-Json

            try {
                $updateResponse = Invoke-RestMethod -Uri $updateUrl -Headers $headers -Method Post -Body $importBody
                Write-Host "✓ Report updated successfully" -ForegroundColor Green
                return $reportId
            }
            catch {
                Write-Host "Update failed: $($_.Exception.Message)" -ForegroundColor Red
                throw
            }
        }
        else {
            Write-Host "Creating new report '$ReportName'..." -ForegroundColor Yellow
            
            # Create new report
            $createUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/import"
            
            $importBody = @{
                displayName = $ReportName
                type = "PBIP"
                datasetId = $SemanticModelId
            } | ConvertTo-Json

            $createResponse = Invoke-RestMethod -Uri $createUrl -Headers $headers -Method Post -Body $importBody
            $reportId = $createResponse.id
            Write-Host "✓ Report created successfully with ID: $reportId" -ForegroundColor Green
            return $reportId
        }
    }
    catch {
        Write-Host "✗ Failed to deploy report: $($_.Exception.Message)" -ForegroundColor Red
        return @{
            Success = $false
            Error   = $_.Exception.Message
        }
    }
}

function Rebind-ReportToDataset {
    param(
        [Parameter(Mandatory=$true)][string]$AccessToken,
        [Parameter(Mandatory=$true)][string]$WorkspaceId,
        [Parameter(Mandatory=$true)][string]$ReportId,
        [Parameter(Mandatory=$true)][string]$DatasetId,
        [string]$FabricApiEndpoint = "https://api.fabric.microsoft.com/v1"
    )

    Write-Host "Rebinding report '$ReportId' to dataset '$DatasetId'..." -ForegroundColor Cyan

    $url = "$FabricApiEndpoint/workspaces/$WorkspaceId/reports/$ReportId/rebind"
    $body = @{ datasetId = $DatasetId } | ConvertTo-Json -Depth 5
    $headers = @{ Authorization = "Bearer $AccessToken"; "Content-Type" = "application/json" }

    try {
        $response = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -Body $body
        Write-Host "✓ Rebind successful." -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "✗ Failed to rebind report: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Response: $($_.Exception.Response)" -ForegroundColor Red
        throw
    }
}

function Ensure-FabricConnection {
    param(
        [Parameter(Mandatory=$true)][string]$WorkspaceId,
        [Parameter(Mandatory=$true)][string]$Server,
        [Parameter(Mandatory=$true)][string]$Database,
        [Parameter(Mandatory=$true)][hashtable]$Headers
    )

    try {
        Write-Host "Ensuring Fabric connection for server: $Server, database: $Database" -ForegroundColor Cyan
        
        # List existing connections
        $connectionsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/connections"
        
        try {
            $connections = Invoke-RestMethod -Uri $connectionsUrl -Headers $Headers -Method Get
            
            # Look for existing connection
            $existingConnection = $connections.value | Where-Object { 
                $_.server -eq $Server -and $_.database -eq $Database 
            }
            
            if ($existingConnection) {
                Write-Host "✓ Found existing connection: $($existingConnection.id)" -ForegroundColor Green
                return $existingConnection.id
            }
        }
        catch {
            Write-Host "Could not retrieve connections: $($_.Exception.Message)" -ForegroundColor Yellow
        }

        # Create new connection if none exists
        Write-Host "Creating new Fabric connection..." -ForegroundColor Yellow
        
        $connectionBody = @{
            displayName = "Fabric_$Database"
            connectionType = "Fabric"
            server = $Server
            database = $Database
        } | ConvertTo-Json

        $newConnection = Invoke-RestMethod -Uri $connectionsUrl -Headers $Headers -Method Post -Body $connectionBody
        Write-Host "✓ Created new connection: $($newConnection.id)" -ForegroundColor Green
        return $newConnection.id
    }
    catch {
        Write-Host "✗ Failed to ensure Fabric connection: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

function Bind-Connection-ToSemanticModel {
    param(
        [Parameter(Mandatory=$true)][string]$WorkspaceId,
        [Parameter(Mandatory=$true)][string]$SemanticModelId,
        [Parameter(Mandatory=$true)][string]$ConnectionId,
        [Parameter(Mandatory=$true)][hashtable]$Headers
    )

    try {
        Write-Host "Binding connection $ConnectionId to semantic model $SemanticModelId..." -ForegroundColor Cyan
        
        $bindUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$SemanticModelId/connections/$ConnectionId/bind"
        
        Invoke-RestMethod -Uri $bindUrl -Headers $Headers -Method Post
        Write-Host "✓ Connection bound successfully" -ForegroundColor Green
    }
    catch {
        Write-Host "✗ Failed to bind connection: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

function Get-WorkspaceIdByName {
    param(
        [Parameter(Mandatory=$true)][string]$WorkspaceName,
        [Parameter(Mandatory=$true)][string]$AccessToken
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken" }
    $url = "https://api.fabric.microsoft.com/v1/workspaces"

    try {
        $resp = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
        $workspace = $resp.value | Where-Object { $_.displayName -eq $WorkspaceName }

        if (-not $workspace) {
            throw "Workspace '$WorkspaceName' not found."
        }

        return $workspace.id
    }
    catch {
        Write-Host "✗ Failed to get workspace ID: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Export functions for use in other scripts
Export-ModuleMember -Function Deploy-PBISemanticModel, Deploy-PBIReport, Rebind-ReportToDataset, Get-WorkspaceIdByName, Ensure-FabricConnection, Bind-Connection-ToSemanticModel