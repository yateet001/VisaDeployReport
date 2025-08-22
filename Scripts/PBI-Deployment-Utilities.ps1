<#
.SYNOPSIS
  Power BI Deployment Utilities
  Handles deployment of semantic models and reports into Power BI workspaces.
#>

function Get-PBIAccessToken {
    param(
        [string]$TenantId,
        [string]$ClientId,
        [string]$ClientSecret,
        [string]$Scope = "https://analysis.windows.net/powerbi/api/.default"
    )

    $body = @{
        grant_type    = "client_credentials"
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = $Scope
    }

    $response = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body $body
    return $response.access_token
}

function Deploy-PBISemanticModel {
    param(
        [string]$WorkspaceId,
        [string]$SemanticModelName,
        [string]$ModelDefinitionPath,
        [string]$AccessToken
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken" }

    try {
        # Check existing semantic models
        $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels"
        $existingModels = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
        $existingModel = $existingModels.value | Where-Object { $_.displayName -eq $SemanticModelName }

        if ($existingModel) {
            Write-Host "Semantic model '$SemanticModelName' already exists. Updating..."
            $semanticModelId = $existingModel.id

            $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/$semanticModelId/import"
            $body = Get-Content -Raw -Path $ModelDefinitionPath
            Invoke-RestMethod -Uri $updateUrl -Headers $headers -Method Post -Body $body -ContentType "application/json"
        }
        else {
            Write-Host "Semantic model '$SemanticModelName' not found. Creating new one..."
            $body = Get-Content -Raw -Path $ModelDefinitionPath
            $createUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/semanticModels/import"
            $resp = Invoke-RestMethod -Uri $createUrl -Headers $headers -Method Post -Body $body -ContentType "application/json"
            $semanticModelId = $resp.id
        }

        return @{
            Success = $true
            Error   = $null
            Warning = $null
            ModelId = $semanticModelId
        }
    }
    catch {
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
        [string]$WorkspaceId,
        [string]$ReportName,
        [string]$ReportPath,
        [string]$SemanticModelId,
        [string]$AccessToken
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken" }

    try {
        if (-not $SemanticModelId) {
            throw "No SemanticModelId provided for report deployment."
        }

        # Check if report exists
        $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports"
        $existingReports = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
        $existingReport = $existingReports.value | Where-Object { $_.name -eq $ReportName }

        if ($existingReport) {
            Write-Host "Report '$ReportName' already exists. Updating..."
            $reportId = $existingReport.id

            $updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/$reportId/import"
            $body = @{
                displayName = $ReportName
                datasetId   = $SemanticModelId
            } | ConvertTo-Json

            Invoke-RestMethod -Uri $updateUrl -Headers $headers -Method Post -Body $body -ContentType "application/json"
        }
        else {
            Write-Host "Report '$ReportName' not found. Creating new one..."
            $createUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/reports/import"
            $body = @{
                displayName = $ReportName
                datasetId   = $SemanticModelId
            } | ConvertTo-Json

            Invoke-RestMethod -Uri $createUrl -Headers $headers -Method Post -Body $body -ContentType "application/json"
        }

        return @{
            Success = $true
            Error   = $null
        }
    }
    catch {
        return @{
            Success = $false
            Error   = $_.Exception.Message
        }
    }
}

function Get-WorkspaceIdByName {
    param(
        [string]$WorkspaceName,
        [string]$AccessToken
    )

    $headers = @{ "Authorization" = "Bearer $AccessToken" }
    $url = "https://api.fabric.microsoft.com/v1/workspaces"

    $resp = Invoke-RestMethod -Uri $url -Headers $headers -Method Get

    $workspace = $resp.value | Where-Object { $_.displayName -eq $WorkspaceName }

    if (-not $workspace) {
        throw "Workspace '$WorkspaceName' not found."
    }

    return $workspace.id
}

Export-ModuleMember -Function Get-PBIAccessToken, Get-WorkspaceIdByName, Deploy-PBISemanticModel, Deploy-PBIReport

