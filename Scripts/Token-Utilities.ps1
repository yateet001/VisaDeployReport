# ...existing code...
function Get-SPNToken {
    <#
    .SYNOPSIS
    Retrieves Azure AD token using Service Principal credentials with fallback mechanisms.
    
    .PARAMETER TenantId
    Azure AD Tenant ID.
    
    .PARAMETER ClientId
    Service Principal Client ID.
    
    .PARAMETER ClientSecret
    Service Principal Client Secret.
    
    .OUTPUTS
    Returns OAuth2 access token as string.
    #>
    param (
        [Parameter(Mandatory=$true)]
        [string]$TenantId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientSecret
    )
    
    # Define scopes to try in order of preference
    $scopes = @(
        @{
            Name = "Fabric API"
            Body = @{
                grant_type    = "client_credentials"
                client_id     = $ClientId
                client_secret = $ClientSecret
                scope         = "https://api.fabric.microsoft.com/.default"
            }
        },
        @{
            Name = "Power BI API (v2.0)"
            Body = @{
                grant_type    = "client_credentials"
                client_id     = $ClientId
                client_secret = $ClientSecret
                scope         = "https://analysis.windows.net/powerbi/api/.default"
            }
        },
        @{
            Name = "Power BI API (v1.0)"
            Body = @{
                grant_type    = "client_credentials"
                client_id     = $ClientId
                client_secret = $ClientSecret
                resource      = "https://analysis.windows.net/powerbi/api"
            }
        }
    )
    
    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $tokenUrlV1 = "https://login.microsoftonline.com/$TenantId/oauth2/token"
    
    foreach ($scope in $scopes) {
        try {
            Write-Host "Attempting to acquire $($scope.Name) access token..." -ForegroundColor Yellow
            
            $url = if ($scope.Name -eq "Power BI API (v1.0)") { $tokenUrlV1 } else { $tokenUrl }
            
            $tokenResponse = Invoke-RestMethod -Uri $url -Method Post -Body $scope.Body -ContentType "application/x-www-form-urlencoded"
            if ($tokenResponse.access_token) {
                Write-Host "Token acquired for $($scope.Name)." -ForegroundColor Green
                return $tokenResponse.access_token
            }
        }
        catch {
            Write-Host "Failed to acquire $($scope.Name) token: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    throw "Unable to acquire access token using provided credentials."
}
