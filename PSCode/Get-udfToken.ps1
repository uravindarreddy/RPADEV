function Get-udfToken{
 [CmdletBinding()]
        param (
              [string] $EndPoint,
              [string] $clientID,
              [string] $clientSecret
          )
####### Token Generation ########

#$EndPoint = -join ($config.APIBaseURL, $c.APITokenGeneration)
$Method = "GET"
$ContentType = "application/json"
#$clientID = $config.clientID
#$clientSecret = $config.clientSecret

$params = @{
    Uri         = $EndPoint
    Method      = $Method
    ContentType = $ContentType
    Headers     = @{ 
                    'clientId' = "$clientID"  
                    'clientSecret' = "$clientSecret" 
                    }
}

#try{
 
    $token = $null
    $rToken = Invoke-RestMethod @params 
    $token = $rToken.token
#}
#catch
#{
##    Write-Host "StatusCode:" $_.Exception.Response.StatusCode.value__ 
##    Write-Host "StatusDescription:" $_.Exception.Response.StatusDescription    
#    
#    Write-Log -Level ERROR -Message "Error during Token Generation API call." -logfile $LogFileName -ErrorAction Stop
#    Write-Log -Level ERROR -Message $_.Exception.Message -logfile $LogFileName -ErrorAction Stop
#    if ($_.Exception.Response.StatusCode.value__ )
#    {
#        Write-Log -Level INFO -Message ("StatusCode:" + $_.Exception.Response.StatusCode.value__ ) -logfile $LogFileName -ErrorAction Stop
#        Write-Log -Level INFO -Message ("StatusCode:" + $_.Exception.Response.StatusDescription) -logfile $LogFileName -ErrorAction Stop
#    }
#}

####### Token Generation ########
}

Get-udfToken -EndPoint "http://iqaapi.hub.r1rcm.local/auth/v1/token"  -clientID "SOUR6337IQ" -clientSecret "VWPIKJXKL9VAQFD8+5X+UGFRZHCVFLTLW56CBWYLYJA="
