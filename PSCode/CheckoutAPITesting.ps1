$EndPoint = "http://iqaapi.hub.r1rcm.local/auth/v1/token"
$Method = "GET"
$ContentType = "application/json"
$clientID = "SOUR6337IQ"
$clientSecret = "VWPIKJXKL9VAQFD8+5X+UGFRZHCVFLTLW56CBWYLYJA="

$params = @{
    Uri         = $EndPoint
    Method      = $Method
    ContentType = $ContentType
    Headers     = @{ 
                    'clientId' = "$clientID"  
                    'clientSecret' = "$clientSecret" 
                    }
}

try{
$rToken = Invoke-RestMethod @params 
}
catch
{
    Write-Host "StatusCode:" $_.Exception.Response.StatusCode.value__ 
    Write-Host "StatusDescription:" $_.Exception.Response.StatusDescription    
    $_.Exception.Message
    $_.Exception.Response
}


$token = $rToken.token


$EndPoint = "http://iqaapi.hub.r1rcm.local/shared-services/v1/activities/source-hov/checkout"
$Method = "PUT"
$ContentType = "application/json"
$FacilityCode = "WPWI"
$Checkoutjson = @"
{
  "focus": {
    "type": "Account",
    "display": "CHRTY",
    "identifier": {
      "type": "AccountNumber",
      "value": "40002756267"
    }
  },
  "performer": {
    "type": "user",
    "code": "12312"
  }
}
"@
$params = @{
    Uri         = $EndPoint
    Method      = $Method
    ContentType = $ContentType 
    Headers     = @{ 
                    'facilityCode' = $FacilityCode
                    'Authorization' = "Bearer $token"
                    }
    Body        = $Checkoutjson
}

try{
$response = Invoke-RestMethod @params 



}
catch
{
    Write-Host "StatusCode:" $_.Exception.Response.StatusCode.value__ 
    Write-Host "StatusDescription:" $_.Exception.Response.StatusDescription    
    $_.Exception.Message
    $_.Exception.Response
}

$response