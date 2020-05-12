<#    
.SYNOPSIS       
   Powershell script for Data ingestion for the SourceHOV Charity Process
.DESCRIPTION
   Data ingestion for getting workitems via APIs for the SourceHOV Charity Process

.PARAMETER ConfigFilePath
   File Path Location to the configuration file in which all the parameters required for the execution of this script are configured

.EXAMPLE       
   Powershell.exe "D:\PowershellScripts\SHOV_Charity_DBIngestion.ps1" -ConfigFilePath "D:\PowershellScripts\SourceHOVISConfig_Powershell.csv" 
   .\SHOV_Charity_DBIngestion.ps1 -ConfigFilePath "D:\PowershellScripts\SourceHOVISConfig_Powershell.csv" 
#>

 [CmdletBinding()]
        param (
              [Parameter (Mandatory = $true, Position = 0)] [string] $ConfigFilePath
          )
#region Function definitions
function Get-UdfConfiguration { 
 [CmdletBinding()]
        param (
                [string] $configpath   
              )
        $configvals = Import-CSV -Path $configpath -Header name, value;

        $configlist = @{};

        foreach ($item in $configvals) 
        {
            $configlist.Add($item.name, $item.value)
        }   
    Return $configlist    
}

Function Write-Log {
    [CmdletBinding()]
    Param(
    [Parameter(Mandatory=$False)]
    [ValidateSet("INFO","WARN","ERROR","FATAL","DEBUG","EXECUTION")]
    [String]
    $Level = "INFO",

    [Parameter(Mandatory=$True)]
    [string]
    $Message,

    [Parameter(Mandatory=$False)]
    [string]
    $logfile
    )

    $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss")
    $Content = [PSCustomObject]@{"Log Level" = $Level ; "Timestamp" = $Stamp; "CurrentTask" = $PSCommandPath; Message = $Message}
    If($logfile) {
        try
        {
            $Content | Export-Csv -Path $logfile -NoTypeInformation -Append
        }
        catch
        {
            Write-Host $_.Exception.Message            
        }
    }
    Else {
        Write-Host $Message
    }
} 

Function Test-SQLConnection{    
    [OutputType([bool])]
    Param
    (
        [Parameter(Mandatory=$true,
                    ValueFromPipelineByPropertyName=$true,
                    Position=0)]
        $ConnectionString
    )
    $ErrorMessage = $null
    try
    {
        $sqlConnection = New-Object System.Data.SqlClient.SqlConnection $ConnectionString;
        $sqlConnection.Open();        
    }
    catch
    {
        $ErrorMessage = $_ 
        
    }
    finally
    {
        $sqlConnection.Close();       
    }

    [PSCustomObject] @{
        Errors = $ErrorMessage
        Success = if ($ErrorMessage -eq $null) { $true } else { $false }
    }
}

Function Invoke-UdfSQLQuery{ 
 [CmdletBinding()]
        param (
              [string] $ConnectionString,   # ConnectionString
              [string] $sqlquery            # SQL Query
          )
    
    $sqlDataAdapterError = $null
    try
    {
      $conn = new-object System.Data.SqlClient.SqlConnection($ConnectionString);
  
      $command = new-object system.data.sqlclient.sqlcommand($sqlquery,$conn)

      [void] $conn.Open()
  
      $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
      $dataset = New-Object System.Data.DataSet
  
      [void] $adapter.Fill($dataset)

    }
  catch
  {
        $sqlDataAdapterError = $_
        $dataset = $null
  }
  finally
  {
        $conn.Close()  
  }

    [PSCustomObject] @{
        DataSet = $dataSet
        Errors = $sqlDataAdapterError
        Success = if ($sqlDataAdapterError -eq $null) { $true } else { $false }
    }
  
}

Function Invoke-UdfUpdateQuery{ 
 [CmdletBinding()]
        param (
              [string] $ConnectionString,   # ConnectionString
              [string] $sqlquery            # SQL Query
          )

    $sqlDataAdapterError = $null
    try
    {
      $conn = new-object System.Data.SqlClient.SqlConnection($ConnectionString);
  
      $cmd = new-object system.data.sqlclient.sqlcommand($sqlquery,$conn)

      $conn.Open() | Out-Null
  
      $cmd.ExecuteNonQuery()

      
    }
    catch
    {
        $sqlDataAdapterError = $_
        
    }
    finally
    {
        $conn.Close()        
    }

    [PSCustomObject] @{
        Errors = $sqlDataAdapterError
        Success = if ($sqlDataAdapterError -eq $null) { $true } else { $false }
    }
  
}
Function Get-udfToken{
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
    $rToken = Invoke-RestMethod @params 
    return $rToken.token

####### Token Generation ########
}

#endregion Function definitions

#region Reading Config file
try 
{
    $config  = Get-UdfConfiguration -configpath $ConfigFilePath -ErrorAction Stop
    Write-Host "Reading config file completed"
}
catch
{
    Write-Host "Error while reading the config file"
    Write-Host $_.Exception.Message
    Write-Host "Bot Execution is stopped."
    Exit
}
#endregion Reading Config file

[bool]$isErrorExit = $false

if ($config.ProcessLogFilePath -ne $null)
{
    if ( -not ( Test-Path -Path $config.ProcessLogFilePath -PathType Container) )
     {
        Write-Host "Log file folder location is not accessible or does not exists."
        Write-Host "Bot Execution is stopped."
        $isErrorExit = $true
       }
}
else 
{
        Write-Host "Log file folder location is blank."
        Write-Host "Bot Execution is stopped."    
        $isErrorExit = $true
}

$ProcessLogFilePath = Join-Path $config.ProcessLogFilePath (Get-Date).ToString('MM.dd.yyyy')
if ( -not (Test-Path -Path $ProcessLogFilePath -PathType Container) )
{
    New-Item -ItemType "directory" -Path $ProcessLogFilePath | Out-Null    
}

$LogFileName = $env:COMPUTERNAME + "_" + $ProcessName + ".csv"
$LogFileName = Join-Path $ProcessLogFilePath $LogFileName

#region Config values validation
if ($config.PSDBConnectionString -ne $null)
{
    $DBTestConnection = Test-SQLConnection $config.PSDBConnectionString
    if ($DBTestConnection.Success -eq $false)
    {
         Write-Log -Level ERROR -Message $DBTestConnection.Errors.Exception.Message -logfile $LogFileName -ErrorAction Stop
         $isErrorExit = $true
    }

}
else
{        
        Write-Log -Level ERROR -Message "Database connectionstring is not provided." -logfile $LogFileName -ErrorAction Stop
        $isErrorExit = $true
}

if ($config.APIBaseURL -eq $null)
{
    Write-Log -Level ERROR -Message "API Base URL is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}

if ($config.APITokenGeneration -eq $null)
{
    Write-Log -Level ERROR -Message "API Base URL is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}
if ($config.APIGetWorkList -eq $null)
{
    Write-Log -Level ERROR -Message "End point for Get Worklist API is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}
if ($config.APICheckoutAccount -eq $null)
{
    Write-Log -Level ERROR -Message "Endpoint for Checkout Account API is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}
<#
if ($config.APIUpdateAccountStatus -eq $null)
{
    Write-Log -Level ERROR -Message "Endpoint for Update Account Status API is blank." -logfile $LogFileName -ErrorAction Stop        
    Write-Host "Bot Execution is stopped."
    Exit
}
#>
if ($config.clientId -eq $null)
{
    Write-Log -Level ERROR -Message "clientId is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}
if ($config.clientSecret -eq $null)
{
    Write-Log -Level ERROR -Message "clientSecret is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}

if ($config.RequestTypes -eq $null)
{
    Write-Log -Level ERROR -Message "Request Type is blank." -logfile $LogFileName -ErrorAction Stop
    $isErrorExit = $true
}

if ($isErrorExit)
{
    Write-Host "Bot Execution is stopped."
    Exit    
}
#endregion Config values validation

#region Code for Token Generation
####### Token Generation ########
#
#$EndPoint = -join ($config.APIBaseURL, $c.APITokenGeneration)
#$Method = "GET"
#$ContentType = "application/json"
#$clientID = $config.clientID
#$clientSecret = $config.clientSecret
#
#$params = @{
#    Uri         = $EndPoint
#    Method      = $Method
#    ContentType = $ContentType
#    Headers     = @{ 
#                    'clientId' = "$clientID"  
#                    'clientSecret' = "$clientSecret" 
#                    }
#}
#
#try{
#    Write-Log -Level INFO -Message "Calling Token generation API" -logfile $LogFileName
#    $token = $null
#    $rToken = Invoke-RestMethod @params 
#    $token = $rToken.token
#    if ($token)
#    {
#        Write-Log -Level INFO -Message "Token generated successfully" -logfile $LogFileName    
#    }
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
#endregion code for Token Generation

#region Code for GetWorkList

$token = $null
Write-Log -Level INFO -Message "Calling Token generation API" -logfile $LogFileName
try
{
    $token = Get-udfToken -EndPoint (-join ($config.APIBaseURL, $c.APITokenGeneration)) -clientID $config.clientID -clientSecret $config.clientSecret -ErrorAction Stop
}
catch
{
    $token = $null
    Write-Log -Level ERROR -Message "Error during Token Generation API call." -logfile $LogFileName -ErrorAction Stop
    Write-Log -Level ERROR -Message $_.Exception.Message -logfile $LogFileName -ErrorAction Stop
    Write-Log -Level ERROR -Message $_.ErrorDetails.Message -logfile $LogFileName -ErrorAction Stop
    Write-Host "Bot Execution Stopped."
    Exit
}


foreach ($RequestType in $config.RequestTypes.Split(","))
{


    $EndPoint = -join ($config.APIBaseURL, $config.APIGetWorkList, $RequestType)
    $Method = "GET"
    $ContentType = "application/json"
    $clientID = $config.clientID
    $clientSecret = $config.clientSecret

    $params = @{
        Uri         = $EndPoint
        Method      = $Method
        ContentType = $ContentType
        Headers     = @{ 
                        'clientId' = "$clientID"  
                        'clientSecret' = "$clientSecret"                     
                        'Authorization' = "Bearer $token"
                        }
    }

    try{
        Write-Log -Level INFO -Message "Calling Get Worklist API" -logfile $LogFileName
        $WorkListjson = $null
        $response = Invoke-RestMethod @params -ErrorAction Stop    
        $WorkListjson = $response | ConvertTo-Json -Depth 10;

        if ($WorkListjson)
        {
            Write-Log -Level INFO -Message "Response received from Get Worklist API successfully" -logfile $LogFileName
        }
    }
    catch{
#        Write-Host "StatusCode:" $_.Exception.Response.StatusCode.value__ 
#        Write-Host "StatusDescription:" $_.Exception.Response.StatusDescription    
#        $_.Exception.Message

        Write-Log -Level ERROR -Message "Error during Get Worklist API call." -logfile $LogFileName -ErrorAction Stop
        if ($_.Exception.Response.StatusCode.value__ )
        {
            Write-Log -Level INFO -Message ("StatusCode:" + $_.Exception.Response.StatusCode.value__ ) -logfile $LogFileName -ErrorAction Stop
            Write-Log -Level INFO -Message ("StatusCode:" + $_.Exception.Response.StatusDescription) -logfile $LogFileName -ErrorAction Stop
        }
        Write-Log -Level ERROR -Message $_.Exception.Message -logfile $LogFileName -ErrorAction Stop
        Write-Log -Level ERROR -Message $_.ErrorDetails.Message -logfile $LogFileName -ErrorAction Stop
    
    }

             [string]$InsertWorkListSP = -JOIN ("EXEC ", $config.InsertWorkListSP, "@json = ", $WorkListjson )
             Invoke-UdfUpdateQuery -ConnectionString $config.PSDBConnectionString -sqlquery $InsertWorkListSP | Out-Null
}
#endregion Code for GetWorkList

#region Checkout Account
Write-Log -Level INFO -Message "Calling Token generation API" -logfile $LogFileName
try
{
    $token = Get-udfToken -EndPoint (-join ($config.APIBaseURL, $c.APITokenGeneration)) -clientID $config.clientID -clientSecret $config.clientSecret -ErrorAction Stop
}
catch
{
    $token = $null
    Write-Log -Level ERROR -Message "Error during Token Generation API call." -logfile $LogFileName -ErrorAction Stop
    Write-Log -Level ERROR -Message $_.Exception.Message -logfile $LogFileName -ErrorAction Stop
    Write-Log -Level ERROR -Message $_.ErrorDetails.Message -logfile $LogFileName -ErrorAction Stop
    Write-Host "Bot Execution Stopped."
    Exit
}


    [string]$GetWorkListSP = -JOIN ("EXEC ", $config.GetWorkListSP, "@json = ", $WorkListjson )
    $WorkListData = Invoke-UdfSQLQuery -ConnectionString $config.PSDBConnectionString -sqlquery $GetWorkListSP; 

    if($WorkListData.Success -eq $true)
    {

        if ($WorkListData.DataSet.Tables[0].Rows.Count -gt 0)
        {
            foreach ($WorkListItem in $WorkListData.DataSet.Tables[0].Rows)
            {
            $AccountNo = $WorkListItem.AccountNo
            $reqType = $WorkListItem.RequestType
                $jsonbody = @"
{
  "focus": {
    "type": "Account",
    "display": "$reqType",
    "identifier": {
      "type": "AccountNumber",
      "value": "$AccountNo"
    }
  },
  "performer": {
    "type": "user",
    "code": "12312"
  }
}
"@
                
                $EndPoint = -join ($config.APIBaseURL, $config.APICheckoutAccount, $RequestType)
                $Method = "PUT"
                $ContentType = "application/json"
                $clientID = $config.clientID
                $clientSecret = $config.clientSecret
                $FacilityCode = $WorkListItem.

                $params = @{
                    Uri         = $EndPoint
                    Method      = $Method
                    ContentType = $ContentType
                    Headers     = @{ 
                                    'clientId' = "$clientID"  
                                    'clientSecret' = "$clientSecret"                     
                                    'Authorization' = "Bearer $token"
                                    }
                }

                try{
                    Write-Log -Level INFO -Message "Calling Checkout Account API" -logfile $LogFileName
                    $WorkListjson = $null
                    $CheckOutResponse = Invoke-RestMethod @params -ErrorAction Stop    
                    $Checkoutjson = $CheckOutResponse | ConvertTo-Json -Depth 10;

                    if ($WorkListjson)
                    {
                        Write-Log -Level INFO -Message "Response received from Checkout Account API successfully" -logfile $LogFileName
                    }
                }
                catch{
            #        Write-Host "StatusCode:" $_.Exception.Response.StatusCode.value__ 
            #        Write-Host "StatusDescription:" $_.Exception.Response.StatusDescription    
            #        $_.Exception.Message

                    Write-Log -Level ERROR -Message "Error during Checkout Account API call." -logfile $LogFileName -ErrorAction Stop
                    Write-Log -Level ERROR -Message $_.Exception.Message -logfile $LogFileName -ErrorAction Stop
                    Write-Log -Level ERROR -Message $_.ErrorDetails.Message -logfile $LogFileName -ErrorAction Stop
                    if ($_.Exception.Response.StatusCode.value__ )
                    {
                        Write-Log -Level INFO -Message ("StatusCode:" + $_.Exception.Response.StatusCode.value__ ) -logfile $LogFileName -ErrorAction Stop
                        Write-Log -Level INFO -Message ("StatusCode:" + $_.Exception.Response.StatusDescription) -logfile $LogFileName -ErrorAction Stop
                    }
    
                }

                         [string]$CheckoutAccountSP = -JOIN ("EXEC ", $config.CheckoutAccountSP, "@AccntJSON = ", $Checkoutjson)
                         Invoke-UdfUpdateQuery -ConnectionString $config.PSDBConnectionString -sqlquery $CheckoutAccountSP | Out-Null
            }
        }
        else
        {
           # Write-Host ('No Requests To be Processed')       
            Write-Log -Level INFO -Message "No Pending accounts to be processed. All accounts have been checked out." -logfile $LogFileName             
        }
        
    }
    else
    {
        Write-Log -Level ERROR -Message $WorkListData.Errors.Exception.Message -logfile $LogFileName -ErrorAction Stop        
    }

#endregion Checkout Account
Write-Log -Level INFO -Message "Data ingestion for Charity process completed." -logfile $LogFileName
Write-Log -Level INFO -Message "End of Bot execution." -logfile $LogFileName