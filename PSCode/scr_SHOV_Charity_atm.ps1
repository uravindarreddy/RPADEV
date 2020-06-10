<#    
.SYNOPSIS       
   Powershell script for Charity Process with Stagecode implementation
.DESCRIPTION
   Charity Process with StageCode implementation

.PARAMETER ConfigFilePath
   File Path Location to the configuration file in which all the parameters required for the execution of this script are configured

.EXAMPLE       
   Powershell.exe "D:\PowershellScripts\scr_SHOV_Charity_atm.ps1" -ConfigFilePath "D:\PowershellScripts\SourceHOVISConfig_Powershell.csv" 
   .\scr_SHOV_Charity_atm.ps1 -ConfigFilePath "D:\PowershellScripts\SourceHOVISConfig_Powershell.csv" 
#>

[CmdletBinding()]
param (
    [Parameter (Mandatory = $true, Position = 0)] [string] $ConfigFilePath
)
#region Function definitions
Function Get-UdfConfiguration { 
    [CmdletBinding()]
    param (
        [string] $configpath   
    )
    $configvals = Import-CSV -Path $configpath -Header name, value;

    $configlist = @{ };

    foreach ($item in $configvals) {
        $configlist.Add($item.name, $item.value)
    }   
    Return $configlist    
}
Function Write-Log {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory = $False)]
        [ValidateSet("INFO", "WARN", "ERROR", "FATAL", "DEBUG", "EXECUTION")]
        [String]
        $Level = "INFO",

        [Parameter(Mandatory = $True)]
        [string]
        $Message,

        [Parameter(Mandatory = $False)]
        [string]
        $logfile
    )

    $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss")
    #$Line = "$Level,$Stamp,$Message"    
    $Content = [PSCustomObject]@{
        "Log Level" = $Level 
        Timestamp   = $Stamp
        CurrentTask = $PSCommandPath
        Message     = $Message 
    }
    If ($null -ne $logfile) {
        try {
            Export-Csv -InputObject $Content -Path $logfile -NoTypeInformation -Append;
            
        }
        catch {
            Write-Output $_.Exception.Message            
        }
    }
    Else {
        Write-Output $Content
    }
} 

Function Invoke-UdfStoredProcedure { 
    [CmdletBinding()]
    param (
        [string] $sqlconnstring          , # Connection string
        [string] $sqlspname              , # SQL Query
        $parameterset                        # Parameter properties
    )
         
    $sqlDataAdapterError = $null
    try {
        $conn = new-object System.Data.SqlClient.SqlConnection($sqlconnstring);  

  
        $command = new-object system.data.sqlclient.Sqlcommand($sqlspname, $conn)

        $command.CommandType = [System.Data.CommandType]'StoredProcedure'; 

        foreach ($parm in $parameterset) {
            if ($parm.Direction -eq 'Input') {
                [void]$command.Parameters.AddWithValue($parm.Name, $parm.Value); 
            }
        }

        [void] $conn.Open()
  
        $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
        $dataset = New-Object System.Data.DataSet
  
        [void] $adapter.Fill($dataset)
 
    }
    catch {
        $sqlDataAdapterError = $_
        $dataset = $null
        
    }
    finally {
        $conn.Close()  
    }

    [PSCustomObject] @{
        DataSet = $dataSet
        Errors  = $sqlDataAdapterError
        Success = if ($null -eq $sqlDataAdapterError) { $true } else { $false }
    }
 
}

Function Add-UdfParameter { 
    [CmdletBinding()]
    param (
        [string] $name                    , # Parameter name from stored procedure, i.e. @myparm
        [string] $direction               , # Input or Output or InputOutput
        [string] $value                   , # parameter value
        [string] $datatype                , # db data type, i.e. string, int64, etc.
        [int]    $size                        # length
    )

    $parm = New-Object System.Object
    $parm | Add-Member -MemberType NoteProperty -Name "Name" -Value "$name"
    $parm | Add-Member -MemberType NoteProperty -Name "Direction" -Value "$direction"
    $parm | Add-Member -MemberType NoteProperty -Name "Value" -Value "$value"
    $parm | Add-Member -MemberType NoteProperty -Name "Datatype" -Value "$datatype"
    $parm | Add-Member -MemberType NoteProperty -Name "Size" -Value "$size"

    Write-Output $parm
    
}

Function Invoke-udfExecUpdateStageCode {
    [CmdletBinding()]
    param (
        [string] $connstring          , # Connection string
        [string] $spName              , # SQL Query
        [string] $pReqID              , # SP Parameter @ReqID
        [int]    $pStagecode            # SP Parameter @StageCode                            
                 
    )
    $parmset = @()   # Create a collection object.
   
    # Add the parameters we need to use...
    $parmset += (Add-UdfParameter "@ReqID" "Input" "$pReqID" "string" -1)
    $parmset += (Add-UdfParameter "@StageCode" "Input" "$pStagecode" "int32" 0)
   
    $spExecParams = @{
        sqlconnstring = $connstring
        sqlspname     = $spname
        parameterset  = $parmset
    }
    Invoke-UdfStoredProcedure @spExecParams
}

Function Invoke-udfExecUpdateStatusCode {
    [CmdletBinding()]
    param (
        [string] $connstring          , # Connection string
        [string] $spName              , # SQL Query
        [string] $pReqID,
        [int]    $pStatuscode
              
    )
    $parmset = @()   # Create a collection object.

    # Add the parameters we need to use...
    $parmset += (Add-UdfParameter "@ReqID" "Input" "$pReqID" "string" -1)
    $parmset += (Add-UdfParameter "@StatusCode" "Input" "$pStatuscode" "Byte" 0)

    $spExecParams = @{
        sqlconnstring = $connstring
        sqlspname     = $spname
        parameterset  = $parmset
    }
    Invoke-UdfStoredProcedure @spExecParams
}

Function Invoke-udfExecDTOReporting {
    [CmdletBinding()]
    param (
        [string] $connstring          , # Connection string
        [string] $spName              , # SQL Query
        [string] $Rptjson
              
    )
    $parmset = @()   # Create a collection object.

    # Add the parameters we need to use...
    $parmset += (Add-UdfParameter "@RptJson" "Input" "$Rptjson" "string" -1)
    $spExecParams = @{
        sqlconnstring = $connstring
        sqlspname     = $spname
        parameterset  = $parmset
    }
    Invoke-UdfStoredProcedure @spExecParams;
}

Function Invoke-udfDTOReporting {
    [CmdletBinding()]
    param (
        [string] $rptconnstring          , # Connection string
        [string] $rptspname              , # SQL Query
        $DTOReportData                        # Parameter properties
    )

    $DTOReportJson = ConvertTo-Json -InputObject $DTOReportData

    Invoke-udfExecDTOReporting -connstring $rptconnstring -spname $rptspname -Rptjson $DTOReportJson;

}

Function Test-SQLConnection {    
    [OutputType([bool])]
    Param
    (
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        $ConnectionString
    )
    $ErrorMessage = $null
    try {
        $sqlConnection = New-Object System.Data.SqlClient.SqlConnection $ConnectionString;
        $sqlConnection.Open();        
    }
    catch {
        $ErrorMessage = $_ 
        
    }
    finally {
        $sqlConnection.Close();       
    }

    [PSCustomObject] @{
        Errors  = $ErrorMessage
        Success = if ($null -eq $ErrorMessage) { $true } else { $false }
    }
}

Function Add-udfDatFile {
    [CmdletBinding()]
    param (
        $DatData          , # Dat File Data
        [string] $DatFileName       # Dat File Name
    )
    $DatError = $null
    try {
        ConvertTo-Csv -InputObject $DatData -NoTypeInformation | 
        Select-Object -Skip 1 | Out-File -FilePath $DatFileName
    }
    catch {
        $DatError = $_
    }

    [PSCustomObject] @{
        Errors  = $DatError
        Success = if ($null -eq $DatError) { $true } else { $false }
    }
}

Function Copy-UdfFile {
    [CmdletBinding(SupportsShouldProcess = $true)]
    param (
        [string[]]$sourcepathandfile,
        [string]$targetpathandfile,
        [switch]$overwrite 
    )

    $ErrorActionPreference = "Stop"
    $CopyError = $null 
    try {
        If ($overwrite = $true) {
            Copy-Item -PATH $sourcepathandfile -DESTINATION $targetpathandfile -Force -Confirm:$false
        }
        else {
            Copy-Item -PATH $sourcepathandfile -DESTINATION $targetpathandfile -Confirm:$false
        }
    }
    catch {
        $CopyError = $_
    }
    [PSCustomObject] @{
        Errors  = $CopyError
        Success = if ($null -eq $CopyError) { $true } else { $false }
    }
}
#endregion Function definitions

#region Reading Config file
if ( Test-Path -Path $ConfigFilePath -PathType Leaf) {
    try {
        $config = Get-UdfConfiguration -configpath $ConfigFilePath -ErrorAction Stop
        Write-Output "Reading config file completed"
    }
    catch {
        Write-Output "Error while reading the config file"
        Write-Output $_.Exception.Message
        Write-Output "Bot Execution is stopped."
        Exit
    }
}
else {
    Write-Output "Config File does not exist. Hence can't continue with Bot Execution."
    Exit
}
#endregion Reading Config file

#region Variable Declaration
[string] $PSDBConnectionString = $config.PSDBConnectionString
[string] $ProcessLogFilePath = $config.ProcessLogFilePath
[string] $WorkListSP = $config.WorkListSP
[string] $StatusCodeUpdateSP = $config.StatusCodeUpdateSP
[string] $StageCodeUpdateSP = $config.StageCodeUpdateSP
[string] $DatFolderPath = $config.DatFolderPath
[string] $SharePointFolderName = $config.SharePointFolderName
[string] $ProcessName = $config.ProcessName
[string] $ShippingMethod = $config.ShippingMethod
[string] $CharityTemplateLocation = $config.CharityTemplateLocation
[string] $DTOReportingSP = $config.DTOReportingSP
[bool]$isErrorExit = $false # Variable to determine whether to exit the execution or not

#endregion Variable Declaration

#region Logfile Initialization
if ($null -ne $config.ProcessLogFilePath) {
    if ( -not ( Test-Path -Path $config.ProcessLogFilePath -PathType Container) ) {
        Write-Output "Log file folder location is not accessible or does not exists."
        Write-Output "Bot Execution is stopped."
        Exit
    }
}
else {
    Write-Output "Log file folder location is blank."
    Write-Output "Bot Execution is stopped."    
    Exit
}

$ProcessLogFilePath = Join-Path $config.ProcessLogFilePath (Get-Date).ToString('MM.dd.yyyy')
if ( -not (Test-Path -Path $ProcessLogFilePath -PathType Container) ) {
    New-Item -ItemType "directory" -Path $ProcessLogFilePath | Out-Null    
}
# Log File Name convention BotName(machineName)_ProcessName.csv
$LogFileName = $env:COMPUTERNAME + "_" + $ProcessName + ".csv"
$LogFileName = Join-Path $ProcessLogFilePath $LogFileName
#endregion Logfile Initialization

#region Config values validation
if ($PSDBConnectionString) {
    $DBTestConnection = Test-SQLConnection $PSDBConnectionString
    if ($DBTestConnection.Success -eq $false) {
        Write-Log -Level FATAL -Message $DBTestConnection.Errors.Exception.Message -logfile $LogFileName -ErrorAction Stop
        $isErrorExit = $true
    }

}
else {        
    Write-Log -Level FATAL -Message "Database connectionstring is not provided." -logfile $LogFileName -ErrorAction Stop
    #Write-Output "Bot Execution is stopped."
    $isErrorExit = $true 
}

if ($null -eq $DatFolderPath) {
    Write-Log -Level FATAL -Message "Dat File location is blank." -logfile $LogFileName    
    $isErrorExit = $true
}
elseif (!(Test-Path -Path $DatFolderPath -PathType Container)) {
    $isErrorExit = $true
    Write-Log -Level FATAL -Message "Dat File location path mentioned does not exist." -logfile $LogFileName    
}

if ($null -eq $CharityTemplateLocation) {
    Write-Log -Level FATAL -Message "Charity Templates Location is blank." -logfile $LogFileName    
    $isErrorExit = $true
}
elseif (!(Test-Path -Path $CharityTemplateLocation -PathType Container)) {
    $isErrorExit = $true
    Write-Log -Level FATAL -Message "File location path mentioned for Charity templates does not exist." -logfile $LogFileName    
}

if ($null -eq $ShippingMethod) {
    $ShippingMethod = "First Class"
    Write-Log -Level FATAL -Message "Shipping Method not mentioned." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($null -eq $WorkListSP) {
    Write-Log -Level FATAL -Message "WorklistSP is blank. This sp is used to pull the worklist items." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($null -eq $DTOReportingSP) {
    Write-Log -Level FATAL -Message "DTOReportingSP is blank. This sp is used for DTO Reporting." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($null -eq $StatusCodeUpdateSP) {
    Write-Log -Level FATAL -Message "StatusCodeUpdateSP is blank." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($null -eq $StageCodeUpdateSP) {
    Write-Log -Level FATAL -Message "StageCodeUpdateSP is blank." -logfile $LogFileName    
    $isErrorExit = $true
}

if (!(Test-Path -Path $SharePointFolderName -PathType Container)) {    
    Write-Log -Level FATAL -Message "SharePoint drive is not accessible or not exists." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($isErrorExit) {
    Write-Host "Bot Execution is stopped."
    Exit    
}

#endregion Config values validation

Write-Log -Level EXECUTION -Message "Bot execution for Charity process has started" -logfile $LogFileName
# Starting an infinite loop for looping through all the worklist items 
# This infinite loop is added because the worklist sp fetches one worklist item at a time
# This is by design so that multiple instances can work on different worklist items simultaneously
# although in complete isolation so that no 2 bots or instances touches the same pending worklist item
while ($true) {

    Write-Log -Level INFO -Message "Getting Pending WorkList items to be processed." -logfile $LogFileName    
    #region Calling Worklist Proc to get pending worklist items
    <#
        This proc will give one worklist item at a time 
        so that multiple bot instances can work on 
        other worklist items simultaneosly
    #>
    $WorklistSPExecParams = @{
        sqlconnstring = $PSDBConnectionString
        sqlspname     = $WorkListSP
    }
    $resultset = Invoke-UdfStoredProcedure @WorklistSPExecParams;
    #endregion Calling Worklist Proc to get pending worklist items
    if ($resultset.Success) {

        if ($resultset.DataSet.Tables[0].Rows.Count -gt 0) {
            $StartTime = (Get-date -Format "yyyy-MM-dd HH:mm:ss:fff")
            Write-Log -Level INFO -Message "Pending WorkListItems found and processing started." -logfile $LogFileName
            # iterating through each row of the sp result set
            # one worklist item may consists of mutliple database rows
            foreach ($row in $resultset.DataSet.Tables[0].Rows) {
                
                               
                # DTO Reporting details
                $ReportDet = [PSCustomObject]@{
                    UserID            = $env:USERNAME
                    BotName           = $env:COMPUTERNAME
                    FacilityCode      = $row.FacilityCode
                    AccountNumber     = $row.AccountNumber
                    ProcessName       = $ProcessName
                    ProcessStatus     = $null
                    LogFilePath       = $LogFileName
                    StartProcess      = $StartTime
                    StatusDescription = $null
                    RequestType       = $row.RequestType
                    MRN               = $row.MRN
                }
                # Creating a folder for each worklist item 
                #ReqID is the identity column coming from worklistsp resultset
                $FolderName = Join-Path $DatFolderPath $row.ReqID.Tostring();
                if ( -not ( Test-Path $FolderName -PathType Container) ) {
                    New-Item -ItemType "directory" -Path $FolderName | Out-Null
                }
                # File Naming Convention
                # File Name = PatientName_Account#_MRN
                # Dat file Name
                $FName = $row.PatientName.ToString() + "_" + $row.AccountNumber.ToString() + "_" + $row.MRN.ToString() + ".DAT"
                $DATFName = Join-Path $FolderName $FName
                           
                
               
                #region Creating DAT File
                # Stage Code 1 = Creating Dat File
                if ($row.StageCode -eq 0) {

                    $CharityTemplatFile = Join-Path -Path $CharityTemplateLocation -ChildPath $row.DocFileName
                    $CharityTemplatFile = $CharityTemplatFile  + ".pdf"

                    if (Test-Path -Path $CharityTemplatFile -PathType Leaf) {
                        Write-Log -Level INFO -Message ("Creating Dat File for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName
                        $DATFileData = [PSCustomObject] @{
                            ReqDate        = $row.ReqDate.ToString('MM/dd/yyyy')
                            # Topmost accountnumber is fetched when there are multiple account numbers against an MRN & Facility Code group
                            AccountNumber  = $row.AccountNumber
                            FacilityCode   = $row.FacilityCode
                            RequestType    = $row.RequestType
                            PatientName    = $row.PatientName
                            AddressLine1   = $row.AddressLine1
                            AddressLine2   = $row.AddressLine2
                            AddressCity    = $row.AddressCity
                            AddressState   = $row.AddressState
                            ZipCode        = $row.ZipCode
                            DocFileName    = $row.DocFileName
                            NumOfPages     = $row.NumOfPages
                            ShippingMethod = $ShippingMethod 
                            UserName       = $row.UserName
                        }
                        $DatFileStatus = Add-udfDatFile -DatData $DATFileData -DatFileName $DATFName;

                        if ($DatFileStatus.Success -eq $true) {
                            $spParams = @{
                                connstring = $PSDBConnectionString
                                spname     = $StageCodeUpdateSP
                                pReqID     = $row.ReqID.ToString() 
                                pStagecode = 1
                            }                            
                            $StageCodeUpdate = Invoke-udfExecUpdateStageCode @spParams;
                            if ($StageCodeUpdate.Success -eq $true) {
                                Write-Log -Level INFO -Message ("Dat File is created for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName        
                            }
                            else {
                                Write-Log -Level ERROR -Message ("Error occured while updating the stagecode for the Request ID: " + $row.ReqID.ToString()) -logfile $LogFileName    
                                Write-Log -Level ERROR -Message $StageCodeUpdate.Errors.Exception.Message   -logfile $LogFileName    
                            
                                $ReportDet.ProcessStatus = "Fail"
                                $ReportDet.StatusDescription = $StageCodeUpdate.Errors.Exception.Message;

                                $DTOspExecParams = @{
                                    rptconnstring = $PSDBConnectionString
                                    rptspname     = $DTOReportingSP
                                    DTOReportData = $ReportDet
                                }
                                $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                                if ($DTORptDet.Success -eq $false) {
                                    Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                    Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                                }                            
                                Break                            
                            }
                        
                        }
                        else {
                            Write-Log -Level ERROR -Message ("Error occured while creating Dat File for the Request ID: " + $row.ReqID.ToString()) -logfile $LogFileName;
                            Write-Log -Level ERROR -Message $DatFileStatus.Errors.Exception.Message -logfile $LogFileName;

                            $ReportDet.ProcessStatus = "Fail";
                            $ReportDet.StatusDescription = $DatFileStatus.Errors.Exception.Message;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false) {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                        
                            Break                        
                        }
                    }
                    else {
                        Write-Log -Level ERROR -Message ("Charity template file does not exists on the charity template file location mentioned for the Request ID: " + $row.ReqID.ToString()) -logfile $LogFileName;
                        $ReportDet.ProcessStatus = "Fail";
                        $ReportDet.StatusDescription = "Charity template file does not exists on the charity template file location mentioned.";

                        $DTOspExecParams = @{
                            rptconnstring = $PSDBConnectionString
                            rptspname     = $DTOReportingSP
                            DTOReportData = $ReportDet
                        }
                        $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                        if ($DTORptDet.Success -eq $false) {
                            Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                            Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                        }                    
                        Break
                    }
                }
                #endregion Creating DAT File

                #region Upload to SharePoint    
                
                if ($row.StageCode -le 1) {
                    if ($row.StageCode.Tostring() -eq "1") {
                        # Fetch the DAT file name if it is already created in the previous run
                        $F = Get-ChildItem -Path $FolderName -Filter "*.DAT" | Select-Object -Property FullName
                        $DATFName = $F.FullName
                    }

                    Write-Log -Level INFO -Message ("Starting the Execution of the Upload To Sharepoint Bot for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName;
                    
                    $UploadToSharePointParams = @{
                        sourcepathandfile = $DATFName, $CharityTemplatFile
                        targetpathandfile = $SharePointFolderName
                        overwrite         = $true
                    }

                    $SharePtUploadStatus = Copy-UdfFile @UploadToSharePointParams;

                    if ($SharePtUploadStatus.Success -eq $true) {
                        Write-Log -Level INFO -Message ("Files uploaded to sharepoint successfully for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName
                        Write-Log -Level INFO -Message ("Updating the status as Completed for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName

                        $StatuCodeExecParams = @{
                            connstring  = $PSDBConnectionString
                            spName      = $StatusCodeUpdateSP
                            pReqID      = $row.ReqID.ToString()
                            pStatuscode = 4

                        }                      
                        $StatuscodeDet = Invoke-udfExecUpdateStatusCode @StatuCodeExecParams;

                        if ($StatuscodeDet.Success -eq $true) {
                            Write-Log -Level INFO -Message ("Status updated as Completed for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName
                            $ReportDet.ProcessStatus = "Pass"
                            $ReportDet.StatusDescription = $null;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false) {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                            
                                
                        }
                        else {
                            Write-Log -Level ERROR -Message ("Error occurred while updating the status as Completed for the Request ID: " + $row.ReqID.ToString())  -logfile $LogFileName
                            Write-Log -Level ERROR -Message $StatuscodeDet.Errors.Exception.Message  -logfile $LogFileName

                            $ReportDet.ProcessStatus = "Fail"
                            $ReportDet.StatusDescription = $StatuscodeDet.Errors.Exception.Message;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false) {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                            
                            Break
                        }

                    }
                    else {
                        Write-Log -Level ERROR -Message "Error occurred while uploading files to sharepoint." -logfile $LogFileName;
                        Write-Log -Level ERROR -Message $SharePtUploadStatus.Errors.Exception.Message -logfile $LogFileName;
                        
                        $ReportDet.ProcessStatus = "Fail"
                        $ReportDet.StatusDescription = $SharePtUploadStatus.Errors.Exception.Message;

                        $DTOspExecParams = @{
                            rptconnstring = $PSDBConnectionString
                            rptspname     = $DTOReportingSP
                            DTOReportData = $ReportDet
                        }
                        $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                        if ($DTORptDet.Success -eq $false) {
                            Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                            Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                        }                        
                        Break
                    }
                }
                #endregion Upload to SharePoint

                #region cleanup
                #Deleting the folder created during the start of the worklist id processing
                Remove-Item $FolderName -Recurse -Force -Confirm:$false -ErrorAction Stop | Out-Null;
                #endregion

                
            }
        }
        else {

            Write-Output ('NO Requests To be Processed')       
            Write-Log -Level INFO -Message "No Pending WorkList items to be processed. Hence stoppig the bot execution." -logfile $LogFileName 
            break 
        }

    }
    else {
        Write-Log -Level FATAL -Message "Error occured while getting pending work list items." -logfile $LogFileName -ErrorAction Stop
        Write-Log -Level ERROR -Message $resultset.Errors.Exception.Message -logfile $LogFileName -ErrorAction Stop
        Break
    }
}
Write-Log -Level EXECUTION -Message "End of the Bot Execution." -logfile $LogFileName
Write-Output ('End of the Powershell Execution.')
