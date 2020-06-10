<#    
.SYNOPSIS       
   Powershell script for Itemzed Statement Process with Stagecode implementation
.DESCRIPTION
   Itemized Statement Process with StageCode implementation

.PARAMETER ConfigFilePath
   File Path Location to the configuration file in which all the parameters required for the execution of this script are configured

.EXAMPLE       
   Powershell.exe "D:\PowershellScripts\src_SHOV_Itemized_atm.ps1" -ConfigFilePath "D:\PowershellScripts\SourceHOVISConfig_Powershell.csv" 
   .\src_SHOV_Itemized_atm.ps1 -ConfigFilePath "D:\PowershellScripts\SourceHOVISConfig_Powershell.csv" 
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

Function Invoke-udfExecGetChargesAndPaymentsData {
    [CmdletBinding()]
    param (
        [string] $connstring          , # Connection string
        [string] $spName              , # SQL Query
        [string] $pReqID
              
    )
    $parmset = @()   # Create a collection object.

    # Add the parameters we need to use...
    $parmset += (Add-UdfParameter "@ReqID" "Input" "$pReqID" "int" -1)

    $spExecParams = @{
        sqlconnstring = $connstring
        sqlspname     = $spname
        parameterset  = $parmset
    }
    Invoke-UdfStoredProcedure @spExecParams;
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

Function Write-UdfItemizedStatement {

    Param
    (
        [string]$FileType,
        [string]$PatientName,
        [datetime]$DOB,
        [string]$AccountNumber,
        [string]$FacilityName,
        $ChargesData,
        $PaymentsData,
        $InsuranceData,
        $AdjustmentData,
        $SummaryData,
        $ExcelFilePath

    )

    $ErrorMessage = $null
    [int]$PageCount = $null
    try {

        $excel = New-Object -ComObject excel.application
        $excel.visible = $false
        $workbook = $excel.Workbooks.Add()
        $ws = $workbook.Worksheets.Add()
        $xlFixedFormat = “Microsoft.Office.Interop.Excel.xlFixedFormatType” -as [type]
        $Data = $workbook.Worksheets.Item(1)
                

        $Data.Cells.Item(1, 1) = "Itemize Statement"

        $Data.Cells.Item(1, 1).Font.Size = 16
        $Data.Cells.Item(1, 1).Font.Bold = $True
        $Data.Cells.Item(1, 1).Font.Name = "Calibri"

        $range = $Data.Range("A1", "H1")
        #$range.Style = 'Title'
        $range = $Data.Range("A1", "C1")
        $range.Merge() | Out-Null
        $range.VerticalAlignment = -4160

        $Data.Cells.Font.Name = "Calibri"
        $Data.Cells.Font.Size = 16

               
        #$Data.Cells.VerticalAlignment = -4160
        #$Data.Cells.Indentlevel = 0
        #$Data.Rows.EntireRow.AutoFit()| Out-Null
        $Data.Range("A:F").Columns.AutoFit() | Out-Null
        $Data.Columns("A").ColumnWidth = 28
        $Data.Columns("B").ColumnWidth = 18 
        $Data.Columns("C").ColumnWidth = 17.5 
        $Data.Columns("D").ColumnWidth = 15
        #$Data.Columns("E").AutoFit()| Out-Null
        $Data.Columns("F").ColumnWidth = 50
        $Data.Columns("A:B").WrapText = $True
        $Data.Columns("F").WrapText = $True
        $Data.Columns("C:E").WrapText = $True


        #$Data.Cells.Borders.LineStyle = $xlContinuous
        #$Data.Cells.Borders.Color = $vbBlack
        #$Data.Cells.Borders.Weight = 2
     
                
        $Data.Name = 'Table 1'
        $Data.Cells.Item(2, 1) = 'Patient Name'
        #$range = $Data.Range("A3","A3")
        $range = $Data.Range("A2")
        #$range.Merge() | Out-Null
        $range.VerticalAlignment = -4160
        $range.HorizontalAlignment = -4131
        $range.Font.Bold = $True
        $Data.Cells.Item(3, 1) = 'DOB'
        $range = $Data.Range("A3")
        #$range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.VerticalAlignment = -4160
        $range.HorizontalAlignment = -4131
        $Data.Cells.Item(4, 1) = 'DOS'
        $range = $Data.Range("A4")
        #$range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.VerticalAlignment = -4160
        $range.HorizontalAlignment = -4131
        $Data.Cells.Item(5, 1) = 'ACC#'
        $range = $Data.Range("A5")
        #$range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.VerticalAlignment = -4160
        $range.HorizontalAlignment = -4131

        $Data.Cells.Item(6, 1) = 'Facility Name'
        $range = $Data.Range("A6")
        #$range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.VerticalAlignment = -4160
        $range.HorizontalAlignment = -4131

        $Data.Cells.Item(2, 2) = $PatientName
        $range = $Data.Range("B2", "C2")
        $range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.VerticalAlignment = -4160
        $range.HorizontalAlignment = -4131
        $Data.Cells.Item(3, 2) = $DOB
        $range = $Data.Range("B3", "C3")
        $range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.HorizontalAlignment = -4131
        $range.VerticalAlignment = -4160
        $Data.Cells.Item(4, 2) = ""
        $range = $Data.Range("B4", "C4")
        $range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.HorizontalAlignment = -4131
        $range.VerticalAlignment = -4160
        $Data.Cells.Item(5, 2).NumberFormat = "@"
        $Data.Cells.Item(5, 2) = $AccountNumber
        $range = $Data.Range("B5", "C5")
        $range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.HorizontalAlignment = -4131
        $range.VerticalAlignment = -4160    
        $Data.Cells.Item(6, 2) = $FacilityName
        $range = $Data.Range("B6", "F6")
        $range.Merge() | Out-Null
        $range.Font.Bold = $True
        $range.HorizontalAlignment = -4131
        $range.VerticalAlignment = -4160                               
                
        $usedRange = $Data.UsedRange
        $lastRow = $Data.UsedRange.rows.count + 1
                
                

        if ($ChargesData.Count -gt 0) {
            $start = $lastRow
            $Data.Cells.Item($lastRow, 1) = "Service Date"
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2) = "UB-04 Rev Code"
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 3) = "CPT code"
            $Data.Cells.Item($lastRow, 3).Font.Bold = $true
            $Data.Cells.Item($lastRow, 4) = "Number of Units"
            $Data.Cells.Item($lastRow, 4).Font.Bold = $true
            $Data.Cells.Item($lastRow, 5) = "Total Charge"
            $Data.Cells.Item($lastRow, 5).Font.Bold = $true
            $Data.Cells.Item($lastRow, 6) = "Charge Description"
            $Data.Cells.Item($lastRow, 6).Font.Bold = $true
                    

            $lastRow = $lastRow + 1
            $ChargesData | ForEach-Object {
                $Data.Cells.Item($lastRow, 1) = $_.ServiceDate
                $Data.Cells.Item($lastRow, 2).NumberFormat = "###0000;###0000"
                $Data.Cells.Item($lastRow, 2) = $_.GLCode
                $Data.Cells.Item($lastRow, 3).NumberFormat = "@"
                $Data.Cells.Item($lastRow, 3) = $_.ChargeCode
                $Data.Cells.Item($lastRow, 4) = $_.Units
                $Data.Cells.Item($lastRow, 5) = $_.ChargeAmt
                $Data.Cells.Item($lastRow, 5).NumberFormat = "$#,##0.00"
                $Data.Cells.Item($lastRow, 6) = $_.ChargeDesc
                $lastRow++
            }
            $Data.Cells.Item($lastRow, 1) = "Total"
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 5).NumberFormat = "$#,##0.00"
            $Data.Cells.Item($lastRow, 5).Font.Bold = $true
            $Data.Cells.Item($lastRow, 5) = $SummaryData.ChargeAmt.ToString()

            $FormulaRangeAddress = $("A" + "$start" + ":" + "A" + "$lastRow")
            $Data.Range("B4").Formula = "=CONCATENATE(TEXT(MIN($FormulaRangeAddress),`"m/dd/yyyy`"), `"-`", TEXT(MAX($FormulaRangeAddress),`"m/dd/yyyy`"))"
                    
            #$Data.Range($Data.Cells($start, 1), $Data.Cells($lastRow-1, 6)).Borders.LineStyle = 1

            $lastRow = $Data.UsedRange.rows.count + 1
                    
        }



        if ($PaymentsData.Count -gt 0) {
            $lastRow = $lastRow + 1
            $Data.Cells.Item($lastRow, 1) = "Patient Payments"
            $Data.Cells.Item($lastRow, 2) = $SummaryData.PatientPaidAmt.ToString()
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2).NumberFormat = "$#,##0.00"
                                        
            $lastRow = $lastRow + 1
            $start = $lastRow 

            $Data.Cells.Item($lastRow, 1) = "Date Posted"
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2) = "Amount"
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 3) = "Payment Description"
            $Data.Cells.Item($lastRow, 3).Font.Bold = $true
            $range = $Data.Range($Data.Cells($lastRow, 3), $Data.Cells($lastRow, 5))
            $range.Merge() | Out-Null
            $lastRow = $lastRow + 1
            $PaymentsData | ForEach-Object {
                $Data.Cells.Item($lastRow, 1) = $_.PostedDate
                $Data.Cells.Item($lastRow, 2) = $_.PaymentAmt
                $Data.Cells.Item($lastRow, 2).NumberFormat = "$#,##0.00"
                $Data.Cells.Item($lastRow, 3) = $_.PaymentDesc
                $range = $Data.Range($Data.Cells($lastRow, 3), $Data.Cells($lastRow, 5))
                $range.Merge() | Out-Null
                $lastRow++
            }
            #$Data.Range($Data.Cells($start, 1), $Data.Cells($lastRow-1, 3)).Borders.LineStyle = 1
            <#
                    $PaymentsData | ConvertTo-Csv -NoType -Del "`t" | Select-Object -Skip 1| Clip
                    $Range = $ws.Range("A" + $lastRow)
                    $Range.PasteSpecial($xlPasteValues) | Out-Null
                    #>

            $lastRow = $Data.UsedRange.rows.count + 1
            #$lastRow
        }
                

                
                
        if ($InsuranceData.Count -gt 0) {
            $lastRow = $lastRow + 1
            $Data.Cells.Item($lastRow, 1) = "Insurance Payments"
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2) = $SummaryData.PayerPaidAmt.ToString()
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2).NumberFormat = "$#,##0.00"
                    
            $lastRow = $lastRow + 1
            $start = $lastRow 
                    
            $Data.Cells.Item($lastRow, 1) = "Date Posted"
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2) = "Amount"
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 3) = "Payment Description"
            $Data.Cells.Item($lastRow, 3).Font.Bold = $true
            $range = $Data.Range($Data.Cells($lastRow, 3), $Data.Cells($lastRow, 5))
            $range.Merge() | Out-Null
            $lastRow = $lastRow + 1
            $InsuranceData | ForEach-Object {
                $Data.Cells.Item($lastRow, 1) = $_.PostedDate
                $Data.Cells.Item($lastRow, 2) = $_.PaymentAmt
                $Data.Cells.Item($lastRow, 2).NumberFormat = "$#,##0.00"
                $Data.Cells.Item($lastRow, 3) = $_.PaymentDesc
                $range = $Data.Range($Data.Cells($lastRow, 3), $Data.Cells($lastRow, 5))
                $range.Merge() | Out-Null
                $lastRow++
            }

            $lastRow = $Data.UsedRange.rows.count + 1              
        }



        if ($AdjustmentData.Count -gt 0) {
                    
            $lastRow = $lastRow + 1
            $Data.Cells.Item($lastRow, 1) = "Adjustments"
            $Data.Cells.Item($lastRow, 2) = $SummaryData.AdjustmentAmt.ToString()
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2).NumberFormat = "$#,##0.00"
                    
                    
            $lastRow = $lastRow + 1
            $start = $lastRow

            $Data.Cells.Item($lastRow, 1) = "Date Posted"
            $Data.Cells.Item($lastRow, 1).Font.Bold = $true
            $Data.Cells.Item($lastRow, 2) = "Amount"
            $Data.Cells.Item($lastRow, 2).Font.Bold = $true
            $Data.Cells.Item($lastRow, 3) = "Payment Description"
            $Data.Cells.Item($lastRow, 3).Font.Bold = $true
            $range = $Data.Range($Data.Cells($lastRow, 3), $Data.Cells($lastRow, 5))
            $range.Merge() | Out-Null
            $lastRow = $lastRow + 1
            $AdjustmentData | ForEach-Object {
                $Data.Cells.Item($lastRow, 1) = $_.PostedDate
                $Data.Cells.Item($lastRow, 2) = $_.AdjustmentAmt
                $Data.Cells.Item($lastRow, 2).NumberFormat = "$#,##0.00"
                $Data.Cells.Item($lastRow, 3) = $_.AdjustmentDesc
                $range = $Data.Range($Data.Cells($lastRow, 3), $Data.Cells($lastRow, 5))
                $range.Merge() | Out-Null
                $lastRow++

            }
            #$Data.Range($Data.Cells($start, 1), $Data.Cells($lastRow-1, 3)).Borders.LineStyle = 1
        }

        $usedRange = $Data.UsedRange
        $usedRange.Font.Name = "Calibri"
        $usedRange.Font.Size = 16
        $usedRange.HorizontalAlignment = -4131
        $usedRange.VerticalAlignment = -4160
        $usedRange.Cells.Indentlevel = 0
        $usedRange.Rows.EntireRow.AutoFit() | Out-Null

        #$Data.Cells.Item(1,1).Font.Size = 24
        $Data.Cells.Item(1, 1).Font.Bold = $True
        $Data.Cells.Item(1, 1).Font.Name = "Calibri"

        $workbook.Saved = $True
        $ws.PageSetup.Zoom = $false
        $ws.PageSetup.FitToPagesTall = $false
        $ws.PageSetup.FitToPagesWide = 1
        #$ws.PageSetup.PrintTitleRows = "8:8"
        if ($FileType -eq "PDF") {
            $workbook.ExportAsFixedFormat($xlFixedFormat::xlTypePDF, $ExcelFilePath) | Out-Null

            $PageCount = $ws.PageSetup.Pages.Count  
        }
        else {
            $workbook.SaveAs($ExcelFilePath)
        }

                
    }
    catch {
        $ErrorMessage = $_
        $PageCount = $null
    
    }
    finally {
        $workbook.Saved = $true
        $excel.Workbooks.Close() | Out-Null
        $excel.Quit() | Out-Null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject([System.__ComObject]$excel) | Out-Null
    }


    [PSCustomObject] @{
        NumOfPages = $PageCount
        Errors     = $ErrorMessage
        Success    = if ($null -eq $ErrorMessage) { $true } else { $false }
    }

        
}

Function Merge-UdfXlFiles {

    [CmdletBinding()]
    param (
        [string] $ExcelFolder,   
        [string] $MergeFileName
    )
    $ErrorMessage = $null
    [int]$PageCount = $null
    

    try {
    $ExcelFiles = Get-ChildItem -Path $ExcelFolder -Filter "*.xls*";

    if ($ExcelFiles) {

        $ExcelObject = New-Object -ComObject excel.application
        $ExcelObject.visible = $false
        $Workbook = $ExcelObject.Workbooks.add()
        $Worksheet = $Workbook.Sheets.Item(1)

        foreach ($ExcelFile in $ExcelFiles) {
 
            $Everyexcel = $ExcelObject.Workbooks.Open($ExcelFile.FullName)
            $Everysheet = $Everyexcel.Sheets.Item(1)
            $Everysheet.PageSetup.Zoom = $false
            $Everysheet.PageSetup.FitToPagesTall = $false
            $Everysheet.PageSetup.FitToPagesWide = 1 
            $PageCount += $Everysheet.PageSetup.Pages.Count
            $Everysheet.Copy($Worksheet)
            $Everyexcel.Close()

        }

        $Worksheet.PageSetup.Zoom = $false
        $Worksheet.PageSetup.FitToPagesTall = $false
        $Worksheet.PageSetup.FitToPagesWide = 1 
        $Workbook.ExportAsFixedFormat($xlFixedFormat::xlTypePDF, $MergeFileName) | Out-Null
        }
        else {
            throw "Mentioned folder $ExcelFolder does not have any xls files for merging.";
        }
    }
    Catch {
        $ErrorMessage = $_
        $PageCount = $null        
    }
    finally {
    if ($ExcelFiles) {
        $Workbook.Saved = $true
        $ExcelObject.Workbooks.Close() | Out-Null
        $ExcelObject.Quit() | Out-Null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject([System.__ComObject]$ExcelObject) | Out-Null
        }
    }

    [PSCustomObject] @{
        NumOfPages = $PageCount
        Errors     = $ErrorMessage
        Success    = if ($null -eq $ErrorMessage) { $true } else { $false }
    }

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
[string] $ItemizedSP = $config.ItemizedSP
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


<#
#1
$PSDBConnectionString = "Data Source=RAVINDARREDDYU\SQLEXPRESS;Integrated Security=SSPI;Initial Catalog=DTO_DB"
#2
$ProcessLogFilePath = "D:\Testing\" 
#3
$DatFolderPath = "D:\Testing\"
#4
$WorkListSP = "dbo.usp_SHOV_IS_GetWorkList"
#5
$StatusCodeUpdateSP = "dbo.usp_SHOV_IS_UpdateStatusCode"
#6
$StageCodeUpdateSP = "dbo.usp_SHOV_IS_UpdateStageCode"
#7
$SharePointFolderName = "C:\Users\ravindarreddyu\Documents\SQL Server Management Studio\HOV\IS_CSU"
#>

if ($null -eq $DatFolderPath) {
    Write-Log -Level FATAL -Message "Dat File location is blank." -logfile $LogFileName    
    $isErrorExit = $true
}
elseif (!(Test-Path -Path $DatFolderPath -PathType Container)) {
    $isErrorExit = $true
    Write-Log -Level FATAL -Message "Dat File location path mentioned does not exist." -logfile $LogFileName    
}

if ($null -eq $ShippingMethod) {
    Write-Log -Level FATAL -Message "Shipping Method not mentioned." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($null -eq $WorkListSP) {
    Write-Log -Level FATAL -Message "WorklistSP is blank. This sp is used to pull the worklist items." -logfile $LogFileName    
    $isErrorExit = $true
}

if ($null -eq $ItemizedSP) {
    Write-Log -Level FATAL -Message "ItemizedSP is blank. This sp is used to pull Charges and payments data." -logfile $LogFileName    
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

if ($null -eq $DTOReportingSP) {
    Write-Log -Level FATAL -Message "DTOReportingSP is blank." -logfile $LogFileName    
    $isErrorExit = $true
}


if (!(Test-Path -Path $SharePointFolderName -PathType Container)) {
    
    Write-Log -Level FATAL -Message "SharePoint drive is not accessible or not exists." -logfile $LogFileName;
    $isErrorExit = $true
}

if ($isErrorExit) {
    Write-Host "Bot Execution is stopped."
    Exit    
}

#endregion Config values validation

Write-Log -Level EXECUTION -Message "Bot execution for Itemized Statement process has started" -logfile $LogFileName
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
    if ($resultset.Success -eq $true) {

        if ($resultset.DataSet.Tables[0].Rows.Count -gt 0) {
            $StartTime = (Get-date -Format "yyyy-MM-dd HH:mm:ss:fff")
            [string]$WorkListStatus = "" # Variable for holding the worklist status
            [string]$WorkListStatusDesc = "" # Variable for holding the worklist Status Description
            Write-Log -Level INFO -Message "Pending WorkListItems found and processing started." -logfile $LogFileName
            # iterating through each row of the sp result set
            # one worklist item may consists of mutliple database rows
            foreach ($row in $resultset.DataSet.Tables[0].Rows) {
                [int]$NumOfPages = 0;
                
                # This variable holds the account number to displayed in the filename and 
                # under the Account Number column in DAT File
                [string] $AccNo = $row.AccountNumber.Split('_')[0].ToString()
                
                # DTO Reporting details
                $ReportDet = [PSCustomObject]@{
                    UserID            = $env:USERNAME
                    BotName           = $env:COMPUTERNAME
                    FacilityCode      = $row.FacilityCode
                    AccountNumber     = $AccNo
                    ProcessName       = $ProcessName
                    ProcessStatus     = $null
                    LogFilePath       = $LogFileName
                    StartProcess      = $StartTime
                    StatusDescription = $null
                    RequestType       = $row.RequestType
                    MRN               = $row.MRN
                }
                # Creating a folder for each worklist item 
                # in this case a worklist item consists of a group of Facilitycode and MRN
                if ((Get-Member -InputObject $row -Name MRN -MemberType Properties) -AND
                    (Get-Member -InputObject $row -Name FacilityCode -MemberType Properties) ) {

                    $FolderName = Join-Path $DatFolderPath  ($row.FacilityCode.Tostring() + $row.MRN.Tostring())
                    if ( -not ( Test-Path $FolderName -PathType Container) ) {
                        New-Item -ItemType "directory" -Path $FolderName | Out-Null
                    }
                }
                else {
                    $FolderName = $DatFolderPath
                }

                # File Naming Convention
                # File Name = PatientName_Account#_MRN
                # Dat file Name
                $FName = $row.PatientName.ToString() + "_" + $AccNo + "_" + $row.MRN.ToString() + ".DAT"
                $DATFileName = Join-Path $FolderName $FName
                
                # Itemized PDF File Name
                $PDFName = $row.PatientName.ToString() + "_" + $AccNo + "_" + $row.MRN.ToString() + ".pdf"
                $PDFFileName = Join-Path $FolderName $PDFName              
                
                #region Generate Itemized Statement PDF file                
                <#
                    Stage code 1 = First step ie Generating Itemized File
                    
                    if the database column Stage code 0 indicates 
                    that no processing done on this worklist item
                    
                    if the database column Stagecode = 1 then 
                    it means Iemized pdf is already created for this worklist
                #>
                if ($row.StageCode -eq 0) {
                    $ExcelData = $null
                    [array]$RequestIDs = $row.ReqIDs.ToString().Split(',')
                    # if ReqIDs column coming from database has multiple values separated by comma (,)
                    # it means the requests must be grouped and a single Itemized statement need to be generated
                    if ( $RequestIDs.Count -gt 1) {
                        [array]$AccountNumbers = $row.AccountNumber.ToString().Split('_')
                        [int] $i = 0
                        foreach ($ReqID in $RequestIDs) {
                            $AccountNumber = $AccountNumbers[$i]
                            Write-Log -Level INFO -Message "Starting the task of filling Itemized template file for the Request ID: $ReqID"  -logfile $LogFileName;

                            $ExcelFileName = Join-Path $FolderName ($ReqID.ToString() + ".xlsx")

                            if ( Test-Path -Path $ExcelFileName -PathType Leaf) {
                                Remove-Item -Path $ExcelFileName -Force -Confirm:$false -ErrorAction Stop | Out-Null
                            }

                            #[string] $ItemizedSP = "usp_SHOV_IS_GetChargesAndPaymentsData"
                            $ItemizedSPExecParams = @{
                                connstring = $PSDBConnectionString
                                spname     = $ItemizedSP
                                pReqID     = $ReqID
                            }
                            $ItemizedData = Invoke-udfExecGetChargesAndPaymentsData @ItemizedSPExecParams

                            if ($ItemizedData.Success -eq $true) {

                            $xlParams = @{
                                FileType       = "xlsx"
                                PatientName    = $row.PatientName.ToString()
                                DOB            = $row.DOB.ToString()
                                AccountNumber  = $AccountNumber
                                FacilityName   = $row.FacilityName.ToString()
                                ChargesData    = $ItemizedData.DataSet.Tables[0].Rows
                                PaymentsData   = $ItemizedData.DataSet.Tables[1].Rows
                                InsuranceData  = $ItemizedData.DataSet.Tables[2].Rows
                                AdjustmentData = $ItemizedData.DataSet.Tables[3].Rows
                                SummaryData    = $ItemizedData.DataSet.Tables[4].Rows
                                ExcelFilePath  = $ExcelFileName                            

                            }    
                            $ExcelData = Write-UdfItemizedStatement  @xlParams;

                            if ($ExcelData.Success -eq $true) {            
                                Write-Log -Level INFO -Message "Itemized file is created for the Request ID: $ReqID"  -logfile $LogFileName
                            }
                            else {
                                Write-Log -Level ERROR -Message "Error occured while creating Itemized template file for the Request ID: $ReqID"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message $ExcelData.Errors.Exception.Message -logfile $LogFileName 
                                $ReportDet.ProcessStatus = "Fail"
                                $ReportDet.StatusDescription = $ExcelData.Errors.Exception.Message;

                                $DTOspExecParams = @{
                                    rptconnstring = $PSDBConnectionString
                                    rptspname     = $DTOReportingSP
                                    DTOReportData = $ReportDet
                                }
                                $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                                if ($DTORptDet.Success -eq $false)
                                {
                                    Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                    Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                                }
                                Break
                            }
                            $i++
                            }
                            else {
                                Write-Log -Level ERROR -Message "Error occured while fetching the itemized details from ItemizedSP for the Request ID: $ReqID"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message $ItemizedData.Errors.Exception.Message -logfile $LogFileName 
                                $ReportDet.ProcessStatus = "Fail"
                                $ReportDet.StatusDescription = $ItemizedData.Errors.Exception.Message;

                                $DTOspExecParams = @{
                                    rptconnstring = $PSDBConnectionString
                                    rptspname     = $DTOReportingSP
                                    DTOReportData = $ReportDet
                                }
                                $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                                if ($DTORptDet.Success -eq $false)
                                {
                                    Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                    Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                                }
                                Break

                            }
                        }
                        $MergeData = Merge-UdfXlFiles -ExcelFolder $FolderName -MergeFileName $PDFFileName        

                        if ($MergeData.Success -eq $true) {
                            $NumOfPages = $MergeData.NumOfPages
                            #if merging of multiple excel files to create one file is completed
                            # call StageCode update sp to update the stagecode against this worklist as 1
                            $spParams = @{
                                connstring = $PSDBConnectionString
                                spname     = $StageCodeUpdateSP
                                pReqID     = $ReqID.ToString()  
                                pStagecode = 1
                            }                            
                            $StageCodeUpdate = Invoke-udfExecUpdateStageCode @spParams;
                            
                            if ($StageCodeUpdate.Success -eq $true) {
                                Write-Log -Level INFO -Message "Itemized file is created for the Request ID: $ReqID"  -logfile $LogFileName
                            }
                            else {
                                Write-Log -Level ERROR -Message "Error occured while updating the stagecode for the Request ID: $ReqID" -logfile $LogFileName    
                                Write-Log -Level ERROR -Message $StageCodeUpdate.Errors.Exception.Message -logfile $LogFileName;
                                
                                $ReportDet.ProcessStatus = "Fail"
                                $ReportDet.StatusDescription = $StageCodeUpdate.Errors.Exception.Message;

                                $DTOspExecParams = @{
                                    rptconnstring = $PSDBConnectionString
                                    rptspname     = $DTOReportingSP
                                    DTOReportData = $ReportDet
                                }
                                $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                                if ($DTORptDet.Success -eq $false)
                                {
                                    Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                    Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                                }                                
                                Break                                
                            }
                            
                        }
                        else {
                            Write-Log -Level ERROR -Message ("Error occured while merging multiple Itemized statement files for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                            Write-Log -Level ERROR -Message $MergeData.Errors.Exception.Message -logfile $LogFileName 
                            $WorkListStatus = "Fail"
                            $WorkListStatusDesc = $MergeData.Errors.Exception.Message;
                            Break
                        }

                    }
                    else {
                        Write-Log -Level INFO -Message ("Starting the task of filling Itemized template file for the Request ID: " + $row.ReqIDs.ToString()) -logfile $LogFileName

                        $ItemizedSPExecParams = @{
                            connstring = $PSDBConnectionString
                            spname     = $ItemizedSP
                            pReqID     = $row.ReqIDs.ToString()
                        }
                        $ItemizedData = Invoke-udfExecGetChargesAndPaymentsData @ItemizedSPExecParams;
                        if($ItemizedData.Success -eq $true) {
                        $xlParams = @{
                            FileType       = "PDF"
                            PatientName    = $row.PatientName.ToString()
                            DOB            = $row.DOB.ToString()
                            AccountNumber  = $row.AccountNumber.ToString()
                            FacilityName   = $row.FacilityName.ToString()
                            ChargesData    = $ItemizedData.DataSet.Tables[0].Rows
                            PaymentsData   = $ItemizedData.DataSet.Tables[1].Rows
                            InsuranceData  = $ItemizedData.DataSet.Tables[2].Rows
                            AdjustmentData = $ItemizedData.DataSet.Tables[3].Rows
                            SummaryData    = $ItemizedData.DataSet.Tables[4].Rows    
                            ExcelFilePath  = $PDFFileName
                        }

                        $ExcelData = Write-UdfItemizedStatement @xlParams;

                        if ($ExcelData.Success -eq $true) {
                            $NumOfPages = $ExcelData.NumOfPages
                            $spParams = @{
                                connstring = $PSDBConnectionString
                                spname     = $StageCodeUpdateSP
                                pReqID     = $row.ReqIDs.ToString()
                                pStagecode = 1
                            }                            
                            $StageCodeUpdate = Invoke-udfExecUpdateStageCode @spParams;
                            if ($StageCodeUpdate.Success -eq $true) {
                                Write-Log -Level INFO -Message "Itemized file is created for the Request ID: $ReqID"  -logfile $LogFileName
                            }
                            else {
                                Write-Log -Level ERROR -Message ("Error occured while updating the stagecode for the Request ID: " + $row.ReqIDs.ToString()) -logfile $LogFileName    
                                Write-Log -Level ERROR -Message $StageCodeUpdate.Errors.Exception.Message   -logfile $LogFileName    
                                
                                $ReportDet.ProcessStatus = "Fail"
                                $ReportDet.StatusDescription = $StageCodeUpdate.Errors.Exception.Message;

                                $DTOspExecParams = @{
                                    rptconnstring = $PSDBConnectionString
                                    rptspname     = $DTOReportingSP
                                    DTOReportData = $ReportDet
                                }
                                $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                                if ($DTORptDet.Success -eq $false)
                                {
                                    Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                    Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                                }                                
                                Break
                            }
                        }
                        else {
                            Write-Log -Level ERROR -Message ("Error occured while creating Itemized template file for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                            Write-Log -Level ERROR -Message $ExcelData.Errors.Exception.Message -logfile $LogFileName 
                                                        
                            $ReportDet.ProcessStatus = "Fail"
                            $ReportDet.StatusDescription = $ExcelData.Errors.Exception.Message;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false)
                            {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                            
                            Break
                        }
                        }
                        else {

                            Write-Log -Level ERROR -Message ("Error occured while fetching data from ItemizedSP for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                            Write-Log -Level ERROR -Message $ItemizedData.Errors.Exception.Message -logfile $LogFileName 
                                                        
                            $ReportDet.ProcessStatus = "Fail"
                            $ReportDet.StatusDescription = $ItemizedData.Errors.Exception.Message;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false)
                            {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                            
                            Break

                        }
                    }
                
                }
                #endregion Generate Itemized Statement PDF file
                
                
                #region Creating DAT File
                # Stage Code 2 = Creating Dat File
                if ($row.StageCode -le 1) {

                    Write-Log -Level INFO -Message ("Creating Dat File for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                    $DATFileData = [PSCustomObject] @{
                        ReqDate        = $row.ReqDate.ToString('MM/dd/yyyy')
                        # Topmost accountnumber is fetched when there are multiple account numbers against an MRN & Facility Code group
                        AccountNumber  = $AccNo 
                        FacilityCode   = $row.FacilityCode
                        RequestType    = $row.RequestType
                        PatientName    = $row.PatientName
                        AddressLine1   = $row.AddressLine1
                        AddressLine2   = $row.AddressLine2
                        AddressCity    = $row.AddressCity
                        AddressState   = $row.AddressState
                        ZipCode        = $row.ZipCode
                        DocFileName    = $PDFName # Generated Itemized PDF file Name
                        NumOfPages     = $NumOfPages # Extracted Number of pages when pdf file is generated
                        ShippingMethod = $ShippingMethod 
                        UserName       = $row.UserName
                    }
                    $DatFileStatus = Add-udfDatFile -DatData $DATFileData -DatFileName $DATFileName;

                    if ($DatFileStatus.Success -eq $true) {
                        $spParams = @{
                            connstring = $PSDBConnectionString
                            spname     = $StageCodeUpdateSP
                            pReqID     = $row.ReqIDs.ToString() 
                            pStagecode = 2
                        }                            
                        $StageCodeUpdate = Invoke-udfExecUpdateStageCode @spParams;
                        if ($StageCodeUpdate.Success -eq $true) {
                            Write-Log -Level INFO -Message ("Dat File is created for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName        
                        }
                        else {
                            Write-Log -Level ERROR -Message ("Error occured while updating the stagecode for the Request ID: " + $row.ReqIDs.ToString()) -logfile $LogFileName    
                            Write-Log -Level ERROR -Message $StageCodeUpdate.Errors.Exception.Message   -logfile $LogFileName    
                            
                            $ReportDet.ProcessStatus = "Fail"
                            $ReportDet.StatusDescription = $StageCodeUpdate.Errors.Exception.Message;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false)
                            {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                            
                            Break                            
                        }
                        
                    }
                    else {
                        Write-Log -Level ERROR -Message ("Error occured while creating Dat File for the Request ID: " + $row.ReqIDs.ToString()) -logfile $LogFileName;
                        Write-Log -Level ERROR -Message $DatFileStatus.Errors.Exception.Message -logfile $LogFileName;

                        $ReportDet.ProcessStatus = "Fail";
                        $ReportDet.StatusDescription = $DatFileStatus.Errors.Exception.Message;

                        $DTOspExecParams = @{
                            rptconnstring = $PSDBConnectionString
                            rptspname     = $DTOReportingSP
                            DTOReportData = $ReportDet
                        }
                        $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                        if ($DTORptDet.Success -eq $false)
                        {
                            Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                            Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                        }                        
                        Break                        
                    }
                }
                #endregion Creating DAT File

                #region Upload to SharePoint    
                #### Stage Code 3 Process starts
                if ($row.StageCode -le 3) {
                    if ($row.StageCode.Tostring() -eq "3") {
                        # Fetch the DAT file name if it is already created in the previous run
                        $F = Get-ChildItem -Path $FolderName -Filter "*.DAT" | Select-Object -Property FullName
                        $DATFileName = $F.FullName
                        # Fetch the PDF file name if it is already created in the previous run
                        $F = Get-ChildItem -Path $FolderName -Filter "*.PDF" | Select-Object -Property FullName
                        $PDFFileName = $F.FullName
                    }

                    Write-Log -Level INFO -Message ("Starting the Execution of the Upload To Sharepoint Bot for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName;
                    
                    $UploadToSharePointParams = @{
                        sourcepathandfile = $DATFileName, $PDFFileName 
                        targetpathandfile = $SharePointFolderName
                        overwrite         = $true
                    }

                    $SharePtUploadStatus = Copy-UdfFile @UploadToSharePointParams;

                    if ($SharePtUploadStatus.Success -eq $true) {
                        Write-Log -Level INFO -Message ("Files uploaded to sharepoint successfully for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                        Write-Log -Level INFO -Message ("Updating the status as Completed for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName

                        $StatuCodeExecParams = @{
                            connstring  = $PSDBConnectionString
                            spName      = $StatusCodeUpdateSP
                            pReqID      = $row.ReqIDs.ToString()
                            pStatuscode = 4

                        }                      
                        $StatuscodeDet = Invoke-udfExecUpdateStatusCode @StatuCodeExecParams;

                        if ($StatuscodeDet.Success -eq $true) {
                            Write-Log -Level INFO -Message ("Status updated as Completed for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                            $ReportDet.ProcessStatus = "Pass"
                            $ReportDet.StatusDescription = $null;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false)
                            {
                                Write-Log -Level ERROR -Message "Error during DTO Reporting"  -logfile $LogFileName
                                Write-Log -Level ERROR -Message  $DTORptDet.Errors.Exception.Message -logfile $LogFileName
                            }                            
                                
                        }
                        else {
                            Write-Log -Level ERROR -Message ("Error occurred while updating the status as Completed for the Request ID: " + $row.ReqIDs.ToString())  -logfile $LogFileName
                            Write-Log -Level ERROR -Message $StatuscodeDet.Errors.Exception.Message  -logfile $LogFileName

                            $ReportDet.ProcessStatus = "Fail"
                            $ReportDet.StatusDescription = $StatuscodeDet.Errors.Exception.Message;

                            $DTOspExecParams = @{
                                rptconnstring = $PSDBConnectionString
                                rptspname     = $DTOReportingSP
                                DTOReportData = $ReportDet
                            }
                            $DTORptDet = Invoke-udfDTOReporting @DTOspExecParams;
                            if ($DTORptDet.Success -eq $false)
                            {
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
                        if ($DTORptDet.Success -eq $false)
                        {
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
