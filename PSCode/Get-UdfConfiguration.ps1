function Get-UdfConfiguration { 
 [CmdletBinding()]
        param (
                [string] $configpath   
              )

    $configvals = Import-CSV -Path $configpath -Header name, value


        $configlist = @{}

        foreach ($item in $configvals) 
        {
            $configlist.Add($item.name, $item.value)
        }   
    Return $configlist
    
}
try 
{
    $c  = Get-UdfConfiguration  "D:\PowershellScripts\SourceHOV_Charity_DB_Ingesti.csv"
}
catch
{
    $_.Exception.Message
}

$c.clientId
$c.clientSecret 
$c.APIGetWorkList
$c.APIBaseURL

-join ($c.APIBaseURL, $c.APITokenGeneration)
-join ($c.APIBaseURL, $c.APIGetWorkList)
-join ($c.APIBaseURL, $c.APICheckoutAccount)
-join ($c.APIBaseURL, $c.APIUpdateAccountStatus)

if ($c.DBConnectionString)
{
    "value found"
}
else
{
    "Value not found"
}


$p1 = "http://iqaapi.hub.r1rcm.local"
$p2 = "/auth/v1/token"


-join ($p1, $p2)

