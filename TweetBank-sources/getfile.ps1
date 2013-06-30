$client = new-object System.Net.WebClient
$client.DownloadFile( '', "." )