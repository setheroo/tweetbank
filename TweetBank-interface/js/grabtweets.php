<?

//We use already made Twitter OAuth library
//https://github.com/mynetx/codebird-php
require_once ('codebird.php');

//Twitter OAuth Settings, enter your settings here:
$CONSUMER_KEY = 'TPP5N6kziVqRz1olQGwGCg';
$CONSUMER_SECRET = '4PkiztGG5fQ27XiAAJ8w5eti6se0H8xoifaAIZMh5E';
$ACCESS_TOKEN = '17905874-9eWghDNYNwAFbHRWCaDHRr5d62M1Y5DLP6agk7yDI';
$ACCESS_TOKEN_SECRET = 'NEwPdAicDKR76lGeAAKtKthuRtgI91hR2tjmi5H3M0';

//Get authenticated
Codebird::setConsumerKey($CONSUMER_KEY, $CONSUMER_SECRET);
$cb = Codebird::getInstance();
$cb->setToken($ACCESS_TOKEN, $ACCESS_TOKEN_SECRET);


//retrieve posts
$q = $_POST['q'];
$count = $_POST['count'];
$api = $_POST['api'];

//https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
//https://dev.twitter.com/docs/api/1.1/get/search/tweets
$params = array(
	'screen_name' => $q,
	'q' => $q,
	'count' => $count
);

//Make the REST call
$data = (array) $cb->$api($params);

//Output result in JSON, getting it ready for jQuery to process
echo json_encode($data);

?>