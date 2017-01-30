<?php
Class Cleaner {
   public $conn;
   private $host;
   private $db;
   private $username;
   private $pass;
   private $operation;

   function __construct($host, $db, $username, $pass, $operation) {
      $this->host = $host;
      $this->db = $db;
      $this->username = $username;
      $this->pass = $pass;
      $this->operation = $operation;
      $this->db_list = array();

      $this->connect();
   }

   private function convert_to_array($dataResource) {
      $newArray = array();
      $var_type = gettype($dataResource);
      if ($var_type == "object") {
         for ($i = 0; $i < mysqli_num_rows($dataResource); $i++) {
            $data = mysqli_fetch_assoc($dataResource);
            foreach ($data as $key => $value) {
               $newArray[$i][$key] = $value;
            }
         }
      }
      return $newArray;
   }

   private function connect() {
      $this->conn = mysqli_connect($this->host, $this->username, $this->pass, $this->db);
      if($this->conn) {
        if($this->db === "information_schema") {
	   $this->getGWCDR();
           mysqli_close($this->conn);
           unset($this->conn);
	   foreach($this->db_list as $key=>$value) {
	      $schema_name = $value["SCHEMA_NAME"];
	      $gwNcdr = new Cleaner($this->host, $schema_name, $this->username, $this->pass, $this->operation);
	   }
        } else {
           if($this->operation === "addEvents") {
	      $this->addEvent();
	   } else if($this->operation === "removeEvents") {
	      $this->removeEvents();
  	   }
	}
      } else {
	print_r("Connection Failed, Exiting...\n\n");
      }
   }

   private function getGWCDR() {
      $query = "SELECT SCHEMA_NAME FROM SCHEMATA WHERE SCHEMA_NAME REGEXP '^gw[0-9]*cdr'";
      $dataResource = mysqli_query($this->conn, $query);
      $this->db_list = $this->convert_to_array($dataResource);
   }

   private function addEvent() {
      $this->removeEvents();
      $query = "CREATE EVENT `clean_local_cdr` ON SCHEDULE EVERY 1 DAY STARTS '2017-01-31 02:00:00' ON COMPLETION PRESERVE ENABLE DO CALL clean_local_cdr()";
      $data = mysqli_query($this->conn, $query);
      mysqli_close($this->conn);
   }

   private function removeEvents() {
      $query = "DROP EVENT IF EXISTS clean_local_cdr";
      $data = mysqli_query($this->conn, $query);
      if($this->operation === "removeEvents") 
	mysqli_close($this->conn);
   }
}

if(!isset($argv[1]) || !isset($argv[2]) || !isset($argv[3]) || !isset($argv[4]) || ($argv[4] !== "addEvents" && $argv[4] !== "removeEvents")) {
   print_r("Useage: #>php clean_local_dir.php {host} {user} {password} {addEvents | removeEvents}\n");
} else {
   $cleaner = new Cleaner($argv[1], "information_schema", $argv[2], $argv[3], $argv[4]);
}

?>
