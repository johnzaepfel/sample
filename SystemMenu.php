<?php

// -------------------------------------------
// This class is used primarily inteact with the table 
// `mydaemen_systems_each`.  It is used to retrieve various aspects of 
// individual systems and generating the menu and navigation in MyDaemen 
// -------------------------------------------

namespace Daemen;

//Using these classes
use PDO, PDOStatement, PFOException;

class SystemMenu implements \JsonSerializable {
	
	private $system_id;
	private $submit_datetime;
	private $username;
	private $start_date;
	private $end_date;
	private $name;
	private $system_code;
	private $type;
	private $url;
	private $open_new_window;
	private $description;
	private $keywords;
	private $parent_id;
	private $total_count;
	private $weekly_count;
	private $is_favorite;
	private $vpn;
	
    function __construct($contruct_type="",$system_value="",$type="main") { 
		
		if( $contruct_type == "id" or $contruct_type == "code" ) {
			
			$conn = $this->getConn();
			
			//if an ID is set, the create the class with this system's data
			if( $contruct_type == "id" ) {
				
				$this->system_id = $system_value;
			
				$sql = "
					SELECT 
						* 
					FROM 
						`mydaemen_systems_each` 
					WHERE
						`system_id` = ?
				";
				
				$exec_array = array($system_value);
				
			}
			// Create the system class with data from a system code and type
			else {
	
				$this->system_code = $system_value;
			
				$sql = "
					SELECT 
						* 
					FROM 
						`mydaemen_systems_each` 
					WHERE
						`system_code` = ? AND
						`type` = ?
				";
				
				$exec_array = array($system_value,$type);
				
			}
					
			try{
				$stmt = $conn->prepare($sql);
				$stmt->execute($exec_array);
				$stmt->setFetchMode(PDO::FETCH_ASSOC);
				
				$row = $stmt->fetch();
				
				// Assigning the data to variables in the class
				if($row){
					
					$this->system_id = $row['system_id'];
					$this->submit_datetime = $row['submit_datetime'];
					$this->username = $row['username'];
					$this->start_date = $row['start_date'];
					$this->end_date = $row['end_date'];
					$this->name = $row['name'];
					$this->system_code = $row['system_code'];
					$this->type = $row['type'];
					$this->url = $row['url'];
					$this->open_new_window = $row['open_new_window'];
					$this->description = $row['description'];
					$this->keywords = $row['keywords'];
					$this->parent_id = $row['parent_id'];
					$this->total_count = $row['total_count'];
					$this->weekly_count = $row['weekly_count'];
					$this->vpn = $row['vpn'];
					
				}
				
				return TRUE;
	
			} catch(PDOException $e) {
				return $e;
			}
		}
		else {
			return TRUE;
		}
    } 


	// Default of the class is refered to as a string type	
	public function __toString () {
		
		return $this->system_id;
		
	}
	
	
	// Simple compare funtion
	public static function cmp_name($a, $b)
	{
	    return strcmp($a->name, $b->name);
	}


	// Connecgting to MySQL, used in most methods needing to inteact with the database
	public static function getConn() {
		
		include "/home/nginx/config/SystemMenu.php";
		
		try {
			$user_db = new PDO("mysql:host=localhost;dbname=daemen", "systems_access", $system_menu_pwd);
			$user_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
			return $user_db;
		} catch (\PDOException $e) {
			return false;
		}
		
	}


	// Returns an array of all non-child SystemMenu classes or just those containing the parent ID if that was specified
	public static function getAllSystems($parent_id="") {
		
		$conn = self::getConn();
		
		if( $parent_id != "" ) {
			$parent_id = intval( $parent_id );
			
			$sql = "
				SELECT 
					*
				FROM 
					`mydaemen_systems_each` 
				WHERE
					`parent_id` = ?
				ORDER BY
					`mydaemen_systems_each`.`name`
			"; 
			
			$exec_array = array($parent_id);
		
		}
		else {
			
			$sql = "
				SELECT 
					* 
				FROM 
					`mydaemen_systems_each` 
				WHERE
					`parent_id` = '0'
				ORDER BY 
					`mydaemen_systems_each`.`name`
			";
			
			$exec_array = array();
		}
		
		try{
			
			$stmt = $conn->prepare($sql);
			$stmt->execute($exec_array);
			return $stmt->fetchAll(PDO::FETCH_CLASS, "\Daemen\SystemMenu");
			
		} catch(PFOException $e) {
			return $e;
		}
		
	}
	
	
	// Returns an array of all SystemMenu classes matching the `name` and %% wild cards
	public static function getAllSystemsSearch($query) {
		
		$conn = self::getConn();
		
		$sql = "
			SELECT 
				* 
			FROM 
				`mydaemen_systems_each` 
			WHERE
				`parent_id` = '0'
			AND
				`name` LIKE :query
			ORDER BY 
				`mydaemen_systems_each`.`name`
		";
		$stmt = $conn->prepare($sql);
		$stmt->execute(['query'=>"%".$query."%"]);
		return $stmt->fetchAll(PDO::FETCH_CLASS, "\Daemen\SystemMenu");

		
	}
	
	
	// Returns an array of all SystemMenu containing the parent ID
	public function getAllChildSystems() {
		
		return self::getAllSystems($this->system_id)
		
	}
	
	
	// Returns an array of all SystemMenu classes or just those containing the parent ID if that was specified
	public static function getAllSystemsChilden() {
		
		return self::getAllSystems()
		
	}
	
	
	// Returns an array of all SystemMenuAccess classes provided with a User class 
	public static function getAllSystemMenusArraysByUser($myd_user) {
		
		$all_system_menus = array();
		
		// Get all systems that are available to all users
		$system_menus_all = SystemMenuAccess::getSystemMenusByAllAccess();
		
		// Get all systems that are specific to this user
		$system_menus_username = SystemMenuAccess::getSystemMenusByUsername($myd_user->getUsername());
		
		// Get all systems that this user is a group member
		$system_menus_groups = SystemMenuAccess::getSystemMenusByGroupDN($myd_user->getMemberOf());
		
		// Get all systems that this user is an OU member
		$system_menus_ou = SystemMenuAccess::getSystemMenusByOU($myd_user->getOu());

		$all_system_menus = array_merge($system_menus_all,$system_menus_username,$system_menus_groups,$system_menus_ou);
		
		return $all_system_menus;

	}


	// Get a multidimensional of SystemMenus keyed by SystemMenuCategory ID for the User
	public static function getAllSystemMenusByUser($myd_user) {
		
		$all_system_menus = self::getAllSystemMenusArraysByUser($myd_user);
		
		// Interate through all SystemMenu classes for the User class
		foreach($all_system_menus as $each_system_menu) { 
			
			// Get the SystemMenus's larger categories
			$system_menu_categories = SystemMenuCategory::getCategoriesBySystemMenu($each_system_menu);
			
			foreach( $system_menu_categories as $each_system_menu_category ) {

				// Creating a array of SystemMenuCategory with the its ID as key
				$all_system_menu_categories[$each_system_menu_category->getCategoryID()] = $each_system_menu_category;
				
				// For sorting
				$all_system_menus_bycategory[$each_system_menu_category->getCategoryID()][$each_system_menu->getSystemID()] = $each_system_menu;
				
			}
		}
		
		uasort($all_system_menu_categories, array('\Daemen\SystemMenuCategory','cmp_order'));
		
		foreach( $all_system_menu_categories as $category_id => $each_system_menu_category ) {
			
			uasort($all_system_menus_bycategory[$category_id], array('\Daemen\SystemMenu','cmp_name'));
			
			// Creating a multidimensional array by SystemMenuCategory ID with all the Users's SystemsMenus under each
			$menu_array[$category_id] = array(
				"SystemMenuCategory"=>$each_system_menu_category,
				"SystemMenus"=>$all_system_menus_bycategory[$category_id]
			);
			
		}
		
		return $menu_array;
		
	}
	
	
	//Pass in User and SystemMenuCategory  to get all menu items in that category for that user
	public static function getAllSystemMenusByUserCategory($myd_user, $category) {
		
		$all_system_menus = self::getAllSystemMenusArraysByUser($myd_user);
		
		$systems_by_category = array();

		foreach($all_system_menus as $each_system_menu) { 
			
			$system_menu_categories = SystemMenuCategory::getCategoriesBySystemMenu($each_system_menu);
			
			foreach( $system_menu_categories as $each_system_menu_category ) {

				if($each_system_menu_category->getCategoryID() === $category->getCategoryId()) {
					$systems_by_category[] = $each_system_menu;
				}
				
			}
		}
		uasort($systems_by_category, array('\Daemen\SystemMenu','cmp_name'));
		return $systems_by_category;
		
	}
	
	
	// Get a multidimensional of SystemMenus keyed by SystemMenuCategory ID for NO User
	public static function getAllSystemMenusByNoUser() {
		
		$all_system_menus = array();
		
		$system_menus_all = SystemMenuAccess::getSystemMenusByAllAccess();
		
		$system_menus_not = SystemMenuAccess::getSystemMenusByNotLoggedIn();

		// An array of all SystemMenuAccesss provided with NO User 
		$all_system_menus = array_merge($system_menus_all,$system_menus_not);

		// Interate through all SystemMenu classes for NO User
		foreach($all_system_menus as $each_system_menu) { 
			
			// Get the SystemMenus's larger categories
			$system_menu_categories = SystemMenuCategory::getCategoriesBySystemMenu($each_system_menu);
			
			foreach( $system_menu_categories as $each_system_menu_category ) {

				// Creating a array of SystemMenuCategory with the its ID as key
				$all_system_menu_categories[$each_system_menu_category->getCategoryID()] = $each_system_menu_category;
				
				// For sorting
				$all_system_menus_bycategory[$each_system_menu_category->getCategoryID()][$each_system_menu->getSystemID()] = $each_system_menu;
				
			}
		}
		
		uasort($all_system_menu_categories, array('\Daemen\SystemMenuCategory','cmp_order'));
		
		foreach( $all_system_menu_categories as $category_id => $each_system_menu_category ) {
			
			uasort($all_system_menus_bycategory[$category_id], array('\Daemen\SystemMenu','cmp_name'));
			
			// Creating a multidimensional array by SystemMenuCategory ID with all of NO User SystemsMenus under each
			$menu_array[$category_id] = array(
				"SystemMenuCategory"=>$each_system_menu_category,
				"SystemMenus"=>$all_system_menus_bycategory[$category_id]
			);
			
		}
		
		return $menu_array;
				
	}


	// Array of Sub menu list for navigation 
	public function printSubMenuNew($myd_user) {
		
		// Getting all the sub menus for a this SystemMenu 
		$all_submenus = SystemMenuSub::getAllSubMenus($this);

		// Getting all the access levels for the User for this SystemMenu 
		$system_menu_access = SystemMenuAccess::getUserAccessLevelForSystem($this,$myd_user);

		$active_selection = "";

		$url = $this->getURL();
		
		$user_submenus = array();
		
		// Go through each SystemMenuSub and add them to the array if they have access
		foreach($all_submenus as $submenu) {
			if($submenu->getAccessLevel() === $system_menu_access->getAccessLevel()) {
				$user_submenus[] = $submenu;
			}
		}
		
		return $user_submenus;

	}


	public function getSubMenuCount() {
		
		return SystemMenuSub::getSubMenuCount($this);
		
	}
	
	
	// Get a SystemMenu's of the other type "main" or "admin" - Do not this being used, Check to eliminate
	public function getRelatedSystem() {
		
		if( $this->type == "admin" ) {
			$other_type = "main";
		}
		else {
			$other_type = "admin";
		}

		$conn = self::getConn();

		$sql = "
			SELECT 
				* 
			FROM 
				`mydaemen_systems_each` 
			WHERE
				`system_code` = ? AND
				`type` = '".$other_type."'
		";
		
		try{
			
			$stmt = $conn->prepare($sql);
			
			$stmt->execute([$this->system_code]);
			
			if( $stmt->rowCount() > 0 ) {
			
				$obj_array = $stmt->fetchAll(PDO::FETCH_CLASS, "\Daemen\SystemMenu");
	
				return $obj_array;
			}
			else {
				return array();
			}
			
		} catch(PFOException $e) {
			
			return $e;
		}
		
	}
	

	// Insert a new record into the table based on values in the private variable
	public function addSystem($myd_user) {
		
		$conn = $this->getConn();
		
		// If this is a main or parent system
		if( $this->system_code != "" ) {

			$sql = "
				INSERT INTO 
					`mydaemen_systems_each`
				SET
					`submit_datetime` = NOW(),
					`username` = ?,
					`name` = ?,
					`system_code` = ?,
					`start_date` = NOW(),
					`end_date` = NULL
			";

			$execute_array = array(
				$myd_user->getUsername(),
				$this->name,
				$this->system_code
			);
			
		}
		//if this child system
		elseif( $this->parent_id != "" ) {
			
			$sql = "
				INSERT INTO 
					`mydaemen_systems_each`
				SET
					`submit_datetime` = NOW(),
					`username` = ?,
					`name` = ?,
					`parent_id` = ?
			";
			
			$execute_array = array(
				$myd_user->getUsername(),
				$this->name,
				$this->parent_id
			);
			
		}
		else {
			return FALSE;
		}
		
		
		try {
			
			$stmt = $conn->prepare($sql);
			
			$stmt->execute($execute_array);
			
			$this->system_id = $conn->lastInsertId();			
			
			return true;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}
		
	}
	
	
	// Increment the total_count and weekly_count for usage and "commonly used" list
	public static function incrementSystemCount($system_id) {
		
		$conn = self::getConn();
		
		$sql = "
			UPDATE
				`mydaemen_systems_each`
			SET
				`total_count` = `total_count` + 1,
				`weekly_count` = `weekly_count` + 1
			WHERE
				`system_id` = :system_id
		";
		
		$stmt = $conn->prepare($sql);
		$stmt->execute(['system_id' => $system_id]);
		
	}
	
	// Updating the the table with values in the private variable
	public function updateSystem($myd_user) {
		
		$conn = $this->getConn();
			
		// If this is a main or parent system
		if( $this->parent_id === 0 ) {

			$sql = "
				UPDATE 
					`mydaemen_systems_each` 
				SET 
					`submit_datetime` = NOW(),
					`username` = ?,
					`start_date` = ?,
					`end_date` = ?,
					`name` = ?,
					`system_code` = ?,
					`url` = ?,
					`open_new_window` = ?,
					`description` = ?,
					`keywords` = ?,
					`vpn` = ?
				WHERE 
					`system_id` = ?
			";
			
			$execute_array = array(
				$myd_user->getUsername(),
				$this->start_date,
				$this->end_date,
				$this->name,
				$this->system_code,
				$this->url,
				$this->open_new_window,
				$this->description,
				$this->keywords,
				$this->vpn,
				$this->system_id
			);
			
		}
		//if this child system
		elseif( $this->parent_id !== 0 ) {
			
			$sql = "
				UPDATE 
					`mydaemen_systems_each` 
				SET 
					`submit_datetime` = NOW(),
					`username` = ?,
					`name` = ?,
					`url` = ?,
					`open_new_window` = ?,
					`parent_id` = ?
				WHERE 
					`system_id` = ?
			";
			
			$execute_array = array(
				$myd_user->getUsername(),
				$this->name,
				$this->url,
				$this->open_new_window,
				$this->parent_id,
				$this->system_id
			);
			
		}
		else {
			return FALSE;
		}
		
	
		try {
			
			$stmt = $conn->prepare($sql);
			
			$stmt->execute($execute_array);
			
			return true;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}
		
	}
	
	
	// Updating the the table with a stripped string from the name of the system - Do not this being used, Check to eliminate
	public function makeSystemCodefromName($myd_user) {
		
		$conn = $this->getConn();
		
		if( $this->system_code == "" ) {
			
			$this->system_code = strtolower(preg_replace('/[^A-Za-z0-9\-]/', '', str_replace('-', '', $this->name)));
			
			$sql = "
				UPDATE 
					`mydaemen_systems_each` 
				SET 
					`submit_datetime` = NOW(),
					`username` = ?,
					`system_code` = ?
				WHERE 
					`system_id` = ?
			";
			
		
			try {
				
				$stmt = $conn->prepare($sql);
				
				$stmt->execute([$myd_user->getUsername(),$this->system_code,$this->system_id]);
				
				return TRUE;
				
			} catch(PDOException $e) {
				
				return $e;
				
			}
		
		}
		else {
			return FALSE;
		}
		
	}
	
	
	// Simple boolean check if the system exsits in the table
	public static function systemExistsByCode($system_code) {
		
		$conn = self::getConn();
		
		$sql = "
			SELECT
				`system_id`
			FROM 
				`mydaemen_systems_each`
			WHERE 
				`system_code` = ?
		";
	
		try {
			
			$stmt = $conn->prepare($sql);
			
			$stmt->execute([$system_code]);

			$stmt->setFetchMode(PDO::FETCH_ASSOC);
			
			if( $stmt->rowCount() > 0 ) {
				return TRUE;
			}
			else {
				return FALSE;
			}
			
		} catch(PDOException $e) {
			
			return $e;
			
		}
		
	}
	
	
	// Deleting the system from the 3 related tables for system
	public function removeSystem() {
		
		$conn = $this->getConn();
			
		$sql = "
			DELETE FROM 
				`mydaemen_systems_each`
			WHERE 
				`system_id` = ? OR
				`parent_id` = ?
			";
				
		try {
			
			$stmt = $conn->prepare($sql);
			$stmt->execute([$this->system_id,$this->parent_id]);
			
			return TRUE;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}
		
		$sql = "
			DELETE FROM 
				`mydaemen_central_user_access`
			WHERE 
				`system_id` = ?
			";
		
		try {
			
			$stmt = $conn->prepare($sql);
			$stmt->execute([$this->system_id]);
			
			return TRUE;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}
		
		$sql = "
			DELETE FROM 
				`mydaemen_systems_categories`
			WHERE 
				`system_id` = ?
			";
		
		try {
			
			$stmt = $conn->prepare($sql);
			$stmt->execute([$this->system_id]);
			
			return TRUE;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}
		
	}
	
	
	// Adding/Inserting a category from this SystemMenu
	public function addCatergory($myd_user,$catergoy_id) {
		
		$conn = $this->getConn();
		
		$sql = "
			INSERT INTO 
				`mydaemen_systems_categories`
			SET 
				`system_id` = ?,
				`submit_datetime` = NOW(),
				`username` = ?,
				`category_id` = ?
		";
		
		try {
			
			$stmt = $conn->prepare($sql);
			$stmt->execute([$this->system_id,$myd_user->getUsername(),$catergoy_id]);
			
			return TRUE;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}

	}		


	// Remove all categories from this SystemMenu 
	public function removeAllCategories() {
		
		$conn = $this->getConn();
		
		$sql = "
			DELETE FROM 
				`mydaemen_systems_categories`
			WHERE 
				`system_id` = ?
			";
		
		try {
			
			$stmt = $conn->prepare($sql);
			$stmt->execute([$this->system_id]);
			
			return TRUE;
			
		} catch(PDOException $e) {
			
			return $e;
			
		}

	}
	
	
	// Standard Getter and setter methods
	public function getSystemID() {
		return intval($this->system_id);
	}

	public function getSubmitDateTime() {
		return $this->submit_datetime;
	}

	public function getUsername() {
		return $this->username;
	}
	public function setUsername($username) {
		$this->username = $username;
	}
	
	public function getStartDate() {
		return $this->start_date;
	}
	public function getStartDate_mdY() {
		return date("m/d/Y", strtotime($this->start_date));
	}
	public function setStartDate($start_date) {
		$this->start_date = date("Y-m-d", strtotime($start_date));
	}
	
	public function getEndDate() {
		return $this->end_date;
	}
	public function getEndDate_mdY() {
		if( is_null($this->end_date) ) {
			return $this->end_date;
		}
		else {
			return date("m/d/Y", strtotime($this->end_date));
		}
	}
	public function setEndDate($end_date) {
		if( is_null($end_date) ) {
			$this->end_date = $end_date;	
		}
		else {
			$this->end_date = date("Y-m-d", strtotime($end_date));
		}
	}
	
	public function getName() {
		return html_entity_decode($this->name);
	}
	public function setName($name) {
		$this->name = htmlentities($name,ENT_QUOTES,"UTF-8");
	}
	
	public function getSystemCode() {
		return $this->system_code;
	}
	public function setSystemCode($system_code) {
		$this->system_code = $system_code;
	}
	
	public function getSystemType() {
		return $this->type;
	}
	public function setSystemType($type) {
		$this->type = $type;
	}
	
	public function getURL() {
		return $this->url;
	}
	public function setURL($url) {
		$this->url = $url;
	}
	
	public function getOpenNewWindow() {
		return $this->open_new_window;
	}
	public function setOpenNewWindow($open_new_window) {
		$this->open_new_window = $open_new_window;
	}
	
	public function getDescription() {
		return $this->description;
	}
	public function setDescription($description) {
		$this->description = $description;
	}
	
	public function getKeywords() {
		return $this->keywords;
	}
	public function setKeywords($keywords) {
		$this->keywords = $keywords;
	}
	
	public function getParentID() {
		return intval($this->parent_id);
	}
	public function setParentID($parent_id) {
		$this->parent_id = $parent_id;
	}
	
	public function getTotalCount() {
		return intval($this->total_count);
	}
	
	public function getWeeklyCount() {
		return intval($this->weekly_count);
	}
	
	public function getIsFavorite() {
		return $this->is_favorite;
	}
	
	public function setIsFavorite($is_favorite) {
		$this->is_favorite = $is_favorite;
	}
	
	public function getVPN() {
		return $this->vpn;
	}
	
	public function setVPN($vpn) {
		$this->vpn = $vpn;
	}
	
	//Used to send over data to Vue. Not grabbing all fields because we don't them viewable by users
	public function jsonSerialize() {
		
		return array(
			'system_id' => $this->system_id,
			'name' => html_entity_decode($this->name),
			'system_code' => $this->system_code,
			'type' => $this->type,
			'url' => $this->url,
			'open_new_window' => $this->open_new_window,
			'description' => $this->description,
			'is_favorite' => $this->is_favorite,
			'total_count' => $this->total_count,
			'vpn' => $this->vpn
		);
		
	}
		
}