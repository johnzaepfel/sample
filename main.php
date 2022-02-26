<?php
//READY FOR V2.0
include("/home/nginx/html/shared/mydaemen_auth.php");
include("config.php");

$mydaemen_user_system_access->checkAccess();

use \Daemen\SystemMenu;

if($mydaemen_user_system_access->getAccessLevel() != "admin") {
	header("Location: ".$mdm_admin_address."main.php");
	exit;
}

// print "<pre>"; print_r($_POST); print "</pre>";

$add_message = "";

//Adding a new system / page
if(isset($_POST['add_button']) and $_POST['add_button'] == "Add a System") {
	
	$insert_items = $_POST;
	
	if( $insert_items['new_type'] != "" ) {
		//Checking the new_type is not empty 

		//This is a main or parent system
		if( $insert_items['new_type'] == "mainadmin" or $insert_items['new_type'] == "admin" or $insert_items['new_type'] == "main" ) {
			
			if( $insert_items['name-main'] != "" ) {
				//Checking the name-main is not empty 
				
				$edit_system_menu = new SystemMenu();

				//Setting the system_code
				$edit_system_menu->setSystemCode($insert_items['system_code']);
				

				if( $insert_items['new_type'] == "mainadmin" or $insert_items['new_type'] == "main" ) {
				
					//Setters
					$edit_system_menu->setName($insert_items['name-main']);
					$edit_system_menu->setSystemType("main");
					
					//Adding SystemMenu
					$add_result = $edit_system_menu->addSystem($mydaemen_user);
					
					//ElasticSearch insert
					$params = [
		                'index' => 'system_menu_items',
		                'type' => 'my_type',
		                'id' => $edit_system_menu->getSystemID(),
		                'body' => ['name' => $edit_system_menu->getName()]
			        ];
			        
			        \Daemen\ElasticSearch::getClient()->index($params);
					
				}
				if( $insert_items['new_type'] == "mainadmin" or $insert_items['new_type'] == "admin" ) {
				
					//Setters
					$edit_system_menu->setName($insert_items['name-main']." Admin");
					$edit_system_menu->setSystemType("admin");
	
					//Adding SystemMenu
					$add_result = $edit_system_menu->addSystem($mydaemen_user);
					
					//ElasticSearch insert
					$params = [
		                'index' => 'system_menu_items',
		                'type' => 'my_type',
		                'id' => $edit_system_menu->getSystemID(),
		                'body' => ['name' => $edit_system_menu->getName()]
			        ];
			        
			        \Daemen\ElasticSearch::getClient()->index($params);
					
				}
				
				if( $add_result === TRUE ) {
				
					//redirect to the SystemMenu edit page
					header("Location: each.php?system_id=".$edit_system_menu->getSystemID());
					exit;
				
				}
				else {
				
					$add_message = "Error entering in the database: ".$add_result->getMessage() ;
				
				}

			}
			else {

				$add_message = "Name is missing.  It is required.";

			}
			
		}
		//This is a chile system
		elseif ( $insert_items['new_type'] == "child") {

			if( $insert_items['name-child'] != "" ) {
				
				$edit_system_menu = new SystemMenu();

				//Setters
				$edit_system_menu->setName($insert_items['name-child']);
				$edit_system_menu->setParentID($insert_items['parent_id']);
				
				//Adding the SystemMenu
				$add_result = $edit_system_menu->addSystem($mydaemen_user);
				
				//ElasticSearch insert
				$params = [
	                'index' => 'system_menu_items',
	                'type' => 'my_type',
	                'id' => $edit_system_menu->getSystemID(),
	                'body' => ['name' => $edit_system_menu->getName()]
		        ];
		        
		        \Daemen\ElasticSearch::getClient()->index($params);
					
				if( $add_result === TRUE ) {
				
					//redirect to the SystemMenu edit page
					header("Location: each.php?system_id=".$edit_system_menu->getSystemID());
					exit;
				
				}
				else {
				
					$add_message = "Error entering in the database: ".$add_result->getMessage();
				
				}

			}
			else {

				$add_message = "Name is missing.  It is required.";

			}
			
		}
		else {
			$add_message = "Invalid Type!";
		}

	}
	else {
		
		$add_message = "You did not select a type.  Please select one.";
		
	}
		
}

//Details
if(isset($_POST['edit_button']) and $_POST['edit_button'] == "Details") {
	header("Location: each.php?system_id=".$_POST['id']);
	exit;
}

//Sub Menu
if(isset($_POST['submenu_button']) and $_POST['submenu_button'] == "Sub Menu") {
	header("Location: edit_sub.php?system_id=".$_POST['id']);
	exit;
}

$delete_message = "";

//Deleting a user
if(isset($_POST['confirm_button']) and $_POST['confirm_button'] == "Confirm Delete") {
	
	$edit_system_menu = new SystemMenu("id",$_POST['id']);

	$edit_system_menu->setParentID($_POST['id']);
				
	$delete_result = $edit_system_menu->removeSystem();
	
	if( $delete_result === TRUE ) {
	
		$delete_message = "Successfully deleted system.";
		
		//Delete in ElasticSearch
		$params = [
			'index' => 'system_menu_items',
			'type' => 'my_type',
			'id' => $_POST['id']
		];
		
		\Daemen\ElasticSearch::getClient()->delete($params);
		
	}
	else {
	
		$delete_message = "Error entering in the database: ".$delete_result->getMessage( );
	
	}
}

//Getting an array with all the SystemMenus, will all be listed below
$all_systems = SystemMenu::getAllSystems();

// print "<pre style='background:#FFFFFF'>"; var_dump($all_systems); print "</pre>";


$page_title = $mdm_system_title;

?>
<?php require("/home/nginx/html/portal/resources/partials/system_wrapper.php");?>
<link rel="stylesheet" type="text/css" href="/shared/styles/tables.css">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.20/css/jquery.dataTables.min.css"/>
<link rel="stylesheet" type="text/css" href="https://ajax.googleapis.com/ajax/libs/jqueryui/1.8.17/themes/base/jquery-ui.css" media="all" />
<link rel="stylesheet" type="text/css" href = "<?php echo($mdm_admin_css_path)?>">
<br>
<div id="summary-container" class = "article centered-block">
	<h1 class = "page-title centered-heading" style="color:black;"><?php print $page_title; ?></h1>
	<hr>

	<?php 
	
	if(isset($_POST['delete_button']) and $_POST['delete_button'] == "Delete") :
	
	?>
	<form action="" method="post" name="nav" id="nav" class="card border rounded p-2 mb-2">
		<div class="card-body">
			<h5 class="card-title col-sm-12 rounded">Are you sure you want to Delete?</h5>
			<div class="text-center alert alert-danger">
				<p>Warning!! This will delete the system, any children, and all access levels!<p>
			</div>
			<div class="centered-block text-center">
				<input name="id" type="hidden" id="id" value="<?php print $_POST['id']; ?>">
				<input name="cancel_button" type="submit" class="btn btn-primary" id="cancel_button" value="Cancel">
				<input name="confirm_button" type="submit" class="btn btn-danger" id="confirm_button" value="Confirm Delete">
			</div>
		</div>
	</form>
	<?php 
	
	else :
	
	?>
	<form id="add_system" method="post" class="card border rounded p-2 mb-2" action="">
		<input name="new_user" type="hidden" id="new_user" value="new_user" />
		<div class="card-body">
			<div class="row">
				<h5 class="card-title col-sm-12 rounded">Adding Main or Child System</h5>
			</div>
			<?php 
				
			if( $add_message != "" ):
			
			?>
			<div class="row" id="warning_row_add">
				<div class="col-12 text-center alert alert-danger">
					<?php print $add_message; ?>
				</div>
			</div>
			<?php 
				
			endif;
			
			?>
			<div class="row">
				<div class="col-sm-12 centered-block text-center">
					<input type="radio" name="new_type" value="main" id="new_type-main" class="new_type">
					<label for="new_type-main">Main</label>
					<span style="padding-left: 2em; padding-right: 2em;">OR</span>
					<input type="radio" name="new_type" value="child" id="new_type-child" class="new_type">
					<label for="new_type-child">Child</label>
				</div>
			</div>
			<div class="row choose_group" id="main-decription">
				<div class="col-12">
					<p class="text-center">Used if this is a main system.</p>
				</div>
			</div>
			<div class="row choose_group" id="child-decription">
				<div class="col-12">
					<p class="text-center">Used if the system is a sub page of another system.</p>
				</div>
			</div>
			<div class="row choose_group" id="main-group">
				<div class="col-12 col-sm-8">
					<label>System Name:</label>
					<input name="name-main" type="text" class = "form-control" id="name-main" size="40" />
				</div>
				<div class="col-12 col-sm-4">
					<label>System Code:</label>
					<input type="text" class = "form-control" name="system_code" id="system_code" />
				</div>
			</div>
			<div class="row choose_group" id="child-group">
				<div class="col-12 col-sm-6">
					<label>System Name:</label>
					<input name="name-child" type="text" class = "form-control" id="name-child" size="40" />
				</div>
				<div class="col-12 col-sm-6">
					<label>Parent System:</label>
				<select name="parent_id" class="form-control" id="parent_id">
						<option selected="selected"> - </option>
						<?php 
					
						foreach( $all_systems as $each_SystemMenu ) :
						
						?>
						<option value="<?php print $each_SystemMenu->getSystemID(); ?>"><?php print $each_SystemMenu->getName(); ?></option>
						<?php 
						
						endforeach;
						
						?>
					</select></td>
				</div>
			</div>
			<div class="row">
					<div class="col-sm-12 text-center" style="margin-top: 15px;">
						<input name="add_button" type="submit" class="btn btn-primary" value="Add a System" id="add_button" />
					</div>
			</div>
		</div>
	</form>
	
	<div class="card border rounded p-2 mb-2">
		<div class="card-body">
			<div class="row">
				<h5 class="card-title col-sm-12 rounded">Systems</h5>
			</div>
			<?php 
				
			if( $delete_message != "" ):
			
			?>
			<div class="row" id="warning_row_delete">
				<div class="col-12 text-center alert alert-danger">
					<?php print $delete_message; ?>
				</div>
			</div>
			<?php 
				
			endif;
			
			?>
		<div class="table-responsive">
			<table class="table table-striped" id = "data-table" style="width:100%!important;">
				<thead>
					<tr class="cell_heading">
						<td class="cell_header">System Name</td>
						<td class="cell_header">System Code</td>
						<td class="cell_header">Start Date</td>
						<td class="cell_header">Expiration Date</td>
						<td class="cell_header">&nbsp;</td>
						<td class="cell_header">&nbsp;</td>
					</tr>
				</thead>
				<?php
				
				//Listing each SystemMenu
				foreach( $all_systems as $each_SystemMenu ) :
	
				?>
				<tr>
					<td>
						<a name="<?php print $each_SystemMenu->getSystemID(); ?>" id="<?php print $each_SystemMenu->getSystemID(); ?>"></a><?php print $each_SystemMenu->getName(); ?> <a href="<?php print $each_SystemMenu->getURL(); ?>" target="_blank">&rarr;</a>
						
						<?php 
						
						$all_sub_systems = SystemMenu::getAllSystems($each_SystemMenu->getSystemID());
						
						if( count($all_sub_systems) > 0 ) :
						
						?>
						<table class="card-table">
							<tr class="card-table-heading">
								<td nowrap="nowrap"><strong>Sub Name</strong></td>
								<td nowrap="nowrap">&nbsp;</td>
								<td nowrap="nowrap">&nbsp;</td>
							</tr>
						<?php 
						
						foreach( $all_sub_systems as $each_sub_SystemMenu ) : 
						
							//print "<pre style='background:#FFFFFF'>"; print_r($each_sub_SystemMenu->); print "</pre>";
							
						?>
						<tr>
							<td><a name="<?php print $each_sub_SystemMenu->getSystemID(); ?>" id="<?php print $each_sub_SystemMenu->getSystemID(); ?>"></a><?php print $each_sub_SystemMenu->getName(); ?> <a href="<?php print $each_sub_SystemMenu->getURL(); ?>" target="_blank">&rarr;</a></td>
							<td>
								<form id="sub_update_users_<?php print $each_sub_SystemMenu->getSystemID(); ?>" method="post" action="each.php?system_id=<?php print $each_sub_SystemMenu->getSystemID(); ?>">
									<input name="id" type="hidden" value="<?php print $each_sub_SystemMenu->getSystemID(); ?>" />
									<input name="parent_id" type="hidden" value="<?php print $each_sub_SystemMenu->getParentID(); ?>" />
									<input name="edit_child_button" type="submit" class="btn btn-primary" id="edit_child_button<?php print $each_sub_SystemMenu->getSystemID(); ?>" value="Details" />
								</form>
							</td>
							<td>
								<form id="sub_delete_users_<?php print $each_SystemMenu->getSystemID(); ?>" method="post" action="#<?php print $each_sub_SystemMenu->getSystemID(); ?>">
									<input name="id" type="hidden" value="<?php print $each_sub_SystemMenu->getSystemID(); ?>" />
									<input name="parent_id" type="hidden" value="<?php print $each_sub_SystemMenu->getParentID(); ?>" />
									<input name="delete_button" type="submit" class="btn btn-danger" id="delete_button<?php print $each_sub_SystemMenu->getSystemID(); ?>" value="Delete" />
								</form>
							</td>
						</tr>
						<?php 
					
						endforeach; 
						
						?>
						</table>
						<?php 
						
						endif;
						
						?>
					</td>
					<td><?php print $each_SystemMenu->getSystemCode(); ?></td>
					<td nowrap><span style="font-size: 0;"><?php print date( "Ymd", strtotime($each_SystemMenu->getStartDate()) ); ?></span><?php print date( "M j, Y", strtotime($each_SystemMenu->getStartDate()) ); ?></td>
					<?php
					
					if( date("U") > strtotime($each_SystemMenu->getEndDate()) and $each_SystemMenu->getEndDate() != NULL ) {
						$endtime_warning_style = ' style="color: red; font-weight: bold;"';
					}
					else {
						$endtime_warning_style = "";
					}
					
					?>
					<td<?php print $endtime_warning_style; ?> nowrap>
						<?php 
							
						if( $each_SystemMenu->getEndDate() == NULL ) :
						
						?>
						None
						<?php 
							
						else:
						
						?>
						<span style="font-size: 0;"><?php print date( "Ymd", strtotime($each_SystemMenu->getEndDate()) ); ?></span><?php print date( "M j, Y", strtotime($each_SystemMenu->getEndDate()) ); ?>
						<?php
							
						endif;
						
						?>	
					</td>
					<td nowrap="nowrap">
						<form id="update_users_<?php print $each_SystemMenu->getSystemID(); ?>" method="post" action="">
							<input name="id" type="hidden" value="<?php print $each_SystemMenu->getSystemID(); ?>" />
							<input name="parent_id" type="hidden" value="0" />
							<input name="edit_button" type="submit" class="btn btn-primary" id="edit_button<?php print $each_SystemMenu->getSystemID(); ?>" value="Details" />
							<input name="submenu_button" type="submit" class="btn btn-secondary" value="Sub Menu" id="submenu_button<?php print $each_SystemMenu->getSystemID(); ?>" />
						</form>
					</td>
					<td>
						<form id="delete_users_<?php print $each_SystemMenu->getSystemID(); ?>" method="post" action="#<?php print $each_SystemMenu->getSystemID(); ?>">
							<input name="id" type="hidden" value="<?php print $each_SystemMenu->getSystemID(); ?>" />
							<input name="parent_id" type="hidden" value="0" />
							<input name="delete_button" type="submit" class="btn btn-danger" id="delete_button<?php print $each_SystemMenu->getSystemID(); ?>" value="Delete" />
						</form>
					</td>
				</tr>
				<?php 
			
				endforeach; 
			
				?>
			</table>
		</div>
		</div>
	</div>
	<?php 
	
	endif; //Delete
	
	?>
	
</div>
<?php require("/home/nginx/html/portal/resources/partials/footer.php");?>

<script type="text/javascript" src="https://cdn.datatables.net/1.10.16/js/jquery.dataTables.min.js"></script>
<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/bootbox.js/4.4.0/bootbox.min.js"></script>
<script>
	
$(document).ready(function(){
	
	//jquery dataTables 
    $('#data-table').DataTable( {
	    lengthMenu: [
	    	[25,50,100,-1],
	    	['25', '50', '100', 'All']
	    ],
	    "pageLength": 25,
	    "columnDefs": [
		    {"orderable": false, "targets": [4,5] }
		]
    });

	//Date pickers
	$.datepicker.setDefaults( $.datepicker.regional[ "" ] );
	$("[id^=start_date_]").datepicker( $.datepicker.regional[ "en-US" ] );
	$("[id^=end_date_]").datepicker( $.datepicker.regional[ "en-US" ] );

	//jquery actions
	$('.choose_group').hide();
	$('.new_type').change(function() {
		$('.choose_group').hide();
		//Main or parent system
		if( $(this).val() == 'mainadmin' || $(this).val() == 'main' || $(this).val() == 'admin' ) {
			$('#main-group').show();
		}
		//Child system
		else if( $(this).val() == 'child' ) {
			$('#child-group').show();
		}
		$('#'+$(this).val()+'-decription').show();
 	});
 	
 	$('#warning_row_add').delay( 5000 ).fadeOut('slow','swing');
 	
 	$('#warning_row_delete').delay( 5000 ).fadeOut('slow','swing');
 	
 	//Stripping the string from the name of the system for the system code
 	$('#name-main').keyup(function(){
        var dname_without_space = $("#name-main").val().toLowerCase().replace(/ /g, "");
        var name_without_special_char = dname_without_space.replace(/[^a-zA-Z 0-9]+/g, "");
	    $('#system_code').val(name_without_special_char);
	});
 	
});

</script>

