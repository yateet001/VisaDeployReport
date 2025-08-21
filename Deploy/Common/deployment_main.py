import os
import json
import requests
import time
from datetime import datetime, timezone
import pandas as pd

# Assuming these utilities are defined in the original workspace
# from workspace_utilities import *
# from token_utilities import *
# from workspace_item_utilities import *

deployment_env = os.getenv("deployment_env")
environment_type = os.getenv("environment_type")
artifact_path = os.getenv("artifact_path")
build_number = os.getenv("build_number")
connections_json = os.getenv("connections")

# Parse the JSON string
connections_data = json.loads(connections_json)

trimmed_lower_deployment_env = deployment_env.lower().strip()
trimmed_lower_environment_type = environment_type.lower().strip()

# Define paths to the configurations in ADO
config_base_path = f"Configuration/{deployment_env}"
deployment_profile_path = f"{config_base_path}/DEPLOYMENT_PROFILE.csv"
configuration_files_list = ["DEPLOYMENT_PROFILE.csv", "IN_TAKE_CONFIG.csv"]

<<<<<<< HEAD

def poll_item_deletion_status(workspace_id, items_to_delete, access_token, timeout=600, poll_interval=30):
    """
    Polls the workspace to confirm that specified items have been deleted.

    Parameters:
    - workspace_id (str): The ID of the workspace.
    - items_to_delete (list): A list of item IDs to confirm deletion for.
    - access_token (str): The authentication token.
    - timeout (int): The maximum time to wait in seconds.
    - poll_interval (int): The time between polls in seconds.

    Raises:
    - Exception: If the timeout is reached or an item fails to be deleted.
    """
    start_time = time.time()
    deleted_ids = set()
    items_to_confirm = {item['id'] for item in items_to_delete}

    while time.time() - start_time < timeout:
        current_items = list_workspace_all_items(workspace_id, access_token)
        current_item_ids = {item['id'] for item in current_items}
        
        # Check for which items are now confirmed deleted
        for item_id in items_to_confirm.copy():
            if item_id not in current_item_ids:
                deleted_ids.add(item_id)
                items_to_confirm.remove(item_id)

        if not items_to_confirm:
            print(f"All items successfully deleted after {int(time.time() - start_time)} seconds.")
            return

        print(f"Waiting for {len(items_to_confirm)} items to be deleted. Retrying in {poll_interval} seconds...")
        time.sleep(poll_interval)

    raise Exception(f"Timeout reached. The following items could not be confirmed as deleted: {list(items_to_confirm)}")

=======
def get_workspace_by_name_with_retry(workspace_name, spn_access_token, max_retries=3):
    """
    Attempts to retrieve workspace details by name with retry logic.
    
    Parameters:
    - workspace_name (str): Name of the workspace to find
    - spn_access_token (str): Access token for API calls
    - max_retries (int): Maximum number of retry attempts
    
    Returns:
    dict or None: Workspace details if found, None otherwise
    """
    for attempt in range(max_retries):
        try:
            print(f"Attempting to find workspace '{workspace_name}' (attempt {attempt + 1}/{max_retries})")
            workspace_details = does_workspace_exists_by_name(workspace_name, spn_access_token)
            
            if workspace_details:
                print(f"✓ Found existing workspace: {workspace_details}")
                return workspace_details
            else:
                print(f"✗ Workspace '{workspace_name}' not found in attempt {attempt + 1}")
                
        except Exception as e:
            print(f"✗ Error checking workspace existence (attempt {attempt + 1}): {str(e)}")
            
        if attempt < max_retries - 1:
            time.sleep(2)  # Wait 2 seconds before retry
            
    return None

def create_workspace_direct_api(workspace_name, capacity_id, spn_access_token):
    """
    Direct API call to create workspace, bypassing the problematic create_workspace function.
    
    Parameters:
    - workspace_name (str): Name of the workspace to create
    - capacity_id (str): Capacity ID for the workspace (None for trial)
    - spn_access_token (str): Access token for API calls
    
    Returns:
    str: Workspace ID of the created workspace
    """
    url = "https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {spn_access_token}",
        "Content-Type": "application/json"
    }
    
    # Prepare the payload
    payload = {
        "displayName": workspace_name
    }
    
    # Add capacity ID if provided (not for trial workspaces)
    if capacity_id:
        payload["capacityId"] = capacity_id
    
    print(f"Creating workspace via direct API call: {workspace_name}")
    print(f"Request URL: {url}")
    print(f"Request Payload: {json.dumps(payload, indent=1)}")
    
    response = requests.post(url, headers=headers, json=payload)
    print(f"Response Status Code: {response.status_code}")
    
    if response.status_code == 201:
        # Workspace created successfully
        response_data = response.json()
        workspace_id = response_data.get("id")
        print(f"✓ Successfully created workspace with ID: {workspace_id}")
        return workspace_id
    elif response.status_code == 409:
        # Workspace already exists - this is the error we're handling
        print(f"Workspace '{workspace_name}' already exists (409 conflict)")
        return None  # Signal that workspace exists but we need to find it
    else:
        # Other error
        try:
            error_details = response.json()
            error_message = error_details.get("message", "Unknown error")
        except:
            error_message = response.text or f"HTTP {response.status_code}"
        
        raise Exception(f"Failed to create workspace: {error_message}")

def create_workspace_with_fallback(workspace_name, capacity_id, spn_access_token):
    """
    Attempts to create a workspace with proper error handling for existing workspaces.
    
    Parameters:
    - workspace_name (str): Name of the workspace to create
    - capacity_id (str): Capacity ID for the workspace (None for trial)
    - spn_access_token (str): Access token for API calls
    
    Returns:
    str: Workspace ID of the created or existing workspace
    """
    try:
        print(f"Attempting to create workspace: '{workspace_name}'")
        
        # Use direct API call instead of the problematic create_workspace function
        workspace_id = create_workspace_direct_api(workspace_name, capacity_id, spn_access_token)
        
        if workspace_id:
            # Workspace was successfully created
            print(f"✓ Successfully created new workspace with ID: {workspace_id}")
            return workspace_id
        else:
            # workspace_id is None, meaning workspace already exists (409 error)
            print(f"Workspace '{workspace_name}' already exists. Attempting to retrieve existing workspace...")
            
            # Try to get the existing workspace with retry logic
            workspace_details = get_workspace_by_name_with_retry(workspace_name, spn_access_token)
            
            if workspace_details and "id" in workspace_details:
                workspace_id = workspace_details["id"]
                print(f"✓ Successfully retrieved existing workspace ID: {workspace_id}")
                return workspace_id
            else:
                # Last resort: try to list all workspaces and find by name
                print("Attempting to find workspace by listing all workspaces...")
                try:
                    all_workspaces = list_all_workspaces(spn_access_token)
                    for ws in all_workspaces:
                        if ws.get("displayName") == workspace_name or ws.get("name") == workspace_name:
                            workspace_id = ws["id"]
                            print(f"✓ Found workspace via workspace listing: {workspace_id}")
                            return workspace_id
                except Exception as list_error:
                    print(f"✗ Failed to list workspaces: {str(list_error)}")
                
                raise Exception(f"Workspace '{workspace_name}' exists but could not retrieve details. Please check permissions or try again later.")
                
    except Exception as e:
        error_str = str(e).lower()
        print(f"✗ Workspace creation failed: {str(e)}")
        
        # Check if the error is due to workspace already existing
        if ("workspace name already exists" in error_str or 
            "workspacenamealreadyexists" in error_str or 
            "409" in error_str or
            "conflict" in error_str):
            
            print(f"Detected workspace conflict. Attempting to retrieve existing workspace...")
            
            # Try to get the existing workspace with retry logic
            workspace_details = get_workspace_by_name_with_retry(workspace_name, spn_access_token)
            
            if workspace_details and "id" in workspace_details:
                workspace_id = workspace_details["id"]
                print(f"✓ Successfully retrieved existing workspace ID: {workspace_id}")
                return workspace_id
            else:
                # Last resort: try to list all workspaces and find by name
                print("Attempting to find workspace by listing all workspaces...")
                try:
                    all_workspaces = list_all_workspaces(spn_access_token)
                    for ws in all_workspaces:
                        if ws.get("displayName") == workspace_name or ws.get("name") == workspace_name:
                            workspace_id = ws["id"]
                            print(f"✓ Found workspace via workspace listing: {workspace_id}")
                            return workspace_id
                except Exception as list_error:
                    print(f"✗ Failed to list workspaces: {str(list_error)}")
                
                raise Exception(f"Workspace '{workspace_name}' exists but could not retrieve details. Please check permissions or try again later.")
        else:
            # Re-raise the original exception if it's not about existing workspace
            raise e
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448

def orchestrator(tenant_id, client_id, client_secret, connections_data):
    """
    Orchestrates the deployment of networks with improved workspace handling.

    Parameters:
    - tenant_id (str): The Azure Active Directory tenant ID used for authentication.
    - client_id (str): The client ID (application ID) used for authentication with Azure.
    - client_secret (str): The client secret associated with the Azure application.
    - connections_data (dict): A dictionary of all connection names and types.

    Raises:
    Exception: If any error occurs during onboarding of networks.
    """

    try:
        # Read deployment and capacity configuration files
        all_deployment_profile_df = pd.read_csv(deployment_profile_path)

        # Filter the deployment profiles for the environments and networks to be onboarded
        deployment_operation_ws_details_df = all_deployment_profile_df[
            (all_deployment_profile_df["to_be_onboarded"]) &
            (all_deployment_profile_df["deployment_env"].str.strip().str.lower() == trimmed_lower_deployment_env) &
            (all_deployment_profile_df["environment_type"].str.strip().str.lower() == trimmed_lower_environment_type) &
            (all_deployment_profile_df["transformation_layer"].str.strip().str.lower() == "operations")
        ]

        # Ensure there is at least one matching row
        if deployment_operation_ws_details_df.empty:
            raise ValueError("No matching deployment profile found.")

        # Extract the single record correctly
        row = deployment_operation_ws_details_df.iloc[0]  # Get the first (and only) row
        capacity_id = row["capacity_id"]
        workspace_name = row["workspace_prefix"]
        transformation_layer = row["transformation_layer"]
        workspace_users = row["workspace_default_groups"]
        spn_access_token = get_spn_access_token(tenant_id, client_id, client_secret)

<<<<<<< HEAD
        # Check if workspace exists using API
        operations_workspace_details = does_workspace_exists_by_name(workspace_name, spn_access_token)
        workspace_id = operations_workspace_details["id"] if operations_workspace_details else None

        # Check if the workspace does not already exist
        if workspace_id is None:
            print(f"Workspace '{workspace_name}' not found. Creating a new workspace.")
            # Create a new workspace
            workspace_id = create_workspace(workspace_name, capacity_id, spn_access_token)
            
            # Add security group/users to the new workspace
            add_security_group_to_workspace(workspace_id, workspace_name, spn_access_token, workspace_users)
            
            # Deploy artifacts to the newly created workspace
            is_deployment = True
            deploy_artifacts(
                transformation_layer, connections_data, artifact_path,
                "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                is_deployment, items={}
            )
            print(f"Initial deployment to new workspace '{workspace_name}' complete.")

        else:
            print(f"Workspace '{workspace_name}' already exists. Performing incremental update.")
            # If workspace already exists, update it incrementally
            is_deployment = False

            # Ensure the security group/users are still added to the workspace
            add_security_group_to_workspace(workspace_id, workspace_name, spn_access_token, workspace_users)

            # Fetch the list of existing items in the workspace
            existing_items_in_workspace = list_workspace_all_items(workspace_id, spn_access_token)

            # Identify and delete outdated or obsolete items
            print("Identifying and deleting outdated items...")
            items_to_delete = delete_old_items(
                workspace_id, existing_items_in_workspace, artifact_path, "ARM/" + transformation_layer, spn_access_token
            )
            
            # Poll to ensure deletions are processed before deploying new items
            if items_to_delete:
                poll_item_deletion_status(workspace_id, items_to_delete, spn_access_token)

            # Redeploy updated artifacts to the existing workspace
            deploy_artifacts(
                transformation_layer, connections_data, artifact_path,
                "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                is_deployment, items=existing_items_in_workspace
            )
            print(f"Incremental update of workspace '{workspace_name}' complete.")

    except Exception as e:
        print(f"An unexpected error occurred during orchestration: {e}")
        # Re-raise the exception to stop the process and signal an error
        raise e

=======
        print(f"=== Workspace Management for '{workspace_name}' ===")
        
        # STEP 1: Try to find existing workspace first with multiple approaches
        workspace_id = None
        
        # Approach 1: Use the existing function
        print("Checking for existing workspace using does_workspace_exists_by_name...")
        operations_workspace_details = get_workspace_by_name_with_retry(workspace_name, spn_access_token)
        if operations_workspace_details and "id" in operations_workspace_details:
            workspace_id = operations_workspace_details["id"]
            print(f"✓ Found workspace using does_workspace_exists_by_name: {workspace_id}")
        
        # Approach 2: If not found, try listing all workspaces
        if not workspace_id:
            print("Workspace not found via name lookup. Trying to list all workspaces...")
            try:
                all_workspaces = list_all_workspaces(spn_access_token)
                print(f"Found {len(all_workspaces)} total workspaces")
                
                for ws in all_workspaces:
                    ws_name = ws.get("displayName", ws.get("name", ""))
                    if ws_name == workspace_name:
                        workspace_id = ws["id"]
                        print(f"✓ Found workspace in workspace listing: {workspace_id}")
                        break
                        
            except Exception as list_error:
                print(f"✗ Could not list workspaces: {str(list_error)}")

        # STEP 2: Handle workspace creation or update
        if workspace_id is None:
            print(f"Workspace '{workspace_name}' does not exist. Creating new workspace...")
            try:
                # Create a new workspace with improved error handling
                workspace_id = create_workspace_with_fallback(workspace_name, capacity_id, spn_access_token)

                # Add security group/users to the new workspace
                print("Adding security groups to the new workspace...")
                are_user_added = add_security_group_to_workspace(
                    workspace_id, workspace_name, spn_access_token, workspace_users
                )

                # Mark the deployment as full
                is_deployment = True
                print("Performing full deployment to new workspace...")

                # Deploy artifacts to the newly created workspace
                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    is_deployment, items={}
                )

            except Exception as e:
                # If creation fails and workspace was created, attempt cleanup
                if workspace_id:
                    try:
                        print(f"Attempting to clean up workspace {workspace_id} due to deployment failure...")
                        delete_workspace(workspace_id, spn_access_token)
                        workspace_id = None
                        print("Workspace cleanup completed.")
                    except Exception as cleanup_error:
                        print(f"Warning: Failed to clean up workspace: {str(cleanup_error)}")
                
                # Re-raise the exception after cleanup attempt
                raise e

        else:
            print(f"Workspace '{workspace_name}' already exists with ID: {workspace_id}")
            print("Performing incremental deployment to existing workspace...")
            
            try:
                is_deployment = False

                # Ensure the security group/users are still added to the workspace
                print("Updating security groups on existing workspace...")
                are_user_added = add_security_group_to_workspace(
                    workspace_id, workspace_name, spn_access_token, workspace_users
                )

                # Fetch the list of existing items in the workspace
                print("Fetching existing workspace items...")
                items = list_workspace_all_items(workspace_id, spn_access_token)

                # Delete outdated or obsolete items before deploying new ones
                print("Cleaning up outdated items...")
                are_items_deleted = delete_old_items(
                    workspace_id, items, artifact_path, "ARM/" + transformation_layer, spn_access_token
                )

                # Wait for some time before redeploying to ensure deletions are processed
                print("Waiting for cleanup operations to complete...")
                time.sleep(450)

                # Redeploy updated artifacts to the existing workspace
                print("Deploying updated artifacts...")
                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    is_deployment, items=items
                )

            except Exception as exc:
                error_message = error_message + str(exc)

        if error_message:
            raise Exception(error_message)
            
        print(f"✓ Deployment completed successfully for workspace '{workspace_name}' (ID: {workspace_id})")
        
    except Exception as e:
        print(f"✗ Deployment failed with error: {str(e)}")
        raise e # Re-raise for debugging
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448

if __name__ == "__main__":
    try:
        # Extract individual values directly from environment variables
        tenant_id = os.getenv("tenant_id")
        client_id = os.getenv("client_id")
        client_secret = os.getenv("client_secret")

        # Validate that credentials are not None before proceeding
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("Authentication credentials (tenant_id, client_id, client_secret) must be provided as environment variables.")

        print("=== Starting Workspace Deployment Orchestration ===")
        print(f"Environment: {deployment_env}")
        print(f"Environment Type: {environment_type}")
        print(f"Artifact Path: {artifact_path}")
        
        # Call the orchestrator function with the extracted values as arguments
        orchestrator(tenant_id, client_id, client_secret, connections_data)
        
        print("=== Deployment Orchestration Completed Successfully ===")
    
    except Exception as e:
        print(f"=== Deployment Orchestration Failed ===")
        print(f"Error: {str(e)}")
        # If an error occurs during the execution, raise the exception
        raise e