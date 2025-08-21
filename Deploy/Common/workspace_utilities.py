import os 
import json
import requests
import time 
from datetime import datetime, timezone
from workspace_utilities import *
from token_utilities import *
from workspace_item_utilities import *
import pandas as pd

spn = os.getenv("spn")
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

def get_workspace_by_name_with_retry(workspace_name, spn_access_token, max_retries=3):
    """
    Attempts to retrieve workspace details by name with retry logic.
    
    Parameters:
<<<<<<< HEAD
    - workspace_name (str): The name of the Power BI workspace to search for.
    - token (str): The access token used for authentication to the Fabric API.
=======
    - workspace_name (str): Name of the workspace to find
    - spn_access_token (str): Access token for API calls
    - max_retries (int): Maximum number of retry attempts
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448
    
    Returns:
    dict or None: Workspace details if found, None otherwise
    """
<<<<<<< HEAD
    try:
        url = f"https://api.fabric.microsoft.com/v1/workspaces?$filter=displayName eq '{workspace_name}'"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        if len(response.json().get("value", [])) > 0:
            return response.json()["value"][0]
        
        return None
    except requests.exceptions.RequestException as e:
        raise Exception(f"Checking workspace failed: {e}")

def delete_workspace(workspace_id, access_token):
=======
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
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448
    """
    Direct API call to create workspace, bypassing the problematic create_workspace function.
    
    Parameters:
    - workspace_name (str): Name of the workspace to create
    - capacity_id (str): Capacity ID for the workspace (None for trial)
    - spn_access_token (str): Access token for API calls
    
    Returns:
<<<<<<< HEAD
    - bool: True if the workspace was successfully deleted.
=======
    str: Workspace ID of the created workspace
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448
    """
    url = "https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {spn_access_token}",
        "Content-Type": "application/json"
    }
    
<<<<<<< HEAD
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error deleting workspace {workspace_id}: {e}")
=======
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
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448

def create_workspace_with_fallback(workspace_name, capacity_id, spn_access_token):
    """
<<<<<<< HEAD
    Creates a new workspace in Microsoft Fabric.

    Parameters:
    - workspace_name (str): The name to assign to the new workspace.
    - capacity_id (str): The capacity ID under which the workspace should be created.
    - token (str): The access token for authentication to the Fabric API.

    Returns:
    - str: The ID of the created workspace.
    """
    try:
        url = "https://api.fabric.microsoft.com/v1/workspaces"
        payload = {
            "displayName": workspace_name,
            "capacityId": capacity_id
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        workspace_id = response.json()["id"]
        return workspace_id
    except requests.exceptions.RequestException as e:
        raise Exception(f"Unable to create workspace '{workspace_name}': {e}")

def parse_user_info(user_info_str):
    """
    Parses and cleans the user info string into a list of dictionaries.
    
    Parameters:
    - user_info_str (str): The string of user details in JSON format, separated by "|".
    
    Returns:
    - list: A list of user information dictionaries.
    """
    user_info_list = []
    seen_configuration = set()

    for user_info in user_info_str.split("|"):
        user_info = user_info.strip().replace("'", "\"")
        try:
            user_info_dict = json.loads(user_info)
            user_info_dict = {key.strip(): value.strip() for key, value in user_info_dict.items()}
            
            identifier = user_info_dict.get("identifier", "").strip().lower()
            principal_type = user_info_dict.get("principalType", "").strip().lower()
            access = user_info_dict.get("access", "").strip().lower()

            if not all([identifier, principal_type, access]):
                raise ValueError(f"Missing required fields in user info: {user_info_dict}")

            user_tuple = (identifier, principal_type, access)
            if user_tuple not in seen_configuration:
                seen_configuration.add(user_tuple)
                user_info_list.append(user_info_dict)
        except json.JSONDecodeError:
            raise ValueError(f"Error parsing user info: Invalid JSON format in '{user_info}'.")
        except Exception as e:
            raise Exception(f"Error processing user info string: {e}")
    return user_info_list

def validate_no_duplicates(user_info_list):
    """
    Validates that no user appears twice with different roles.

    Parameters:
    - user_info_list (list): A list of user information dictionaries.

    Returns:
    - bool: True if validation is successful.
    """
    seen_identifiers = {}
    for user_info in user_info_list:
        identifier = user_info.get("identifier", "").strip().lower()
        role = user_info.get("access", "").strip().lower()
        if identifier in seen_identifiers and seen_identifiers[identifier] != role:
            raise ValueError(f"User '{identifier}' has been specified more than once with different roles: '{seen_identifiers[identifier]}' and '{role}'.")
        seen_identifiers[identifier] = role
    return True

def list_current_workspace_users(workspace_id, token):
    """
    Lists the current users and their access rights in the specified workspace.
    
    Parameters:
    - workspace_id (str): The workspace ID.
    - token (str): The authorization token.
    
    Returns:
    - dict: A dictionary of {identifier: access_right} for all current users.
    """
    try:
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/users"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        current_users = {user["identifier"].lower(): user["groupUserAccessRight"].lower() for user in response.json().get("value", [])}
        return current_users
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch current users of workspace {workspace_id}: {e}")

def prepare_users_to_add(user_info_list, current_users):
    """
    Prepares a list of users to add to the workspace.
    
    Parameters:
    - user_info_list (list): A list of user information dictionaries.
    - current_users (dict): A dict of current users in the workspace.
    
    Returns:
    - tuple: A tuple containing lists of users to add, update, and a dict of users to remove.
    """
    users_to_add = []
    users_to_update_access = []
    users_to_remove = current_users.copy()

    for user_info in user_info_list:
        identifier = user_info.get("identifier", "").strip().lower()
        access = user_info.get("access", "").strip().lower()

        if identifier not in current_users:
            users_to_add.append(user_info)
        elif current_users[identifier].lower() != access:
            users_to_update_access.append(user_info)
        
        users_to_remove.pop(identifier, None)

    return users_to_add, users_to_update_access, users_to_remove

def _sync_users_in_batches(workspace_id, token, users_to_add, users_to_update, users_to_remove):
    """
    Synchronizes workspace users by making a single bulk API call.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/users/bulk"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    operations = []

    for user in users_to_add:
        operations.append({
            "operation": "add",
            "identifier": user["identifier"],
            "principalType": user["principalType"],
            "groupUserAccessRight": user["access"]
        })
    
    for user in users_to_update:
        operations.append({
            "operation": "update",
            "identifier": user["identifier"],
            "principalType": user["principalType"],
            "groupUserAccessRight": user["access"]
        })
    
    for identifier in users_to_remove.keys():
        operations.append({
            "operation": "remove",
            "identifier": identifier,
            "principalType": users_to_remove[identifier] # This will need to be the actual type, but the current code can't determine it easily.
        })

    if not operations:
        print("No user access changes required.")
        return

    payload = {"operations": operations}
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print("Bulk user synchronization request successful.")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to sync users in bulk: {e}")

def add_security_group_to_workspace(workspace_id, workspace_name, token, user_info_str):
    """
    Adds, updates, and removes users/groups to a Fabric workspace based on a configuration string.
    
    Parameters:
    - workspace_id (str): The ID of the workspace.
    - workspace_name (str): The name of the workspace.
    - token (str): The authorization token.
    - user_info_str (str): A string of user details in JSON format, separated by "|".
    """
    try:
        user_info_list = parse_user_info(user_info_str)
        validate_no_duplicates(user_info_list)
        current_users = list_current_workspace_users(workspace_id, token)
        users_to_add, users_to_update, users_to_remove = prepare_users_to_add(user_info_list, current_users)
        
        # Now, make a single API call to sync all changes
        _sync_users_in_batches(workspace_id, token, users_to_add, users_to_update, users_to_remove)
    except Exception as e:
        raise Exception(f"Error occurred while syncing security groups: {e}")
=======
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

    error_message = ""

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
        row = deployment_operation_ws_details_df.iloc[0]
        # Use workspaceName from environment variable if provided, else from CSV
        workspace_name = os.getenv("workspaceName", row["workspace_prefix"])
        # If using trial workspace, ignore capacity_id
        if workspace_name in ["VISACICDDev", "VISACICDQA"]:
            capacity_id = None
        else:
            capacity_id = row.get("capacity_id", None)
        
        # Fix: Use the exact case from CSV but ensure it matches the artifact structure
        transformation_layer = row["transformation_layer"].strip()
        # Since the artifact structure uses "Operations" with capital O, ensure proper casing
        if transformation_layer.lower() == "operations":
            transformation_layer = "Operations"
            
        workspace_users = row["workspace_default_groups"]
        deployment_code = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        spn_access_token = get_spn_access_token(tenant_id, client_id, client_secret)

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

if __name__ == "__main__":
    try:
        # Preprocess the secret values to replace single quotes with double quotes
        spn_secret_json_value = spn.replace("'", '"')
        
        # Attempt to parse the SPN secret JSON string into a Python dictionary
        key_vault_spn_secrets = json.loads(spn_secret_json_value)

        # Extract individual values from the parsed SPN secrets dictionary
        tenant_id = key_vault_spn_secrets["tenant_id"]
        client_id = key_vault_spn_secrets["client_id"]
        client_secret = key_vault_spn_secrets["client_secret"]

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
>>>>>>> 82a58ba12f802a0bb4c50f80e4abe098adbe9448
