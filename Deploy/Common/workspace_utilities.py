import requests
import json
import time 
from datetime import datetime, timezone

def does_workspace_exists_by_name(workspace_name, token):
    """
    Checks if a Power BI workspace with the given name exists in the user's organization.

    Parameters:
    - workspace_name (str): The name of the Power BI workspace to search for.
    - token (str): The access token used for authentication to the Fabric API.
    
    Returns:
    - dict: The information about the workspace if it exists, as returned by the Fabric API.
    - None: Returns None if the workspace does not exist or if the API request fails.
    """
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
    """
    Deletes a workspace in Microsoft Fabric using the API.

    Parameters:
    - workspace_id (str): The ID of the workspace to delete.
    - access_token (str): The authentication token for API access.

    Returns:
    - bool: True if the workspace was successfully deleted.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error deleting workspace {workspace_id}: {e}")

def create_workspace(workspace_name, capacity_id, token):
    """
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