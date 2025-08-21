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


def orchestrator(tenant_id, client_id, client_secret, connections_data):
    """
    Orchestrates the deployment of networks.

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


if __name__ == "__main__":
    try:
        # Extract individual values directly from environment variables
        tenant_id = os.getenv("tenant_id")
        client_id = os.getenv("client_id")
        client_secret = os.getenv("client_secret")

        # Validate that credentials are not None before proceeding
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("Authentication credentials (tenant_id, client_id, client_secret) must be provided as environment variables.")

        # Call the orchestrator function with the extracted values as arguments
        orchestrator(tenant_id, client_id, client_secret, connections_data)
    
    except Exception as e:
        # If an error occurs during the execution, raise the exception
        raise e