import requests
import time
from datetime import datetime, timezone
import random # Import for jitter

def create_environment(workspace_id, access_token, display_name, description):
    """
    Creates an environment in a given workspace in Microsoft Fabric.

    Parameters:
        workspace_id (str): The ID of the workspace.
        access_token (str): The service principal access token for authentication.
        display_name (str): The display name of the environment.
        description (str): A description for the environment.

    Returns:
        str: The ID of the created environment.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "displayName": display_name,
        "description": description
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get("id", "").strip()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error occurred while creating environment: {str(e)}")


def publish_environment(workspace_id, artifact_id, access_token):
    """
    Publishes the environment after uploading libraries.

    Parameters:
        workspace_id (str): The ID of the workspace.
        artifact_id (str): The ID of the environment artifact.
        access_token (str): The service principal access token for authentication.

    Returns:
        dict: The response from the API confirming the publish operation.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{artifact_id}/staging/publish"

    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error publishing environment: {str(e)}")


def poll_environment_publish_status(workspace_id, artifact_id, access_token, initial_poll_interval=15, max_poll_interval=300, maximum_duration=1200):
    """
    Polls the environment publish status using exponential backoff with jitter.
    Stops polling as soon as the status changes from 'Running' or when the maximum duration is exceeded.

    Parameters:
        workspace_id (str): The ID of the workspace.
        artifact_id (str): The ID of the environment.
        access_token (str): The service principal access token for authentication.
        initial_poll_interval (int): Initial time in seconds between polls (default: 15 seconds).
        max_poll_interval (int): Maximum time in seconds between polls (default: 300 seconds).
        maximum_duration (int): Maximum duration in seconds to poll (default: 1200 seconds).

    Returns:
        str: The publish state once it is no longer 'Running'.

    Raises:
        Exception: If the maximum polling duration is exceeded without a state change.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{artifact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    elapsed_time = 0
    current_poll_interval = initial_poll_interval

    while elapsed_time < maximum_duration:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()

            # Extract the current publish state from the API response
            current_state = result.get("properties", {}).get("publishDetails", {}).get("state", None)

            print(f"Polling environment publish status for {artifact_id}. Current state: {current_state}")

            # Stop polling if the state is not 'Running'
            if current_state != "Running":
                return current_state

        except requests.exceptions.RequestException as e:
            print(f"Error getting environment publish status, retrying: {str(e)}")
            # Do not re-raise immediately; allow polling to continue for transient errors

        # Apply exponential backoff with jitter
        sleep_duration = min(current_poll_interval + random.uniform(0, current_poll_interval * 0.2), max_poll_interval)
        print(f"Waiting for {sleep_duration:.2f} seconds before next poll...")
        time.sleep(sleep_duration)
        elapsed_time += sleep_duration
        current_poll_interval = min(current_poll_interval * 2, max_poll_interval) # Double for exponential backoff

    raise Exception(f"Maximum polling duration ({maximum_duration} seconds) exceeded for environment {artifact_id} without status change. Last state: {current_state}")


def update_default_environment(workspace_id, access_token, environment_name, runtime_version):
    """
    Updates the default environment settings in a workspace.

    Parameters:
        workspace_id (str): The ID of the workspace.
        access_token (str): The service principal access token for authentication.
        environment_name (str): The name of the environment.
        runtime_version (str): The runtime version to set.

    Returns:
        dict: The response from the API confirming the update.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/settings"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "environment": {
            "name": environment_name,
            "runtimeVersion": runtime_version
        }
    }

    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error updating default environment: {str(e)}")


def deploy_custom_environment(workspace_id, access_token):
    """
    Deploys a custom environment to a workspace, including creating the environment,
    uploading a library, publishing the environment, and setting it as the default environment.

    Parameters:
    - workspace_id (str): The ID of the workspace.
    - access_token (str): The service principal access token to authenticate with the workspace.

    Raises:
        Exception: If an error occurs during any of the deployment steps, an exception is raised.
    """
    try:
        env_name = "Spark_Environment"
        
        # Create the environment
        print(f"Creating custom environment '{env_name}' in workspace '{workspace_id}'...")
        artifact_id = create_environment(workspace_id, access_token, env_name, None)
        print(f"Environment '{env_name}' created with ID: {artifact_id}.")

        # Publish the environment
        print(f"Publishing environment '{env_name}'...")
        publish_environment(workspace_id, artifact_id, access_token)
        print(f"Publish request for environment '{env_name}' sent.")

        # Check publish status
        print(f"Polling publish status for environment '{env_name}'...")
        publish_status = poll_environment_publish_status(workspace_id, artifact_id, access_token)

        if publish_status and publish_status.lower() == "success":
            print(f"Environment '{env_name}' published successfully. Setting as default...")
            # Set the environment as the default
            update_default_environment(workspace_id, access_token, env_name, "1.3")
            print(f"Environment '{env_name}' set as default successfully.")
        else:
            raise Exception(f"Error in publishing environment '{env_name}'. Final status: {publish_status}")
    except Exception as e:
        error_message = f"Error occurred while deploying custom environment: {str(e)}"
        raise Exception(error_message)