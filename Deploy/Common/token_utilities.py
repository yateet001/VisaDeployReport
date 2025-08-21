import requests
import time
import random # For adding jitter
from azure.identity import UsernamePasswordCredential
from requests.exceptions import RequestException

def get_spn_access_token(tenant_id, client_id, client_secret, max_retries=5, initial_delay=1):
    """
    Retrieve an access token using a service principal with retry logic and exponential backoff.

    Parameters:
    - tenant_id (str): Tenant ID of the Azure Active Directory tenant.
    - client_id (str): The client ID of the service principal.
    - client_secret (str): The client secret of the service principal.
    - max_retries (int): Maximum number of retries for the API call.
    - initial_delay (int): Initial delay in seconds before the first retry.

    Returns:
    - str: The access token (Bearer token).

    Raises:
    - Exception: If fetching the access token fails after all retries.
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://api.fabric.microsoft.com/.default",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded" # Correct content type for form data
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            
            token_data = response.json()
            if "access_token" in token_data:
                print(f"Successfully retrieved SPN access token on attempt {attempt + 1}.")
                return token_data["access_token"]
            else:
                raise ValueError(f"Access token not found in response: {token_data}")

        except RequestException as e:
            error_details = response.text if response is not None else "No response body."
            print(f"Attempt {attempt + 1}/{max_retries} failed to get SPN access token: {e}. Details: {error_details}")
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.5 * initial_delay * (2 ** attempt)) # Exponential backoff with jitter
                print(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise Exception(f"Failed to get SPN access token after {max_retries} attempts. Last error: {e}. Details: {error_details}")
        except ValueError as e: # Catch ValueError for missing access_token in response
            print(f"Attempt {attempt + 1}/{max_retries} failed due to response content: {e}")
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.5 * initial_delay * (2 ** attempt))
                print(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise Exception(f"Failed to get SPN access token after {max_retries} attempts due to invalid response. Last error: {e}")


def get_upn_access_token(upn_client_id, upn_user_id, upn_password, max_retries=5, initial_delay=1):
    """
    Retrieve an access token using UPN (username/password) credentials with retry logic and exponential backoff.

    WARNING: The Resource Owner Password Credentials (ROPC) flow used here is generally discouraged
    due to security risks (e.g., phishing, incompatibility with MFA). Prefer more secure authentication
    methods like Authorization Code Flow with PKCE for user interaction, or Service Principals for automation.

    Parameters:
    - upn_client_id (str): The client ID of the application.
    - upn_user_id (str): The UPN (username) of the user.
    - upn_password (str): The password of the user.
    - max_retries (int): Maximum number of retries for the API call.
    - initial_delay (int): Initial delay in seconds before the first retry.

    Returns:
    - str: The access token (Bearer token).

    Raises:
    - Exception: If fetching the access token fails after all retries.
    """
    for attempt in range(max_retries):
        try:
            credential = UsernamePasswordCredential(
                client_id=upn_client_id,
                username=upn_user_id,
                password=upn_password
            )
            
            # Get the access token for Power BI API (can be adjusted for different APIs)
            token_response = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            token = token_response.token
            
            if token:
                print(f"Successfully retrieved UPN access token on attempt {attempt + 1}.")
                return token
            else:
                raise ValueError("Access token was None in the response.")

        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed to get UPN access token: {e}")
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.5 * initial_delay * (2 ** attempt)) # Exponential backoff with jitter
                print(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                raise Exception(f"Failed to get UPN access token after {max_retries} attempts. Last error: {e}")