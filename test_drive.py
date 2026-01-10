import sys
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

if len(sys.argv) != 3:
    print("Usage: python3 test_drive.py YOUR_FOLDER_ID YOUR_EMAIL@gmail.com")
    sys.exit(1)

FOLDER_ID = sys.argv[1]
OWNER_EMAIL = sys.argv[2]
CREDENTIALS_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

print("Attempting to authenticate with Google using credentials.json...")
try:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    print("✅ Authentication successful.")
except Exception as e:
    print(f"❌ Authentication failed: {e}")
    sys.exit(1)

file_metadata = {
    'name': 'Vibey Bot Test File',
    'mimeType': 'application/vnd.google-apps.spreadsheet',
    'parents': [FOLDER_ID]
}

print(f"Attempting to create a test file in folder: {FOLDER_ID}...")
try:
    file = drive_service.files().create(body=file_metadata, fields='id').execute()
    print("\n--- ✅✅✅ TEST PASSED ✅✅✅ ---")
    print(f"File created with ID: {file.get('id')}")
    print("This means your credentials and project setup are PERFECT.")
    print("The bug IS in my main bot code. Please show this success message to the AI.")
    print("---------------------------------")
except Exception as e:
    print("\n--- ❌❌❌ TEST FAILED ❌❌❌ ---")
    print("This confirms the issue is with the Google project setup for THESE credentials.")
    print(f"ERROR DETAILS: {e}")
    print("---------------------------------")
