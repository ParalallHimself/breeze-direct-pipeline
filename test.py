from breeze_connect import BreezeConnect # type: ignore
import requests
import json
import urllib.parse
# Initialize SDK
api_key = "lj431BJ113mY810116D492T771E893i2"
secret_key = "328Pp574644L41b0q5SJ832k05H6l86s"
api_session = "56371464"
breeze = BreezeConnect(api_key=api_key)

# 3. Helper to generate your daily login link
# If you don't have a session token yet, run this file once, copy this link, 
# log in via your browser, and grab the 'apisession=' value from the redirect URL.
print("--- DAILY LOGIN LINK ---")
print("https://api.icicidirect.com/apiuser/login?api_key=" + urllib.parse.quote_plus(api_key))
print("------------------------\n")

try:
    print("Attempting validation handshake with Breeze servers...")
    
    # 4. Generate the Session
    breeze.generate_session(api_secret=secret_key, session_token=api_session)
    
    # 5. Fetch Account Details to verify connectivity
    account_info = breeze.get_customer_details(api_session=api_session)
    
    # 6. The Proof of Success Output
    if account_info.get("Status") == 200 or "Success" in account_info:
        print("\n=========================================")
        print("🎉 SUCCESS: Handshake verified!")
        print(f"Connected to Client ID: {account_info.get('Success', {}).get('client_id', 'Active')}")
        print("=========================================")
    else:
        print("\n❌ Handshake failed. Server response:")
        print(account_info)
        
except Exception as e:
    print(f"\n❌ Connection error encountered: {e}")



url = "https://api.icicidirect.com/breezeapi/api/v1/customerdetails"

payload = json.dumps({
  "SessionToken": api_session,
  "AppKey": api_key
})
headers = {
  'Content-Type': 'application/json',
}
response = requests.request("GET", url, headers=headers, data=payload)
data = json.loads(response.text)
print(data)