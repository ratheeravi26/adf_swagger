Run On Another Desktop

Clone & Python: Pull the repo onto the new machine and make sure Python 3.11+ is installed (PyCharm → File → Settings → Project Interpreter → add 3.11 if needed).
Create virtualenv: In PyCharm’s terminal run python -m venv .venv (or use PyCharm’s “Add Interpreter → Virtualenv”). Activate it inside PyCharm.
Install deps (once the venv is active):
pip install fastapi uvicorn azure-identity azure-mgmt-datafactory msal python-dotenv requests httpx
(You can save these into requirements.txt later for convenience.)
Environment: Copy the .env file you committed and update it with the target Azure subscription/factory credentials. For mock mode keep AUTH_USE_MOCK=true and ADF_MOCK_MODE=true; otherwise supply real secrets and set those to false.
Run configuration: In PyCharm create a new “Python” run config with:
Module name: uvicorn  
Parameters: adf_control_api.main:app --host 0.0.0.0 --port 8000  
Working dir: <project root>  
Environment vars: (PyCharm → Edit Config → add from `.env` or point to env file)
Launch: Hit “Run” in PyCharm. Visit http://localhost:8000/ and http://localhost:8000/docs. Use Authorize to paste a bearer token (any value in mock mode, real token when live).
If you plan to deploy beyond localhost, open the firewall/port as required and provide valid Azure credentials.

