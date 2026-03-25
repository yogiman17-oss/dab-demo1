# Databricks notebook source
# DBTITLE 1,Title and Overview
# MAGIC %md
# MAGIC # 🔐 Lab Demo: Databricks PAT & GitHub Token Integration
# MAGIC ## GitHub Account: `yogiman17-oss`
# MAGIC
# MAGIC This lab walks through:
# MAGIC 1. **Generating a Databricks Personal Access Token (PAT)** via the REST API
# MAGIC 2. **Creating a GitHub Personal Access Token** for the `yogiman17-oss` account
# MAGIC 3. **Configuring Git credentials** in Databricks to link both tokens
# MAGIC 4. **Verifying the integration** by connecting to GitHub repos
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Databricks workspace access with token creation permissions
# MAGIC - GitHub account: [yogiman17-oss](https://github.com/yogiman17-oss)
# MAGIC - `requests` library (pre-installed on Databricks)

# COMMAND ----------

# DBTITLE 1,Step 1 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Setup — Retrieve Workspace Context
# MAGIC We start by capturing the current Databricks workspace URL and verifying our environment.

# COMMAND ----------

# DBTITLE 1,Retrieve Workspace Context
import requests
import json
import os

# Auto-detect Databricks workspace URL and current token from notebook context
notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
databricks_host = notebook_context.apiUrl().get()
current_token = notebook_context.apiToken().get()

print(f"✅ Databricks Workspace: {databricks_host}")
print(f"✅ Current Token (masked): {current_token[:5]}...{current_token[-4:]}")
print(f"✅ Environment ready!")

# COMMAND ----------

# DBTITLE 1,Step 2 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Generate a New Databricks Personal Access Token (PAT)
# MAGIC
# MAGIC We'll use the **Token Management API** (`/api/2.0/token/create`) to programmatically generate a new PAT.
# MAGIC
# MAGIC > ⚠️ **Security Note**: Store tokens securely using Databricks Secrets. Never hard-code tokens in notebooks.

# COMMAND ----------

# DBTITLE 1,Generate Databricks PAT
# --- Generate a new Databricks PAT ---
token_endpoint = f"{databricks_host}/api/2.0/token/create"

payload = {
    "comment": "Lab Demo - GitHub Integration Token",
    "lifetime_seconds": 86400 * 90  # 90-day lifetime
}

headers = {
    "Authorization": f"Bearer {current_token}",
    "Content-Type": "application/json"
}

response = requests.post(token_endpoint, headers=headers, json=payload)

if response.status_code == 200:
    token_data = response.json()
    new_databricks_pat = token_data["token_value"]
    token_info = token_data["token_info"]
    
    print("✅ New Databricks PAT Generated Successfully!")
    print(f"   Token ID    : {token_info['token_id']}")
    print(f"   Comment     : {token_info['comment']}")
    print(f"   Created     : {token_info['creation_time']}")
    print(f"   Expiry      : {token_info['expiry_time']}")
    print(f"   Token (masked): {new_databricks_pat[:5]}...{new_databricks_pat[-4:]}")
    print(f"\n⚠️  Save this token securely — it won't be shown again!")
else:
    print(f"❌ Failed to generate token: {response.status_code}")
    print(response.json())

# COMMAND ----------

# DBTITLE 1,Step 3 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: List Existing Databricks PATs
# MAGIC
# MAGIC View all active tokens in your workspace to manage and audit them.

# COMMAND ----------

# DBTITLE 1,List Existing Databricks PATs
# --- List existing Databricks tokens ---
list_endpoint = f"{databricks_host}/api/2.0/token/list"

response = requests.get(list_endpoint, headers=headers)

if response.status_code == 200:
    tokens = response.json().get("token_infos", [])
    print(f"📋 Found {len(tokens)} active token(s):\n")
    for t in tokens:
        print(f"   🔑 ID: {t['token_id']}")
        print(f"      Comment : {t.get('comment', 'N/A')}")
        print(f"      Created : {t['creation_time']}")
        print(f"      Expiry  : {t.get('expiry_time', 'Never')}")
        print()
else:
    print(f"❌ Failed to list tokens: {response.status_code}")

# COMMAND ----------

# DBTITLE 1,Step 4 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Create a GitHub Personal Access Token (PAT)
# MAGIC
# MAGIC To connect Databricks with **GitHub account `yogiman17-oss`**, you need a GitHub PAT.
# MAGIC
# MAGIC ### 📝 Instructions (Manual Steps in GitHub):
# MAGIC 1. Go to [GitHub → Settings → Developer Settings → Personal Access Tokens](https://github.com/settings/tokens)
# MAGIC 2. Click **"Generate new token (classic)"**
# MAGIC 3. Set a **Note**: `Databricks Integration - Lab Demo`
# MAGIC 4. Select scopes:
# MAGIC    - ✅ `repo` (Full control of private repositories)
# MAGIC    - ✅ `workflow` (if using GitHub Actions)
# MAGIC 5. Click **Generate token**
# MAGIC 6. **Copy the token** and paste it below
# MAGIC
# MAGIC > 🔗 Direct link: https://github.com/settings/tokens/new

# COMMAND ----------

# DBTITLE 1,Capture GitHub PAT Input
# --- Enter your GitHub PAT here ---
# Option 1: Use Databricks Secrets (RECOMMENDED for production)
# github_pat = dbutils.secrets.get(scope="github-secrets", key="github-pat")

# Option 2: Use widgets for interactive input (LAB DEMO)
dbutils.widgets.text("github_pat", "", "Enter GitHub PAT")
dbutils.widgets.text("github_email", "", "Enter GitHub Email")

github_pat = dbutils.widgets.get("github_pat")
github_email = dbutils.widgets.get("github_email")
github_username = "yogiman17-oss"

if github_pat:
    print(f"✅ GitHub PAT captured (masked): {github_pat[:4]}...{github_pat[-4:]}")
    print(f"✅ GitHub Username: {github_username}")
    print(f"✅ GitHub Email: {github_email}")
else:
    print("⚠️  Please enter your GitHub PAT in the widget above")
    print("   Follow the instructions in the markdown cell above to generate one")

# COMMAND ----------

# DBTITLE 1,Step 5 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Validate GitHub Token
# MAGIC
# MAGIC Verify the GitHub PAT works by calling the GitHub API.

# COMMAND ----------

# DBTITLE 1,Validate GitHub Token
# --- Validate GitHub PAT by fetching user info ---
if github_pat:
    github_api = "https://api.github.com/user"
    gh_headers = {
        "Authorization": f"token {github_pat}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    gh_response = requests.get(github_api, headers=gh_headers)
    
    if gh_response.status_code == 200:
        user_info = gh_response.json()
        print("✅ GitHub Token Validated Successfully!")
        print(f"   Login       : {user_info['login']}")
        print(f"   Name        : {user_info.get('name', 'N/A')}")
        print(f"   Public Repos: {user_info['public_repos']}")
        print(f"   Profile     : {user_info['html_url']}")
    else:
        print(f"❌ GitHub token validation failed: {gh_response.status_code}")
        print(gh_response.json())
else:
    print("⚠️  Skipped — no GitHub PAT provided")

# COMMAND ----------

# DBTITLE 1,List GitHub Repositories
# --- List repositories for yogiman17-oss ---
if github_pat:
    repos_api = f"https://api.github.com/users/{github_username}/repos"
    gh_response = requests.get(repos_api, headers=gh_headers)
    
    if gh_response.status_code == 200:
        repos = gh_response.json()
        print(f"📂 Repositories for {github_username} ({len(repos)} found):\n")
        for repo in repos[:10]:  # Show first 10
            visibility = "🔒 Private" if repo['private'] else "🌐 Public"
            print(f"   {visibility} | {repo['name']}")
            print(f"            URL: {repo['html_url']}")
            print(f"            Default Branch: {repo.get('default_branch', 'N/A')}")
            print()
        if len(repos) > 10:
            print(f"   ... and {len(repos) - 10} more repositories")
    else:
        print(f"❌ Failed to list repos: {gh_response.status_code}")
else:
    print("⚠️  Skipped — no GitHub PAT provided")

# COMMAND ----------

# DBTITLE 1,Step 6 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Configure Git Credentials in Databricks
# MAGIC
# MAGIC Link the GitHub PAT to Databricks so Git Folders can access `yogiman17-oss` repositories.

# COMMAND ----------

# DBTITLE 1,Configure Git Credentials
# --- Configure Git Credentials in Databricks ---
if github_pat and github_email:
    git_cred_endpoint = f"{databricks_host}/api/2.0/git-credentials"
    
    git_payload = {
        "personal_access_token": github_pat,
        "git_username": github_username,
        "git_provider": "gitHub"
    }
    
    response = requests.post(git_cred_endpoint, headers=headers, json=git_payload)
    
    if response.status_code == 200:
        cred_data = response.json()
        print("✅ Git Credentials Configured Successfully!")
        print(f"   Credential ID : {cred_data.get('credential_id', 'N/A')}")
        print(f"   Git Provider  : {cred_data.get('git_provider', 'N/A')}")
        print(f"   Git Username  : {cred_data.get('git_username', 'N/A')}")
        print(f"\n🎉 Databricks is now linked to GitHub account: {github_username}")
    else:
        print(f"⚠️  Response ({response.status_code}): {response.json()}")
        print("   If credentials already exist, use UPDATE (PATCH) instead of CREATE (POST)")
else:
    print("⚠️  Skipped — provide GitHub PAT and email in the widgets above")

# COMMAND ----------

# DBTITLE 1,Step 7 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Verify — List Configured Git Credentials

# COMMAND ----------

# DBTITLE 1,List Git Credentials
# --- List all configured Git credentials ---
git_cred_endpoint = f"{databricks_host}/api/2.0/git-credentials"

response = requests.get(git_cred_endpoint, headers=headers)

if response.status_code == 200:
    credentials = response.json().get("credentials", [])
    print(f"🔗 Configured Git Credentials ({len(credentials)} found):\n")
    for cred in credentials:
        print(f"   📌 Credential ID : {cred['credential_id']}")
        print(f"      Provider      : {cred['git_provider']}")
        print(f"      Username      : {cred.get('git_username', 'N/A')}")
        print()
else:
    print(f"❌ Failed to list credentials: {response.status_code}")

# COMMAND ----------

# DBTITLE 1,Step 8 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Store Tokens Securely with Databricks Secrets (Best Practice)
# MAGIC
# MAGIC > ⚠️ **Never store tokens in plain text.** Use Databricks Secrets for production workloads.

# COMMAND ----------

# DBTITLE 1,Secrets Best Practice Guide
# --- Best Practice: Store tokens in Databricks Secrets ---
# Run these commands via Databricks CLI or uncomment below:

print("📋 Recommended: Store tokens using Databricks Secrets\n")
print("   # Create a secret scope")
print('   databricks secrets create-scope github-secrets\n')
print("   # Store the GitHub PAT")
print('   databricks secrets put-secret github-secrets github-pat\n')
print("   # Store the Databricks PAT")
print('   databricks secrets put-secret github-secrets databricks-pat\n')
print("   # Retrieve in notebooks:")
print('   github_pat = dbutils.secrets.get(scope="github-secrets", key="github-pat")')
print('   db_pat = dbutils.secrets.get(scope="github-secrets", key="databricks-pat")')

# COMMAND ----------

# DBTITLE 1,Export Notebook & Push to GitHub dab-demo1
import subprocess, shutil, base64, os
from datetime import datetime

# --- Configuration ---
repo_name = "dab-demo1"
branch = "main"
notebook_path = "/Users/ymghorpade@gmail.com/M8. DAB/Lab Demo - Databricks PAT & GitHub Token Integration"
work_dir = "/tmp/git_push_lab"

if not github_pat:
    raise ValueError("GitHub PAT not provided. Please enter your PAT in the widget above.")

# --- Step A: Export the current notebook from Databricks workspace ---
print("📦 Step A: Exporting current notebook from Databricks...")
export_endpoint = f"{databricks_host}/api/2.0/workspace/export"
export_params = {
    "path": notebook_path,
    "format": "SOURCE"
}
export_resp = requests.get(export_endpoint, headers=headers, params=export_params)

if export_resp.status_code == 200:
    notebook_b64 = export_resp.json()["content"]
    notebook_content = base64.b64decode(notebook_b64).decode("utf-8")
    print(f"   ✅ Notebook exported ({len(notebook_content)} bytes)")
else:
    print(f"   ⚠️ Export API returned {export_resp.status_code}: {export_resp.text}")
    print("   Falling back to a demo notebook file...")
    notebook_content = """# Databricks notebook source\n# Lab Demo: Databricks PAT & GitHub Token Integration\n# GitHub Account: yogiman17-oss\nprint('Hello from Databricks!')\n"""

# --- Step B: Prepare /tmp working directory (workspace FS doesn't support .git) ---
print("\n📁 Step B: Preparing local git workspace in /tmp...")
if os.path.exists(work_dir):
    shutil.rmtree(work_dir)
os.makedirs(work_dir, exist_ok=True)

# --- Step C: Clone the repo, add notebook, commit, and push ---
print(f"\n🔗 Step C: Cloning {github_username}/{repo_name} and pushing notebook...")
repo_url = f"https://{github_username}:{github_pat}@github.com/{github_username}/{repo_name}.git"

try:
    def run_git(cmd, cwd=work_dir):
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"   ⚠️ {' '.join(cmd)}: {result.stderr.strip()}")
        return result

    # Clone the repo
    r = run_git(["git", "clone", repo_url, work_dir], cwd="/tmp")
    if r.returncode != 0 and "already exists" in r.stderr:
        run_git(["git", "pull", "origin", branch])
    
    # Configure git identity
    run_git(["git", "config", "user.email", github_email])
    run_git(["git", "config", "user.name", github_username])
    
    # Write the exported notebook
    nb_filename = "Lab_Demo_Databricks_PAT_GitHub_Integration.py"
    nb_path = os.path.join(work_dir, nb_filename)
    with open(nb_path, "w") as f:
        f.write(notebook_content)
    print(f"   ✅ Wrote {nb_filename} ({len(notebook_content)} bytes)")
    
    # Stage, commit, push with versioned commit message
    run_git(["git", "add", "."])
    
    version_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    commit_msg = f"Update lab notebook — v{version_ts}"
    commit_result = run_git(["git", "commit", "-m", commit_msg])
    
    if commit_result.returncode == 0:
        push_result = run_git(["git", "push", "origin", branch])
        if push_result.returncode == 0:
            # Fetch commit SHA for version tracking
            sha_result = run_git(["git", "rev-parse", "--short", "HEAD"])
            short_sha = sha_result.stdout.strip() if sha_result.returncode == 0 else "N/A"
            
            print(f"\n🎉 SUCCESS! Updated notebook pushed to GitHub!")
            print(f"   📂 Repo   : https://github.com/{github_username}/{repo_name}")
            print(f"   📄 File   : {nb_filename}")
            print(f"   🌿 Branch : {branch}")
            print(f"   🔖 Commit : {short_sha}")
            print(f"   🕐 Version: {version_ts}")
        else:
            print(f"\n❌ Push failed: {push_result.stderr}")
    elif "nothing to commit" in commit_result.stdout:
        print("\n✅ Nothing new to commit — repo is already up to date with this version.")
    else:
        print(f"\n❌ Commit failed: {commit_result.stderr}")

    # Show git log for version history
    print(f"\n📜 Version History (last 5 commits):")
    log_result = run_git(["git", "log", "--oneline", "-5"])
    if log_result.returncode == 0:
        for line in log_result.stdout.strip().split("\n"):
            print(f"   {line}")

except Exception as e:
    print(f"\n❌ Git operation failed: {e}")
    print("   Ensure your repo exists, your PAT has 'repo' scope, and you have push access.")
finally:
    # Clean up credentials from /tmp
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
        print("\n🧹 Cleaned up /tmp working directory")

# COMMAND ----------

# DBTITLE 1,Step 9 Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Cleanup — Revoke Lab Tokens (Optional)
# MAGIC
# MAGIC Remove the lab-generated tokens when no longer needed.

# COMMAND ----------

# DBTITLE 1,Cleanup Tokens
# --- Cleanup: Revoke the lab-generated Databricks PAT ---
# Uncomment below to revoke the token created in Step 2

# if 'token_info' in dir() and token_info:
#     revoke_endpoint = f"{databricks_host}/api/2.0/token/delete"
#     revoke_payload = {"token_id": token_info['token_id']}
#     response = requests.post(revoke_endpoint, headers=headers, json=revoke_payload)
#     if response.status_code == 200:
#         print(f"✅ Token {token_info['token_id']} revoked successfully")
#     else:
#         print(f"❌ Failed to revoke: {response.status_code}")

# --- Cleanup: Remove widgets ---
# dbutils.widgets.removeAll()

print("🧹 Cleanup section ready — uncomment code above to revoke tokens")

# COMMAND ----------

# DBTITLE 1,Lab Summary
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Lab Summary
# MAGIC
# MAGIC | Step | Action | API/Method |
# MAGIC |------|--------|------------|
# MAGIC | 1 | Workspace Context | `dbutils.notebook.entry_point` |
# MAGIC | 2 | Generate Databricks PAT | `POST /api/2.0/token/create` |
# MAGIC | 3 | List Databricks PATs | `GET /api/2.0/token/list` |
# MAGIC | 4 | Create GitHub PAT | GitHub Settings → Developer Settings |
# MAGIC | 5 | Validate GitHub Token | `GET https://api.github.com/user` |
# MAGIC | 6 | Configure Git Credentials | `POST /api/2.0/git-credentials` |
# MAGIC | 7 | List Git Credentials | `GET /api/2.0/git-credentials` |
# MAGIC | 8 | Secure Storage | Databricks Secrets |
# MAGIC | 9 | Cleanup | `POST /api/2.0/token/delete` |
# MAGIC
# MAGIC ### 🔗 Key References
# MAGIC - [Databricks PAT Docs](https://docs.databricks.com/dev-tools/auth/pat.html)
# MAGIC - [GitHub PAT Guide](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
# MAGIC - [Databricks Git Integration](https://docs.databricks.com/repos/repos-setup.html)
# MAGIC - [GitHub: yogiman17-oss](https://github.com/yogiman17-oss)