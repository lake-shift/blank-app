import streamlit as st
import requests
from streamlit_autorefresh import st_autorefresh

# --- Secrets ---
host = st.secrets["HOST"]
token = st.secrets["TOKEN"]
job_id = st.secrets["JOB_ID"]
sender_email = st.secrets["SE"]
receiver_email = st.secrets["RE"]
api_key = st.secrets["AK"]
valid_username = st.secrets["USERNAME"]
valid_password = st.secrets["PASSWORD"]

# --- Session state ---
if "run_id" not in st.session_state:
    st.session_state.run_id = None
if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None
if "job_done" not in st.session_state:
    st.session_state.job_done = False
if "job_outputs" not in st.session_state:
    st.session_state.job_outputs = {}  
if "show_form" not in st.session_state:
    st.session_state.show_form = False
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False  # Track login state

# --- UI Header ---
st.markdown("""
    <h1 style="
        text-align: center;
        font-size: 56px;
        background: linear-gradient(90deg, #4facfe, #00f2fe);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: bold;
        text-shadow: 2px 2px 8px rgba(0,0,0,0.2);
        margin-bottom: 20px;
    ">
        LakeShift
    </h1>
""", unsafe_allow_html=True)

st.markdown(
    "<h4 style='text-align:center; color:gray;'>Migrate SAP HANA Calculation Views to Databricks in one click 🚀</h4>",
    unsafe_allow_html=True
)

# --- File Uploader ---
uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

# --- Start Button ---
if uploaded_file is not None and st.button("🚀 Start"):
    # If not logged in, ask for credentials
    if not st.session_state.authenticated:
        with st.form("auth_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit_auth = st.form_submit_button("Login")

            if submit_auth:
                if (username.lower() == valid_username.lower()) and (password == valid_password):
                    st.session_state.authenticated = True
                    st.success("✅ Login successful! Please click Start again to continue.")
                else:
                    st.error("❌ Invalid username or password")
    else:
        # Already authenticated → continue with upload + job
        file_bytes = uploaded_file.read()
        volume_path = f"/Volumes/project1/project1/project1/{uploaded_file.name}"

        # Upload file to Databricks
        url = f"{host}/api/2.0/fs/files{volume_path}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"}
        response = requests.put(url, headers=headers, data=file_bytes)

        if response.status_code in [200, 201, 204]:
            st.success("✅ File uploaded")

            # Trigger Databricks Job
            run_url = f"{host}/api/2.1/jobs/run-now"
            payload = {"job_id": job_id, "notebook_params": {"xml_input": volume_path}}
            run_response = requests.post(run_url, headers={"Authorization": f"Bearer {token}"}, json=payload)

            if run_response.status_code == 200:
                run_id = run_response.json().get("run_id")
                st.session_state.run_id = run_id
                st.session_state.uploaded_file_name = uploaded_file.name
                st.session_state.job_done = False
                st.session_state.job_outputs = {}
                st.success(f"🚀 LakeShift Unique Id: {run_id}")
            else:
                st.error(f"❌ Job failed: {run_response.status_code} - {run_response.text}")
        else:
            st.error(f"❌ Upload failed: {response.status_code} - {response.text}")

# --- Polling with auto-refresh ---
if st.session_state.run_id and not st.session_state.job_done:
    st_autorefresh(interval=10000, key="databricks_status")

    status_url = f"{host}/api/2.1/jobs/runs/get?run_id={st.session_state.run_id}"
    status_resp = requests.get(status_url, headers={"Authorization": f"Bearer {token}"})
    state = status_resp.json().get("state", {})
    life_cycle = state.get("life_cycle_state")
    result_state = state.get("result_state")

    if life_cycle == "TERMINATED":
        if result_state == "SUCCESS":
            st.success("✅ Job completed successfully!")
        else:
            st.error(f"❌ Job failed with state: {result_state}")
        st.session_state.job_done = True
    else:
        st.info(f"⏳ Job : {life_cycle}")
