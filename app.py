import streamlit as st
import requests
import json
from streamlit_autorefresh import st_autorefresh


host = st.secrets["HOST"]
token = st.secrets["TOKEN"]
job_id = st.secrets["JOB_ID"]

# --- Session state ---
if "run_id" not in st.session_state:
    st.session_state.run_id = None
if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None
if "job_done" not in st.session_state:
    st.session_state.job_done = False
if "job_outputs" not in st.session_state:
    st.session_state.job_outputs = {}   # task_key -> (output, filename)


# --- UI Header ---
st.markdown(
    """
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
    """,
    unsafe_allow_html=True
)

st.markdown(
    "<h4 style='text-align:center; color:gray;'>Migrate SAP HANA Calculation Views to Databricks in Seconds 🚀</h4>",
    unsafe_allow_html=True
)

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

# --- Start Button ---
if uploaded_file is not None and st.button("🚀 Start"):
    file_bytes = uploaded_file.read()
    volume_path = f"/Volumes/project1/project1/project1/{uploaded_file.name}"

    url = f"{host}/api/2.0/fs/files{volume_path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }
    response = requests.put(url, headers=headers, data=file_bytes)

    if response.status_code in [200, 201, 204]:
        st.success("✅ File uploaded")

        # Trigger the Databricks Job
        run_url = f"{host}/api/2.1/jobs/run-now"
        payload = {"job_id": job_id, "notebook_params": {"xml_input": volume_path}}
        run_response = requests.post(
            run_url,
            headers={"Authorization": f"Bearer {token}"},
            json=payload
        )

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

        # --- Fetch task outputs ---
        tasks = status_resp.json().get("tasks", [])
        if tasks:
            for task in tasks:
                task_run_id = task.get("run_id")
                task_key = task.get("task_key")

                output_url = f"{host}/api/2.1/jobs/runs/get-output"
                output_resp = requests.get(
                    output_url,
                    headers={"Authorization": f"Bearer {token}"},
                    params={"run_id": task_run_id}
                )

                if output_resp.status_code == 200:
                    task_output = output_resp.json().get("notebook_output", {}).get("result", "")
                    if task_output:
                        out_filename = st.session_state.uploaded_file_name.rsplit(".", 1)[0] + f"_{task_key}_cnv.py"
                        st.session_state.job_outputs[task_key] = (task_output, out_filename)
                    else:
                        st.warning(f"⚠️ No notebook output found for task {task_key}")
                else:
                    st.error(f"❌ Could not fetch output for task {task_key}: {output_resp.text}")
        else:
            st.warning("⚠️ No tasks found in job run response.")
    else:
        st.info(f"⏳ Job : {life_cycle}")


# --- Persist Download Buttons ---
if st.session_state.job_outputs:
    st.subheader("📥 Download Results")
    for task_key, (task_output, filename) in st.session_state.job_outputs.items():
        st.download_button(
            label=f"Download {filename}",
            data=task_output,
            file_name=filename,
            mime="text/plain",
            key=f"dl_{task_key}"
        )
