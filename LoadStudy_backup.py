import streamlit as st
import pandas as pd
import hashlib
import json
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from deepdiff import DeepDiff

# --- MongoDB Connection ---
client = MongoClient(st.secrets["MONGO_URI"])
db = client["StudyDB"]
collection = db["Studycollection"]

# --- Helpers ---
def compute_hash(doc):
    doc_str = json.dumps(doc, sort_keys=True, default=str)
    return hashlib.md5(doc_str.encode()).hexdigest()

def get_latest_versions():
    pipeline = [
        {"$sort": {"version": -1}},
        {
            "$group": {
                "_id": "$StudyID",
                "doc": {"$first": "$$ROOT"}
            }
        },
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]
    return list(collection.aggregate(pipeline))

def save_version(doc):
    version_doc = doc.copy()
    version_doc.pop("_id", None)
    collection.insert_one(version_doc)

def rollback_to_version(study_id, version):
    target = collection.find_one({"StudyID": study_id, "version": version})
    if target:
        target.pop("_id", None)
        new_version = max([d["version"] for d in collection.find({"StudyID": study_id})]) + 1
        target["version"] = new_version
        target["timestamp"] = datetime.utcnow()
        target["hash"] = compute_hash(target)
        target["diff"] = {}
        target["diff_log"] = json.dumps({})
        collection.insert_one(target)
        st.success(f"Rolled back {study_id} to version {version} as version {new_version}.")

# --- Streamlit App ---
st.title("📊 Study Loader with Version Control")

uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    updates, inserts = [], []
    logs = []

    for _, row in df.iterrows():
        new_doc = row.dropna().to_dict()

        # Ensure datetime fields are converted
        for k in ["StartDate", "EndDate"]:
            if k in new_doc and not isinstance(new_doc[k], datetime):
                new_doc[k] = pd.to_datetime(new_doc[k]).to_pydatetime()

        new_doc["StudyID"] = str(new_doc["StudyID"]).strip()
        existing_doc = collection.find_one({"StudyID": new_doc["StudyID"]}, sort=[("version", -1)])

        if existing_doc:
            version = existing_doc["version"] + 1
            raw_diff = DeepDiff(existing_doc, new_doc, ignore_order=True)
            clean_diff = json.loads(raw_diff.to_json())
            diff_log = raw_diff.to_json()

            doc_hash = compute_hash(new_doc)

            update_doc = {
                "$set": {
                    **new_doc,
                    "version": version,
                    "timestamp": datetime.utcnow(),
                    "hash": doc_hash,
                    "diff": clean_diff,
                    "diff_log": diff_log
                }
            }

            updates.append(
                UpdateOne({"StudyID": new_doc["StudyID"], "version": version}, update_doc, upsert=True)
            )
            logs.append(f"Updated {new_doc['StudyID']} to version {version}")
        else:
            new_doc["version"] = 1
            new_doc["timestamp"] = datetime.utcnow()
            new_doc["hash"] = compute_hash(new_doc)
            new_doc["diff"] = {}
            new_doc["diff_log"] = json.dumps({})
            inserts.append(new_doc)
            logs.append(f"Inserted new study {new_doc['StudyID']} version 1")

    if inserts:
        collection.insert_many(inserts)
    if updates:
        collection.bulk_write(updates)

    st.success("Upload completed")
    st.write("Logs:", logs)

st.subheader("📜 Study Version Viewer")
study_ids = collection.distinct("StudyID")
selected = st.selectbox("Select StudyID", study_ids)

if selected:
    versions = list(collection.find({"StudyID": selected}).sort("version", 1))
    st.dataframe(pd.DataFrame(versions).drop(columns=["_id"]))

    export_format = st.radio("Download format", ["CSV", "JSON"])
    if st.button("Download Versions"):
        export_df = pd.DataFrame(versions).drop(columns=["_id"])
        if export_format == "CSV":
            st.download_button("Download CSV", export_df.to_csv(index=False), file_name=f"{selected}_versions.csv")
        else:
            st.download_button("Download JSON", export_df.to_json(orient="records", indent=2), file_name=f"{selected}_versions.json")

    rollback_version = st.number_input("Rollback to version", min_value=1, step=1)
    if st.button("Rollback"):
        rollback_to_version(selected, rollback_version)
