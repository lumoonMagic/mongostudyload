import streamlit as st
import pandas as pd
from pymongo import MongoClient, UpdateOne
from deepdiff import DeepDiff
import hashlib
import datetime
import json

# --- Mongo Connection ---
client = MongoClient(st.secrets["MONGO_URI"])
db = client["StudyDB"]
collection = db["Studycollection"]

# --- Utility Functions ---

def compute_hash(doc):
    doc_str = json.dumps(doc, default=str, sort_keys=True)
    return hashlib.md5(doc_str.encode()).hexdigest()

def serialize_for_mongo(record):
    for k, v in record.items():
        if isinstance(v, pd.Timestamp):
            record[k] = v.to_pydatetime()
    return record

def load_existing_studies():
    return {doc["StudyID"]: doc for doc in collection.find({}, {"_id": 0})}

def rollback_study(study_id, version):
    history_doc = collection.find_one({"StudyID": study_id, "version": version})
    if history_doc:
        collection.insert_one(history_doc)
        st.success(f"Rolled back {study_id} to version {version}")
    else:
        st.error("Version not found for rollback.")

# --- UI ---

st.title("📊 Study Loader & Version Manager")

uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])
if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write("Preview of Uploaded Data", df)

    existing_studies = load_existing_studies()

    updates = []
    inserts = []
    logs = []

    for _, row in df.iterrows():
        study = row.to_dict()
        study = serialize_for_mongo(study)
        study_id = study.get("StudyID")

        if not study_id:
            st.warning("Missing StudyID in row, skipping.")
            continue

        new_hash = compute_hash(study)

        if study_id in existing_studies:
            old_doc = existing_studies[study_id]
            old_hash = old_doc.get("hash")

            if new_hash != old_hash:
                version = old_doc.get("version", 1) + 1
                diff = DeepDiff(old_doc, study, ignore_order=True).to_dict()

                updated_doc = {
                    "$set": {
                        **study,
                        "version": version,
                        "timestamp": datetime.datetime.utcnow(),
                        "hash": new_hash,
                        "diff": diff,
                    }
                }

                updates.append(UpdateOne(
                    {"StudyID": study_id, "version": version},
                    updated_doc,
                    upsert=True
                ))

                logs.append(f"🔄 Updated {study_id} to version {version}")
            else:
                logs.append(f"⏭️ Skipped {study_id} (no changes)")
        else:
            study["version"] = 1
            study["timestamp"] = datetime.datetime.utcnow()
            study["hash"] = new_hash
            inserts.append(study)
            logs.append(f"🆕 Inserted new study {study_id}")

    if inserts:
        collection.insert_many(inserts)
    if updates:
        collection.bulk_write(updates)

    st.success("✅ Sync complete")
    st.code("\n".join(logs))

# --- Rollback Interface ---

with st.expander("⏪ Rollback to a previous version"):
    rollback_id = st.text_input("StudyID for rollback")
    rollback_ver = st.number_input("Version to rollback to", min_value=1, step=1)
    if st.button("Rollback"):
        rollback_study(rollback_id, rollback_ver)
