import streamlit as st
import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import hashlib
from deepdiff import DeepDiff

# MongoDB Connection
@st.cache_resource
def get_db():
    client = MongoClient(st.secrets["MONGO_URI"])
    return client["StudyDB"]["Studycollection"]

collection = get_db()

# Hash generator for change tracking
def generate_hash(row):
    return hashlib.md5(str(row.to_dict()).encode()).hexdigest()

# UI
st.title("📊 Study Uploader with Versioning, Diff, and Rollback")

menu = st.sidebar.selectbox("Choose Action", ["Upload Excel", "View History & Rollback"])

# Upload Excel and insert/update with versioning
if menu == "Upload Excel":
    uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

    if uploaded_file:
        df = pd.read_excel(uploaded_file)

        if "StudyID" not in df.columns:
            st.error("Excel must contain a 'StudyID' column.")
        else:
            updates = []
            for _, row in df.iterrows():
                study_id = row["StudyID"]
                study_data = row.dropna().to_dict()
                new_hash = generate_hash(row)

                existing = collection.find_one(
                    {"StudyID": study_id}, sort=[("version", -1)]
                )

                if existing:
                    existing_hash = existing.get("hash")
                    if existing_hash != new_hash:
                        new_version = existing["version"] + 1
                        study_data.update({
                            "StudyID": study_id,
                            "version": new_version,
                            "timestamp": datetime.utcnow(),
                            "hash": new_hash,
                            "diff": DeepDiff(existing, study_data, ignore_order=True).to_dict()
                        })
                        updates.append(UpdateOne(
                            {"StudyID": study_id, "version": new_version},
                            {"$set": study_data},
                            upsert=True
                        ))
                else:
                    study_data.update({
                        "StudyID": study_id,
                        "version": 1,
                        "timestamp": datetime.utcnow(),
                        "hash": new_hash,
                        "diff": {}
                    })
                    updates.append(UpdateOne(
                        {"StudyID": study_id, "version": 1},
                        {"$set": study_data},
                        upsert=True
                    ))

            if updates:
                result = collection.bulk_write(updates)
                st.success(f"{result.upserted_count + result.modified_count} records updated/inserted.")
            else:
                st.info("No changes detected.")

# View history and rollback
elif menu == "View History & Rollback":
    study_ids = collection.distinct("StudyID")
    selected_id = st.selectbox("Select StudyID", study_ids)

    if selected_id:
        versions = list(collection.find({"StudyID": selected_id}).sort("version", -1))

        if versions:
            for record in versions:
                st.markdown(f"---\n### Version {record['version']} - {record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                st.json(record, expanded=False)

                if record.get("diff"):
                    with st.expander("🧬 View Changes from Previous Version"):
                        st.json(record["diff"])

            # Rollback
            rollback_to = st.selectbox("Rollback to version:", [v["version"] for v in versions])
            if st.button("Rollback"):
                current = collection.find_one({"StudyID": selected_id}, sort=[("version", -1)])
                target = collection.find_one({"StudyID": selected_id, "version": rollback_to})

                if target and current["version"] != target["version"]:
                    rollback_data = target.copy()
                    rollback_data.pop("_id")
                    rollback_data["version"] = current["version"] + 1
                    rollback_data["timestamp"] = datetime.utcnow()
                    rollback_data["hash"] = generate_hash(pd.Series(rollback_data))
                    rollback_data["diff"] = DeepDiff(current, rollback_data, ignore_order=True).to_dict()

                    collection.insert_one(rollback_data)
                    st.success(f"Rolled back to version {rollback_to}. New version {rollback_data['version']} created.")
                else:
                    st.warning("Selected version is already the latest.")
