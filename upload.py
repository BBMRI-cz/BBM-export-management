import requests
import os
import xml.etree.ElementTree as ET
from database import Database

xml_prefix = "{http://www.bbmri.cz/schemas/biobank/data}"

material_type_to_id = {
    "1":0,
    "2":1,
    "3":2,
    "4":3,
    "5":4,
    "53":5,
    "54":6,
    "55":7,
    "56":8,
    "7":9,
    "gD":10,
    "K":11,
    "L":12,
    "PD":13,
    "PK":14,
    "PR":15,
    "S":16,
    "SD":17,
    "T":18,
}

def read_exports(path, db):
    for file in os.listdir(path):
        try:
            tree = ET.parse(os.path.join(path, file))
            read_xml_export(tree.getroot(), db)
        except ET.ParseError:
            print("Can't open XML - ", os.path.join(path, file))
            continue


def read_xml_export(root: ET.ElementTree, db: Database):
    patient_id = root.get("id")
    birth_date = f'1.{root.get("month").replace("-", "")}.{root.get("year")}'
    sex = root.get("sex")
    consent = root.get("consent")
    db.insert_patient(patient_id, birth_date, sex, None, consent)
    lts = root.find(f"{xml_prefix}LTS")
    lts = lts if lts is not None else []
    for child in lts:
        if "tissue" in child.tag:
            db.insert_tissue(
                child.get("sampleId"),
                patient_id,
                child.get("biopsy"),
                child.get("predictive_number"),
                child.find(f"{xml_prefix}samplesNo").text,
                child.find(f"{xml_prefix}availableSamplesNo").text,
                material_type_to_id[child.find(f"{xml_prefix}materialType").text],
                child.find(f"{xml_prefix}diagnosis").text,
                child.find(f"{xml_prefix}pTNM").text,
                child.find(f"{xml_prefix}morphology").text,
                child.find(f"{xml_prefix}cutTime").text,
                child.find(f"{xml_prefix}freezeTime").text,
                child.find(f"{xml_prefix}retrieved").text,
                )
        if "genome" in child.tag:
            db.insert_genome(
                child.get("sampleId"),
                patient_id,
                child.get("biopsy"),
                child.get("predictive_number"),
                child.find(f"{xml_prefix}samplesNo").text,
                child.find(f"{xml_prefix}availableSamplesNo").text,
                material_type_to_id[child.find(f"{xml_prefix}materialType").text],
                child.find(f"{xml_prefix}retrieved").text,
                child.find(f"{xml_prefix}takingDate").text,
                )
        if "serum" in child.tag:
            db.insert_serum(
                child.get("sampleId"),
                patient_id,
                child.get("biopsy"),
                child.get("predictive_number"),
                child.find(f"{xml_prefix}samplesNo").text,
                child.find(f"{xml_prefix}availableSamplesNo").text,
                material_type_to_id[child.find(f"{xml_prefix}materialType").text],
                child.find(f"{xml_prefix}diagnosis").text,
                child.find(f"{xml_prefix}takingDate").text,
            )
        if "cell" in child.tag:
            db.insert_cell(
                child.get("sampleId"),
                patient_id,
                child.get("biopsy"),
                child.get("predictive_number"),
                child.find(f"{xml_prefix}samplesNo").text,
                child.find(f"{xml_prefix}availableSamplesNo").text,
                material_type_to_id[child.find(f"{xml_prefix}materialType").text],
            )
    sts = root.find(f"{xml_prefix}STS")
    sts = sts if sts is not None else []
    for child in sts:
        if "diagnosisMaterial" in child.tag:
            db.insert_diagnosis_material(
                child.get("sampleId"),
                patient_id,
                material_type_to_id[child.find(f"{xml_prefix}materialType").text],
                child.find(f"{xml_prefix}takingDate").text,
                child.find(f"{xml_prefix}diagnosis").text,
                child.find(f"{xml_prefix}retrieved").text,
            )

if __name__ == "__main__":
    db = Database(
        os.environ["PSQL_NAME"],
        os.environ["PSQL_HOST"],
        os.environ["PSQL_PORT"],
        os.environ["PSQL_USER"],
        os.environ["PSQL_PSSWD"]
    )
    path = os.environ["BBM_EXPORT_PATH"]
    read_exports(path, db)
