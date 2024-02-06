import psycopg

class Database:
    def __init__(self, database_name, host, port, user, password=None):
        self.conn = psycopg.connect(f"user={user} password={password} host={host} port={port} dbname={database_name}")
        self.c = self.conn.cursor()

    def insert_patient(self, patient_id, birth_date, sex, status, consent):
        self.c.execute(
            f"INSERT INTO patient (id, birth_date, sex, status, consent) VALUES (%s, %s, %s, %s, %s)",
            (
                patient_id,
                birth_date,
                sex,
                status,
                consent
            ),
        )
        self.conn.commit()

    def insert_tissue(self, sample_id,  patient_id, biopsy_id, predictive_id, samples_no, available_samples_no,
                       material_type_id, diagnosis, ptnm, morphology, cut_time, freeze_time, retrieved):
        self.c.execute(
            f"INSERT INTO tissue (sample_id, patient_id, biopsy_id, predictive_id, samples_no, available_samples_no, material_type_id, diagnosis, ptnm, morphology, cut_time, freeze_time, retrieved) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                sample_id,
                patient_id,
                biopsy_id,
                predictive_id,
                samples_no,
                available_samples_no,
                material_type_id,
                diagnosis,
                ptnm,
                morphology,
                cut_time,
                freeze_time,
                retrieved
            ),
        )

    def insert_serum(self, sample_id,  patient_id, biopsy_id, predictive_id, samples_no, available_samples_no,
                    material_type_id, diagnosis, taking_date):
        self.c.execute(
            f"INSERT INTO serum (sample_id, patient_id, biopsy_id, predictive_id, samples_no, available_samples_no, material_type_id, diagnosis,taking_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                sample_id,
                patient_id,
                biopsy_id,
                predictive_id,
                samples_no,
                available_samples_no,
                material_type_id,
                diagnosis,
                taking_date,
            ),
        )

    def insert_genome(self, sample_id,  patient_id, biopsy_id, predictive_id, samples_no, available_samples_no,
                    material_type_id, retrieved, taking_date):
        self.c.execute(
            f"INSERT INTO genome (sample_id, patient_id, biopsy_id, predictive_id, samples_no, available_samples_no, material_type_id, retrieved, taking_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                sample_id,
                patient_id,
                biopsy_id,
                predictive_id,
                samples_no,
                available_samples_no,
                material_type_id,
                retrieved,
                taking_date,
            ),
        )

    def insert_cell(self, sample_id,  patient_id, biopsy_id, predictive_id, samples_no, available_samples_no,
                    material_type_id):
        self.c.execute(
            f"INSERT INTO cell (sample_id, patient_id, biopsy_id, predictive_id, samples_no, available_samples_no, material_type_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                sample_id,
                patient_id,
                biopsy_id,
                predictive_id,
                samples_no,
                available_samples_no,
                material_type_id,
            ),
        )

    def insert_diagnosis_material(self, sample_id,  patient_id, material_type_id, taking_date, diagnosis, retrieved):
        self.c.execute(
            f"INSERT INTO diagnosis_material (sample_id, patient_id, material_type_id, taking_date, diagnosis, retrieved) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                sample_id,
                patient_id,
                material_type_id,
                taking_date,
                diagnosis,
                retrieved,
            ),
        )

    def get_samples_with_pred_id(self, predictive_id):
        data = []
        self.c.execute(
            f"SELECT sample_id, patient_id, biopsy_id, predictive_id, material_type_id, diagnosis, cut_time, freeze_time FROM tissue WHERE predictive_id = %s",
            (
                predictive_id,
            ),
        )
        for row in self.c.fetchall():
            data.append(
                {
                    "sample_id": row[0],
                    "patient_id": row[1],
                    "biopsy_id": row[2],
                    "predictive_id": row[3],
                    "material_type_id": row[4],
                    "diagnosis": row[5],
                    "cut_time": row[6],
                    "freeze_time": row[7]
                }
            )
        self.c.execute(
            f"SELECT sample_id, patient_id, biopsy_id, predictive_id, material_type_id, diagnosis, taking_date FROM serum WHERE predictive_id = %s",
            (
                predictive_id,
            ),
        )
        for row in self.c.fetchall():
            data.append(
                {
                    "sample_id": row[0],
                    "patient_id": row[1],
                    "biopsy_id": row[2],
                    "predictive_id": row[3],
                    "material_type_id": row[4],
                    "diagnosis": row[5],
                    "sampling_timestamp": row[6],
                    "registration_timestamp": row[6]
                }
            )
        self.c.execute(
            f"SELECT sample_id, patient_id, biopsy_id, predictive_id, material_type_id, taking_date FROM genome WHERE predictive_id = %s",
            (
                predictive_id,
            ),
        )
        for row in self.c.fetchall():
            data.append(
                {
                    "sample_id": row[0],
                    "patient_id": row[1],
                    "biopsy_id": row[2],
                    "predictive_id": row[3],
                    "material_type_id": row[4],
                    "diagnosis": None,
                    "sampling_timestamp": row[5],
                    "registration_timestamp": row[5]
                }
            )
        self.c.execute(
            f"SELECT sample_id, patient_id, biopsy_id, predictive_id, material_type_id FROM cell WHERE predictive_id = %s",
            (
                predictive_id,
            ),
        )
        for row in self.c.fetchall():
            data.append(
                {
                    "sample_id": row[0],
                    "patient_id": row[1],
                    "biopsy_id": row[2],
                    "predictive_id": row[3],
                    "material_type_id": row[4],
                    "diagnosis": None,
                    "sampling_timestamp": None,
                    "registration_timestamp": None,
                }
            )

        return data
