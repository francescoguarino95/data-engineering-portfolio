-- 02) Modello dati CORE (sanitario) - tabelle principali
-- Creazione del modello dati centralizzato per la gestione dei pazienti
-- Il modello è normalizzato per evitare ridondanza e
-- garantire integrità logica tra pazienti, ricoveri,
-- diagnosi, procedure e dati IoT

USE ROLE ACCOUNTADMIN;
USE DATABASE HEALTHDATAPRO_DB;
USE SCHEMA CORE;

-- Tabella anagrafica pazienti
CREATE OR REPLACE TABLE PATIENTS (
  PATIENT_ID          STRING,
  FISCAL_CODE         STRING,          -- dato sensibile (PII)
  FIRST_NAME          STRING,          -- PII
  LAST_NAME           STRING,          -- PII
  BIRTH_DATE          DATE,
  SEX                 STRING,
  EMAIL               STRING,          -- PII
  PHONE               STRING,          -- PII
  ADDRESS             STRING,          -- PII
  CITY                STRING,
  GDPR_CONSENT        BOOLEAN,
  CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (PATIENT_ID)
);

CREATE OR REPLACE TABLE DEPARTMENTS ( 
  DEPARTMENT_ID       STRING,
  DEPARTMENT_NAME     STRING,
  PRIMARY KEY (DEPARTMENT_ID)
);

CREATE OR REPLACE TABLE STAFF (
  STAFF_ID            STRING,
  FULL_NAME           STRING,
  ROLE_TYPE           STRING,
  DEPARTMENT_ID       STRING,
  EMAIL               STRING,
  PRIMARY KEY (STAFF_ID)
);

-- Tabella dei ricoveri/visite
CREATE OR REPLACE TABLE ENCOUNTERS (
  ENCOUNTER_ID        STRING,
  PATIENT_ID          STRING,
  DEPARTMENT_ID       STRING,
  ADMIT_TS            TIMESTAMP_NTZ,
  DISCHARGE_TS        TIMESTAMP_NTZ,
  ENCOUNTER_TYPE      STRING,          
  OUTCOME             STRING,
  PRIMARY KEY (ENCOUNTER_ID)
);

CREATE OR REPLACE TABLE DIAGNOSES (
  DIAGNOSIS_ID         STRING,
  ENCOUNTER_ID         STRING,
  ICD10_CODE           STRING,
  DIAGNOSIS_DESC       STRING,
  DIAGNOSIS_TS         TIMESTAMP_NTZ,
  PRIMARY KEY (DIAGNOSIS_ID)
);

CREATE OR REPLACE TABLE PROCEDURES (
  PROCEDURE_ID         STRING,
  ENCOUNTER_ID         STRING,
  PROCEDURE_CODE       STRING,
  PROCEDURE_DESC       STRING,
  PROCEDURE_TS         TIMESTAMP_NTZ,
  PRIMARY KEY (PROCEDURE_ID)
);

CREATE OR REPLACE TABLE LAB_RESULTS (
  LAB_ID               STRING,
  ENCOUNTER_ID         STRING,
  TEST_NAME            STRING,
  RESULT_VALUE         STRING,
  UNIT                 STRING,
  RESULT_TS            TIMESTAMP_NTZ,
  PRIMARY KEY (LAB_ID)
);

CREATE OR REPLACE TABLE MEDICATIONS (
  MED_ID               STRING,
  ENCOUNTER_ID         STRING,
  DRUG_NAME            STRING,
  DOSE                 STRING,
  START_TS             TIMESTAMP_NTZ,
  END_TS               TIMESTAMP_NTZ,
  PRIMARY KEY (MED_ID)
);

-- Tabella misurazioni provenienti da sensori IOT
CREATE OR REPLACE TABLE VITALS_IOT (
  VITAL_ID             STRING,
  PATIENT_ID           STRING,
  MEASURE_TS           TIMESTAMP_NTZ,
  HEART_RATE           NUMBER,
  SPO2                 NUMBER,
  SYSTOLIC_BP          NUMBER,
  DIASTOLIC_BP         NUMBER,
  TEMPERATURE_C        FLOAT,
  PRIMARY KEY (VITAL_ID)
);