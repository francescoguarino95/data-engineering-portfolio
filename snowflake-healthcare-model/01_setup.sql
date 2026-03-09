-- 01) Setup ambiente progetto (DB + schemi)
-- Creazione dell’ambiente Snowflake dedicato al progetto
-- Isoliamo il sistema ospedaliero in un database
-- separato per garantire ordine, scalabilità e sicurezza

-- Utilizzo il ruolo ACCOUNTADMIN per avere tutti i privilegi necessari
USE ROLE ACCOUNTADMIN;

-- Creo un database dedicato esclusivamente al progetto
-- Questo evita di mescolare oggetti con altri ambienti
CREATE OR REPLACE DATABASE HEALTHDATAPRO_DB;

-- Creo gli schemi logici che rappresentano i layer architetturali:
-- RAW: area di staging per dati provenienti da sistemi legacy
-- CORE: modello dati centralizzato e normalizzato
-- ANALYTICS: viste e oggetti per dashboard e reporting
-- SECURITY: oggetti di sicurezza
CREATE OR REPLACE SCHEMA HEALTHDATAPRO_DB.RAW;
CREATE OR REPLACE SCHEMA HEALTHDATAPRO_DB.CORE;
CREATE OR REPLACE SCHEMA HEALTHDATAPRO_DB.ANALYTICS;
CREATE OR REPLACE SCHEMA HEALTHDATAPRO_DB.SECURITY;

-- Imposto database e schema di lavoro
USE DATABASE HEALTHDATAPRO_DB;
USE SCHEMA CORE;