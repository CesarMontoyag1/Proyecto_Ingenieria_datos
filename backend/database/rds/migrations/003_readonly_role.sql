-- NexaSecure WIDS — RBAC sobre la capa transaccional
-- Implementa 2 de los 4 perfiles exigidos en la sección 3.2 (Componente 3)
-- del Sprint 0 ("RBAC con cuatro perfiles bajo mínimo privilegio"). Los
-- otros 2 perfiles (administrador de plataforma, ingeniero de datos) se
-- gestionan como roles de IAM sobre el resto de la arquitectura (S3, Glue,
-- Kinesis), documentados en DATA_GOVERNANCE.md -- no tiene sentido
-- replicarlos aquí porque no tocan RDS directamente.

BEGIN;

-- Perfil "Analista de Seguridad": puede leer alertas y casos, y cambiar el
-- estado de un caso (su trabajo diario), pero nunca ve la bitácora de
-- auditoría ni puede tocar el catálogo de reglas.
CREATE ROLE security_analyst LOGIN PASSWORD :'security_analyst_password';
GRANT CONNECT ON DATABASE nexasecure TO security_analyst;
GRANT USAGE ON SCHEMA public TO security_analyst;
GRANT SELECT ON alerts, rule_catalog TO security_analyst;
GRANT SELECT, INSERT, UPDATE (status, assigned_to, closed_at, resolution_notes)
    ON incident_cases TO security_analyst;
GRANT USAGE, SELECT ON SEQUENCE incident_cases_id_seq TO security_analyst;

-- Perfil "Auditor": acceso de solo lectura a TODO, incluida la bitácora de
-- auditoría -- es el único perfil, junto al admin, que puede verla. No
-- puede escribir nada, ni siquiera en incident_cases.
CREATE ROLE auditor LOGIN PASSWORD :'auditor_password';
GRANT CONNECT ON DATABASE nexasecure TO auditor;
GRANT USAGE ON SCHEMA public TO auditor;
GRANT SELECT ON alerts, incident_cases, rule_catalog, audit_log TO auditor;

-- Toda consulta hecha con cualquiera de los dos roles anteriores debe
-- quedar registrada. Esto se aplica a nivel de aplicación (el código que
-- abre la conexión inserta en audit_log antes de cada SELECT sensible),
-- no con triggers, para no acoplar la auditoría al motor de base de datos.

COMMIT;

-- Uso: psql -v security_analyst_password='...' -v auditor_password='...' \
--        -h <host> -U <master_user> -d nexasecure -f 003_readonly_role.sql
