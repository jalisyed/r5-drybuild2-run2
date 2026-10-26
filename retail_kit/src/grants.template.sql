-- Run after every pipeline rerun (CREATE OR REPLACE drops grants). The app's service principal needs read access.
-- Replace <app-service-principal> with the application id shown on the app's Authorization tab.
GRANT USE CATALOG ON CATALOG workspace TO `<app-service-principal>`;
GRANT USE SCHEMA ON SCHEMA workspace.r5_retail TO `<app-service-principal>`;
GRANT SELECT ON SCHEMA workspace.r5_retail TO `<app-service-principal>`;
