CREATE DATABASE LSST;
CREATE DATABASE LSST_Shared;
CREATE TABLE LSST.Object_314159 (
    objectId BIGINT,
    subchunkId INT);
INSERT INTO LSST.Object_314159 VALUES
    (1, 42), (2, 42), (3, 42), (4, 42),
    (5, 99), (6, 42), (7, 99), (8, 99);
