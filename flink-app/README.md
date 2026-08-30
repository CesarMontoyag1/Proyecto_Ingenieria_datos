# NexaSecure WIDS - Flink Application

This subproject contains the PyFlink job that reads from the AWID3 Kinesis Data Stream, validates schemas, pseudonymizes MAC addresses, and routes records to either a Trusted bucket or a Quarantine bucket.

This application is designed to run on an Amazon EMR cluster (to be deployed in Sprint 2).

In Sprint 1, testing is done locally using PyTest.

