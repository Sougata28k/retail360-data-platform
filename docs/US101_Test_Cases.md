#US101 Test Cases

## TC001

Scenario:
Valid Sales file

Expected:
Bronze Load Successful

Result:
Pass

---------------------------

## TC002

Scenario:
Missing Transaction Id

Expected:
Data should be in quarantine

Result:
Pass

---------------------------

## TC003

Scenario:
Audit Validation

Expected:
File updates should be in the Audit table

Result:
Pass

---------------------------

## TC004

Scenario:
Negative Quantity

Expected:
Negative quantity should go into the quarantine table

Result:
Pass

---------------------------

## TC005

Scenario:
Duplicate files/ Idempotency

Expected:
Ingestion should be skipped

Result:
Pass