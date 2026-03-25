# PaySend Task Solution

## Overview
PostgreSQL solution for aggregating transactions flows from 'dwh.transaction_details' into a single record per 'pay_id.

## Task

There is a table dwh.transaction_details, that contains financial transactions. One payment (pay_id) could be have one or more financial transitions (flow_num).

Task is for each payment (pay_id) there should output in one line:
1. Payment date according to the first flow (txn_date)
2. Return flag (if for payment there could be found txn_type = RETURN_TO_CARD)
3. Sender contract (first fr_prv_bal_id)
4. Receiver contracts (last to_prv_bal_id). Return transaction doesn’t determine the recipient contract.
5. Sending amount and currency (fr_cur, fr_amt_base)
6. Receiver’s amount and currency (to_cur, to_amt_base)

## PreRequisites
```sql
create schema if not exists dwh;

create table dwh.transaction_details (
    txn_id         BIGINT PRIMARY KEY,
    pay_id         BIGINT NOT NULL,
    txn_type       VARCHAR(50) NOT NULL,
    txn_date       TIMESTAMP NOT NULL,
    fr_cur         INTEGER,
    fr_amt_base    NUMERIC(18,3),
    fr_prv_bal_id  VARCHAR(50),
    to_cur         INTEGER,
    to_amt_base    NUMERIC(18,3),
    to_prv_bal_id  VARCHAR(50),
    flow_num       INTEGER NOT NULL
);
```

Import CSV [init/transaction_details.csv](init/transaction_details.csv)

## Solution

See [sql/postgres.sql](sql/task_solution.sql).

