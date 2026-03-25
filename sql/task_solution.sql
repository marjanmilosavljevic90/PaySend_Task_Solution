--There is a table dwh.transaction_details, that contains financial transactions. One payment (pay_id) could be have one or more financial transitions (flow_num).
--
--Task is for each payment (pay_id) there should output in one line:
--1. Payment date according to the first flow (txn_date)
--2. Return flag (if for payment there could be found txn_type = RETURN_TO_CARD)
--3. Sender contract (first fr_prv_bal_id)
--4. Receiver contracts (last to_prv_bal_id). Return transaction doesn’t determine the recipient contract.
--5. Sending amount and currency (fr_cur, fr_amt_base)
--6. Receiver’s amount and currency (to_cur, to_amt_base)

--------------------------------------------------
-- PreRequisites

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

-- Import CSV transaction_details.csv

select * from dwh.transaction_details td ;
--------------------------------------------------
-- Final query

with rank_transaction as -- Rank flows within each payment
(
	select td.pay_id
	      ,td.flow_num
	      ,td.fr_prv_bal_id
	      ,td.txn_date
      	  ,td.txn_type
      	  ,td.to_prv_bal_id
      	  ,td.to_amt_base
      	  ,td.to_cur
      	  ,td.fr_cur
      	  ,td.fr_amt_base
	      ,row_number() over (          
      			partition by td.pay_id 
      			order by td.flow_num
      	   ) as rank_txn -- Rank transaction flows within each payment so we can identify first flow
	      ,row_number() over (
	      		partition by td.pay_id 
	      		order by
	      			case
	      				when td.txn_type <> 'RETURN_TO_CARD' then td.flow_num
	      				else null
	      			end desc nulls last			
	  	   ) as rn_last_non_return -- Rank non-return flows within each payment in reverse order and put null returns as last
	  	  ,MAX (
	  	  	case 
	  	  		when td.txn_type = 'RETURN_TO_CARD' then 1
	  	  		else 0	
	  	  	end
	  	  ) over (partition by td.pay_id) as return_flag -- Mark payments that have at least one return transaction
	from dwh.transaction_details td 
),
first_flow as -- Take the first flow with required data
(
	select rt.pay_id
	      ,rt.txn_date
	      ,rt.return_flag
	      ,rt.fr_prv_bal_id
	      ,rt.fr_cur
	      ,rt.fr_amt_base
	from rank_transaction rt
	where rt.rank_txn = 1
),
last_non_return_flow as -- Take the last not-return flow with required data
(
	select rt.pay_id
	      ,to_prv_bal_id
	      ,to_amt_base
	      ,to_cur
	from rank_transaction rt
	where rn_last_non_return = 1
)
select ff.pay_id
      ,ff.txn_date as payment_date
      ,ff.return_flag
      ,ff.fr_prv_bal_id as sender_contract
      ,lnrf.to_prv_bal_id as receiver_contract
      ,ff.fr_cur as sending_currency
      ,ff.fr_amt_base as sending_amount
  	  ,lnrf.to_amt_base as receiver_amount
      ,lnrf.to_cur as receiver_currency
from first_flow ff
left join last_non_return_flow lnrf
	on ff.pay_id = lnrf.pay_id
order by ff.pay_id;