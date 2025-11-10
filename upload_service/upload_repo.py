from __future__ import annotations
import datetime
from typing import List, Dict, Any, Optional
from surrealdb import AsyncSurreal
from upload_service.models import AccountHeader, TransactionIn
import logging
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UploadRepo:
    """Handles saving parsed financial statement data efficiently using bulk create."""
    def __init__(self, db: AsyncSurreal):
        self.db = db

    def _convert_to_minor(self, amount: Optional[float], factor: int = 100) -> Optional[int]:
        """Converts float amount (major units) to int (minor units)."""
        if amount is None:
            return None
        return int(round(amount * factor))

    def _parse_datetime(self, time_str: Optional[str]) -> Optional[str]:
        """Attempts to parse known date/time formats and return ISO format."""
        if not time_str:
            return None
        formats_to_try = [
            "%Y %b %d %H:%M:%S", # "2025 May 04 14:56:09"
            "%d %b %Y",         # "04 May 2025" (Value Date fallback)
        ]
        for fmt in formats_to_try:
            try:
                dt = datetime.datetime.strptime(time_str, fmt)
                return dt.isoformat() + "Z" # Add Z for UTC timezone indication
            except ValueError:
                continue
        logger.warning(f"Warning: Could not parse date/time string: {time_str}")
        return None # Return None if parsing fails
    
    async def save_user_upload(
        self,
        user_id: str,
        account_header: AccountHeader,
        transactions: List[TransactionIn],
        s3_url: str,
        upload_id: str # Pass a unique ID for this upload batch
    ) -> str:
        """Saves account and transactions using a single bulk create query within a transaction."""
        logger.info(f"Saving user upload for user: {user_id}, s3_url: {s3_url}, upload_id: {upload_id} - Status: In Progress")
        account_id = None # Initialize account_id
        account_data = {
                "name": account_header.account_name,
                "number": account_header.account_number,
                # Convert balances to minor units (int) for storage
                "opening_balance": self._convert_to_minor(account_header.opening_balance),
                "closing_balance": self._convert_to_minor(account_header.closing_balance),
                "s3_raw_url": s3_url,
                "owner": user_id,
                "created_at": datetime.datetime.utcnow().isoformat() + "Z"
            }

        transactions_list_for_db: List[Dict[str, Any]] = []
        for txn in transactions:
            # Combine debit/credit into a single amount_minor field
            amount = (txn.credit or 0.0) - (txn.debit or 0.0)
            amount_minor = self._convert_to_minor(amount)

            debit = self._convert_to_minor(txn.debit)
            credit = self._convert_to_minor(txn.credit)
            balance = self._convert_to_minor(txn.balance)

            # Parse date/time strings into ISO format for SurrealDB datetime type
            parsed_trans_time = self._parse_datetime(txn.trans_time)
            parsed_value_date = self._parse_datetime(txn.value_date)

            # Compute idempotency key and deterministic id
            idem_raw = f"{upload_id}|{parsed_trans_time}|{txn.description}|{amount_minor}"
            idem_key = hashlib.sha256(idem_raw.encode("utf-8")).hexdigest()
            txn_dict = {
                "id": f"transaction:{idem_key[:24]}",
                "trans_time": parsed_trans_time,
                "value_date": parsed_value_date,
                "description": txn.description,
                "amount_minor": amount_minor,
                "debit": debit,
                "credit": credit,
                "balance": balance,
                "upload_id": upload_id, # Link back to the specific upload
                "idempotency_key": idem_key,
                "created_at": datetime.datetime.utcnow().isoformat() + "Z"
                # Add category, counterparty etc. if available in Transaction model
            }
            transactions_list_for_db.append(txn_dict)
        try:
            
            query = """
            BEGIN TRANSACTION;
                LET $acc =(CREATE accounts CONTENT $account_data);
                LET $acc_id = $acc[0].id;
                LET $all_txns = (INSERT INTO transactions $transactions_list_for_db);
                RETURN $acc_id;
                LET $txn_ids = (SELECT VALUE id FROM $all_txns);

                RELATE $user_id->owns->$acc_id;
                RELATE $acc_id-> has->$txn_ids;

            COMMIT TRANSACTION;
            """
            vars = {
                "account_data": account_data,
                "user_id": user_id,
                "transactions_list_for_db": transactions_list_for_db
            }
            result = await self.db.query(query, vars)
            if isinstance(result, str):
                logger.error(f"Database error: {result}")
                raise Exception(f"Database error: {result}")
            logger.info(f"Successfully saved data to database")
            return result
        except Exception as e:
            logger.error(f"Error saving user upload: {e}")
            raise e
