



from surrealdb import AsyncSurreal
from datetime import datetime
from typing import List
from .models import AccountHeader, TransactionIn
from utils import create_upload_id


class UploadRepo:
    def __init__(self, db: AsyncSurreal):
        self.db = db

    async def save_user_upload(self, user_id: str, account_header: AccountHeader, transactions: List[TransactionIn], s3_url: str, upload_id: str = create_upload_id()) -> str:
      """
      Atomic bulk insert of account and transactions in a single round-trip.
      """
      opening_minor, closing_minor = account_header.to_minor_units()
      now_iso = datetime.utcnow().isoformat()

      account = {
        "name": account_header.account_name,
        "number": account_header.account_number,
        "opening_minor": opening_minor,
        "closing_minor": closing_minor,
        "currency": account_header.currency,
        "s3_raw_url": s3_url,
        "owner": user_id,
        "created_at": now_iso,
      }

      txns = []
      for t in transactions:
        row = t.to_db_row()
        row["created_at"] = now_iso
        row["upload_id"] = upload_id
        txns.append(row)

      query = (
        "BEGIN TRANSACTION;\n"
        "LET $acc = (CREATE account CONTENT $account);\n"
        "LET $rows = array::map($txns, function($t) { $t.account = type::thing($acc.id); return $t; });\n"
        "INSERT INTO transaction $rows;\n"
        "COMMIT TRANSACTION;"
      )
      vars = {
        "account": account,
        "txns": txns
      }

      res = await self.db.query(query, vars)
      # The driver returns an array of results; the second statement ($acc) result is at index 1
      acc = res[1][0]
      return acc["id"]