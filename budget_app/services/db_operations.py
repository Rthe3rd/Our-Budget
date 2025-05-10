from budget_app.init_db import get_db
# DB STUFF

class DBHandler():
    @classmethod
    def get_pulled_transactions(cls, username, transactions_from_upload):
        # get user_id from db that matches the current user_id in session
        db, cursor = cls.get_db_cursor()
        user_id = cls.get_user_id(cursor, username)
        # get all records with user id and after the earliest date
        from_date = transactions_from_upload['transaction_date'].min()
        cursor.execute(
            'SELECT unique_record_id FROM transactions WHERE user_id = %s AND transaction_date >= %s', (user_id, from_date)
        )
        db.commit()
        return cursor.fetchall()  

    @classmethod
    def get_user_id(cls, cursor, username):
        cursor.execute(
            'SELECT id FROM users WHERE username = %s', (username,)
        )
        return cursor.fetchone()[0]

    @classmethod
    def get_db_cursor(cls, ):
        db = get_db()
        return db, db.cursor()

    @classmethod
    def insert_transactions_into_db(cls, username, transactions_to_add_to_db):
        db, cursor = cls.get_db_cursor()
        user_id = cls.get_user_id(cursor, username)

        for transaction in transactions_to_add_to_db:
            cursor.execute(
                'INSERT INTO transactions VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, DEFAULT);', (user_id, *transaction,)
            )