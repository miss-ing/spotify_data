import sqlalchemy as db

def get_engine(db_path): 
    return db.create_engine(f"sqlite:///{db_path}")

