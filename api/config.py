from sqlalchemy import create_engine


def get_connection():
    server = '192.168.0.250\ss2008r2'
    database = 'rsudtasikmalaya'
    username = 'sa'
    password = 'j4s4medik4'

    return create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}')