from sqlalchemy import create_engine


def get_connection():
    server = '192.168.0.250\ss2022'
    database = 'rsud_tasikmalaya'
    username = 'sa'
    password = 'j4s4medik4'

    return create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}')