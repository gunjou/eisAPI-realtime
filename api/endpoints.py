from collections import Counter
from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request

from api.query import *

realtime_bp = Blueprint('realtime', __name__)


def get_default_date(tgl_awal, tgl_akhir):
    if tgl_awal == None:
        tgl_awal = datetime.today() - relativedelta(months=1)
        tgl_awal = datetime.strptime(tgl_awal.strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_awal = datetime.strptime(tgl_awal, '%Y-%m-%d')

    if tgl_akhir == None:
        tgl_akhir = datetime.strptime(
            datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_akhir = datetime.strptime(tgl_akhir, '%Y-%m-%d')
    return tgl_awal, tgl_akhir


def get_categorical_age(birth_date):
    today = date.today()
    age = today.year - birth_date.year - ((today.month, today.day) <
                                          (birth_date.month, birth_date.day))
    category = '<5' if age < 5 else '5-14' if age >= 5 and age < 15 else '15-24' if age >= 15 and age <= 24 \
        else '25-34' if age >= 25 and age <= 34 else '35-44' if age >= 35 and age <= 44 else '45-54' if age >= 45 and age <= 54 \
        else '55-64' if age >= 55 and age <= 64 else '>65'
    return category


def count_values(data, param):
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i][param]] += 1
    return cnt


@realtime_bp.route('/realtime/ketersediaan_bed')
def ketersediaan_bed():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_ketersediaan_bed()
    
    # Extract data by date (dict)
    tmp = [{"kelas": row['Kelas'], "jumlah_bed": row['JmlBed']} for row in result]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'kelas')
    data = [{"name": x, "value": y} for x, y in cnts.items()]
    
    # Define return result as a json
    result = {
        "judul": 'Ketersediaan Bed',
        "label": 'Live Reports',
        "data": data,
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/realtime/tren_pelayanan')
def tren_pelayanan():
    return jsonify({"message": "ini data tren pelayanan"})


@realtime_bp.route('/realtime/absensi_pegawai')
def absensi_pegawai():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_absensi_pegawai(tgl_awal, tgl_akhir + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['Tanggal'], "status_absensi": row['Keterangan']} for row in result]
    
    # Extract data as (dataframe)
    cnts = count_values(tmp, 'status_absensi')
    data = [{"name": x, "value": y} for x, y in cnts.items()]

    # Define return result as a json    
    result = {
        "judul": 'Absensi Pegawai',
        "label": 'Live Reports',
        "data": data, #count_values(data, 'status_absensi'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/realtime/pelayanan_instalasi')
def pelayanan_instalasi():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pelayanan_instalasi(tgl_awal, tgl_akhir + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglPelayanan'], "instalasi": row['NamaInstalasi'].split("\r")[0]} for row in result]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'instalasi')
    data = [{"name": x, "value": y} for x, y in cnts.items()]
    
    # Define return result as a json
    result = {
        "judul": 'Pelayanan Instalasi',
        "label": 'Live Reports',
        "data": data, #count_values(data, 'instalasi'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/realtime/asal_rujukan')
def asal_rujukan():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_rujukan(tgl_awal, tgl_akhir + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglPendaftaran'], "rujukan": row['RujukanAsal']} for row in result]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'rujukan')
    data = [{"name": x, "value": y} for x, y in cnts.items()]

    # Define return result as a json
    result = {
        "judul": 'Rujukan Asal Pasien',
        "label": 'Live Reports',
        "data": data, #count_values(data, 'rujukan'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)
    

@realtime_bp.route('/realtime/kelompok_pasien')
def kelompok_pasien():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_kelompok_pasien(tgl_awal, tgl_akhir + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglPendaftaran'], "kelompok": row['KelompokPasien']} for row in result]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'kelompok')
    data = [{"name": x, "value": y} for x, y in cnts.items()]

    # Define return result as a json
    result = {
        "judul": 'Kelompok Pasien',
        "label": 'Live Reports',
        "data": data, #count_values(data, 'kelompok'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/realtime/pasien_usia_gender')
def pasien_usia_gender():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_umur_jenis_kelamin(tgl_awal, tgl_akhir + timedelta(days=1))
    
    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglPendaftaran'], "umur": get_categorical_age(row['TglLahir']), "jenis_kelamin": row['JenisKelamin']} for row in result]
    
    # Extract data as (dataframe)
    cnts = count_values(tmp, 'umur')
    data = []
    kategori_umur = [x for x, y in cnts.items()]
    for i in kategori_umur:
        p = 0
        l = 0
        for j in range(len(tmp)):
            if tmp[j]['umur'] == i and tmp[j]['jenis_kelamin'] == 'P':
                p += 1
            elif tmp[j]['umur'] == i and tmp[j]['jenis_kelamin'] == 'L':
                l += 1
            else:
                pass
        data.append({"name": i, "value": l+p, "laki_laki": l, "perempuan": p})

    result = {
        "judul": 'Umur dan Jenis Kelamin',
        "label": 'Kunjungan Pasien',
        "data": data,
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/realtime/pendapatan_jenis_produk')
def pendapatan_jenis_produk():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_produk(tgl_awal, tgl_akhir + timedelta(days=1))
    
    # Extract data by date (dict)
    tmp = [{"tanggal": row['TanggalPelayanan'], "jenis_pelayanan": row['Deskripsi'], "total": row['Tarif']} for row in result]

    # Extract data as (dataframe)
    cnts = Counter()
    for i in range(len(tmp)):
        cnts[tmp[i]['jenis_pelayanan']] += float(tmp[i]['total'])
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Jenis Produk',
        "label": 'Live Reports',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@realtime_bp.route('/realtime/pendapatan_instalasi')
def pendapatan_instalasi():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_instalasi(tgl_awal, tgl_akhir + timedelta(days=1))
    
    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglStruk'], "instalasi": row['NamaInstalasi'], "total": row['TotalBiaya']} for row in result]
    
    # Extract data as (dataframe)
    cnts = Counter()
    for i in range(len(tmp)):
        cnts[tmp[i]['instalasi']] += float(tmp[i]['total'])
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Instalasi',
        "label": 'Live Reports',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@realtime_bp.route('/realtime/pendapatan_cara_bayar')
def pendapatan_cara_bayar():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_cara_bayar(tgl_awal, tgl_akhir + timedelta(days=1))
    
    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglBKM'], "cara_bayar": row['CaraBayar'], "total": row['JmlBayar']} for row in result]
    
    # Extract data as (dataframe)
    cnts = Counter()
    for i in range(len(tmp)):
        cnts[tmp[i]['cara_bayar']] += float(tmp[i]['total'])
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Cara Bayar',
        "label": 'Live Reports',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@realtime_bp.route('/realtime/pendapatan_kelas')
def pendapatan_kelas():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_kelas(tgl_awal, tgl_akhir + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglStruk'], "kelas": row['DeskKelas'], "total": row['TotalBiaya']} for row in result]

    # Extract data as (dataframe)
    cnts = Counter()
    for i in range(len(tmp)):
        cnts[tmp[i]['kelas']] += float(tmp[i]['total'])
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Kelas',
        "label": 'Live Reports',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)
