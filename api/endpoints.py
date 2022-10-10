from collections import Counter
from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request
from sqlalchemy import text

from api.config import get_connection


realtime_bp = Blueprint('realtime', __name__)
engine = get_connection()


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
        cnt[data[i][param].lower().replace(' ', '_')] += 1
    return cnt


@realtime_bp.route('/ketersediaan_bed')
def ketersediaan_bed():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT DISTINCT vjbpr.Kelas, vjbpr.JmlBed
            FROM dbo.V_JmlBedPerRuangan vjbpr;"""))
    data = []
    for row in result:
        data.append({
            "kelas": row['Kelas'],
            "jumlah_bed": row['JmlBed']
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['kelas'].lower().replace(' ', '_')] += data[i]['jumlah_bed']
    result = {
        "judul": 'Ketersediaan Bed',
        "label": 'Live Reports',
        "tersedia": cnt,
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/tren_pelayanan')
def tren_pelayanan():
    return jsonify({"message": "ini data tren pelayanan"})


@realtime_bp.route('/absensi_pegawai')
def absensi_pegawai():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT evap.Tanggal, evap.Keterangan
            FROM dbo.EIS_ViewAbsensiPegawai evap
            WHERE evap.Tanggal >= '{tgl_awal}'
            AND evap.Tanggal < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY evap.Tanggal ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['Tanggal'],
            "status_absensi": row['Keterangan'].split("\r")[0]
        })
    result = {
        "judul": 'Absensi Pegawai',
        "label": 'Live Reports',
        "status_absensi": count_values(data, 'status_absensi'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/pelayanan_instalasi')
def pelayanan_instalasi():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT bp.TglPelayanan, i.NamaInstalasi
            FROM dbo.BiayaPelayanan bp
            INNER JOIN dbo.Ruangan r
            ON bp.KdRuangan = r.KdRuangan
            INNER JOIN dbo.Instalasi i
            ON r.KdInstalasi = i.KdInstalasi
            WHERE bp.TglPelayanan >= '{tgl_awal}'
            AND bp.TglPelayanan < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY bp.TglPelayanan ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglPelayanan'],
            "instalasi": row['NamaInstalasi'].split("\r")[0]
        })
    result = {
        "judul": 'Pelayanan Instalasi',
        "label": 'Live Reports',
        "instalasi": count_values(data, 'instalasi'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/asal_rujukan')
def asal_rujukan():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(f"""SELECT DISTINCT bp.NoPendaftaran, ra.RujukanAsal
            FROM rsudtasikmalaya.dbo.BiayaPelayanan bp
            INNER JOIN rsudtasikmalaya.dbo.Rujukan r
            ON bp.NoPendaftaran = r.NoPendaftaran
            INNER JOIN rsudtasikmalaya.dbo.RujukanAsal ra
            ON r.KdRujukanAsal = ra.KdRujukanAsal
            WHERE bp.TglPelayanan >= '{tgl_awal}'
            AND bp.TglPelayanan < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY bp.NoPendaftaran ASC;"""))
    data = []
    for row in result:
        data.append({
            # "tanggal": row['TglPelayanan'],
            "rujukan": row['RujukanAsal']
        })
    result = {
        "judul": 'Rujukan Asal Pasien',
        "label": 'Live Reports',
        "rujukan": count_values(data, 'rujukan'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)
    

@realtime_bp.route('/kelompok_pasien')
def kelompok_pasien():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT DISTINCT bp.NoPendaftaran, pd.TglPendaftaran, kp.JenisPasien as KelompokPasien
            FROM dbo.BiayaPelayanan bp
            INNER JOIN dbo.PasienDaftar pd
            ON bp.NoPendaftaran = pd.NoPendaftaran
            INNER JOIN dbo.KelompokPasien kp
            ON pd.KdKelasAkhir = kp.KdKelompokPasien
            WHERE bp.TglPelayanan >= '{tgl_awal}' 
            AND bp.TglPelayanan < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY pd.TglPendaftaran ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglPendaftaran'],
            "kelompok": row['KelompokPasien']
        })
    result = {
        "judul": 'Kelompok Pasien',
        "label": 'Live Reports',
        "kelompok": count_values(data, 'kelompok'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/pasien_usia_gender')
def pasien_usia_gender():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(f"""SELECT DISTINCT bp.NoPendaftaran, pd.TglPendaftaran, p.TglLahir, p.JenisKelamin
            FROM dbo.BiayaPelayanan bp
            INNER JOIN dbo.PasienDaftar pd
            ON bp.NoPendaftaran = pd.NoPendaftaran 
            INNER JOIN dbo.Pasien p
            ON pd.NoCM = p.NoCM 
            WHERE bp.TglPelayanan >= '{tgl_awal}' 
            AND bp.TglPelayanan < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY bp.NoPendaftaran ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglPendaftaran'],
            "umur": get_categorical_age(row['TglLahir']),
            "jenis_kelamin": row['JenisKelamin']
        })
    result = {
        "judul": 'Umur dan Jenis Kelamin',
        "label": 'Live Reports',
        "jenis_kelamin": count_values(data, 'jenis_kelamin'),
        "umur": count_values(data, 'umur'),
        "tgl_filter": {
            "tgl_awal": tgl_awal,
            "tgl_akhir": tgl_akhir
        }
    }
    return jsonify(result)


@realtime_bp.route('/pendapatan_jenis_produk')
def pendapatan_jenis_produk():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT DISTINCT evprsna.TanggalPelayanan, evprsna.Tarif, jp.Deskripsi
            FROM dbo.EIS_ViewPendapatanRumahSakitNewAll evprsna
            INNER JOIN dbo.ListPelayananRS lpr 
            ON evprsna.KdPelayananRS = lpr.KdPelayananRS 
            INNER JOIN dbo.JenisPelayanan jp 
            ON lpr.KdJnsPelayanan = jp.KdJnsPelayanan 
            WHERE evprsna.TanggalPelayanan >= '{tgl_awal}'
            AND evprsna.TanggalPelayanan < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY evprsna.TanggalPelayanan ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TanggalPelayanan'],
            "jenis_pelayanan": row['Deskripsi'],
            "total": row['Tarif']
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['jenis_pelayanan'].lower().replace(
            ' ', '_')] += float(data[i]['total'])
    result = {
        "judul": 'Pendapatan Jenis Produk',
        "label": 'Live Reports',
        "cara_bayar": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@realtime_bp.route('/pendapatan_instalasi')
def pendapatan_instalasi():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT spp.TglStruk, spp.NoPendaftaran, i.NamaInstalasi , spp.TotalBiaya
           FROM dbo.StrukPelayananPasien spp
           INNER JOIN dbo.Ruangan r
           ON spp.KdRuanganTerakhir = r.KdRuangan
           INNER JOIN dbo.Instalasi i
           ON r.KdInstalasi = i.KdInstalasi
           WHERE spp.TglStruk >= '{tgl_awal}'
           AND spp.TglStruk < '{tgl_akhir + timedelta(days=1)}'
           ORDER BY spp.TglStruk ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglStruk'],
            "instalasi": row['NamaInstalasi'],
            "total": row['TotalBiaya'],
            "judul": 'Pendapatan Instalasi',
            "label": 'Live Reports'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['instalasi'].lower().replace(
            ' ', '_')] += float(data[i]['total'])
    result = {
        "judul": 'Pendapatan Instalasi',
        "label": 'Live Reports',
        "instalasi": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@realtime_bp.route('/pendapatan_cara_bayar')
def pendapatan_cara_bayar():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT sbkm.TglBKM, cb.CaraBayar, sbkm.JmlBayar
            FROM dbo.StrukBuktiKasMasuk sbkm
            INNER JOIN dbo.CaraBayar cb
            ON sbkm.KdCaraBayar = cb.KdCaraBayar
            WHERE sbkm.TglBKM >= '{tgl_awal}'
            AND sbkm.TglBKM < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY sbkm.TglBKM ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglBKM'],
            "cara_bayar": row['CaraBayar'],
            "total": row['JmlBayar'],
            "judul": 'Pendapatan Cara Bayar',
            "label": 'Live Reports'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['cara_bayar'].lower().replace(
            ' ', '_')] += float(data[i]['total'])

    result = {
        "judul": 'Pendapatan Cara Bayar',
        "label": 'Live Reports',
        "cara_bayar": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@realtime_bp.route('/pendapatan_kelas')
def pendapatan_kelas():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT spp.TglStruk, spp.NoPendaftaran, kp.DeskKelas, spp.TotalBiaya
           FROM dbo.StrukPelayananPasien spp
           INNER JOIN dbo.PasienDaftar pd
           ON spp.NoPendaftaran = pd.NoPendaftaran
           INNER JOIN dbo.KelasPelayanan kp
           ON pd.KdKelasAkhir = kp.KdKelas
           WHERE spp.TglStruk >= '{tgl_awal}'
           AND spp.TglStruk < '{tgl_akhir + timedelta(days=1)}'
           ORDER BY spp.TglStruk ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglStruk'],
            "kelas": row['DeskKelas'],
            "total": row['TotalBiaya'],
            "judul": 'Pendapatan Kelas',
            "label": 'Live Reports'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['kelas'].lower().replace(
            ' ', '_')] += float(data[i]['total'])
    result = {
        "judul": 'Pendapatan Kelas',
        "label": 'Live Reports',
        "kelas": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)
