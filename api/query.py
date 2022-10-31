from sqlalchemy import text

from api.config import get_connection

engine = get_connection()


def query_ketersediaan_bed():
    result = engine.execute(
        text(f"""SELECT DISTINCT vjbpr.Kelas, vjbpr.JmlBed
            FROM dbo.V_JmlBedPerRuangan vjbpr;"""))
    return result


def query_absensi_pegawai(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT evap.Tanggal, evap.Keterangan
            FROM dbo.EIS_ViewAbsensiPegawai evap
            WHERE evap.Tanggal >= '{start_date}'
            AND evap.Tanggal < '{end_date}'
            ORDER BY evap.Tanggal ASC;"""))
    return result


def query_pelayanan_instalasi(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT bp.TglPelayanan, i.NamaInstalasi
            FROM dbo.BiayaPelayanan bp
            INNER JOIN dbo.Ruangan r
            ON bp.KdRuangan = r.KdRuangan
            INNER JOIN dbo.Instalasi i
            ON r.KdInstalasi = i.KdInstalasi
            WHERE bp.TglPelayanan >= '{start_date}'
            AND bp.TglPelayanan < '{end_date}'
            ORDER BY bp.TglPelayanan ASC;"""))
    return result


def query_rujukan(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT pd.TglPendaftaran, ra.RujukanAsal
            FROM dbo.PasienDaftar pd
            INNER JOIN dbo.Rujukan r
            ON pd.NoCM = r.NoCM
            INNER JOIN dbo.RujukanAsal ra
            ON r.KdRujukanAsal = ra.KdRujukanAsal
            WHERE pd.TglPendaftaran >= '{start_date}'
            AND pd.TglPendaftaran < '{end_date}'
            ORDER BY pd.TglPendaftaran ASC;"""))
    return result


def query_kelompok_pasien(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT DISTINCT bp.NoPendaftaran, pd.TglPendaftaran, kp.JenisPasien as KelompokPasien
            FROM dbo.BiayaPelayanan bp
            INNER JOIN dbo.PasienDaftar pd
            ON bp.NoPendaftaran = pd.NoPendaftaran
            INNER JOIN dbo.KelompokPasien kp
            ON pd.KdKelasAkhir = kp.KdKelompokPasien
            WHERE bp.TglPelayanan >= '{start_date}'
            AND bp.TglPelayanan < '{end_date}'
            ORDER BY pd.TglPendaftaran ASC;"""))
    return result


def query_umur_jenis_kelamin(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT DISTINCT bp.NoPendaftaran, pd.TglPendaftaran, p.TglLahir, p.JenisKelamin
            FROM dbo.BiayaPelayanan bp
            INNER JOIN dbo.PasienDaftar pd
            ON bp.NoPendaftaran = pd.NoPendaftaran 
            INNER JOIN dbo.Pasien p
            ON pd.NoCM = p.NoCM 
            WHERE bp.TglPelayanan >= '{start_date}'
            AND bp.TglPelayanan < '{end_date}'
            ORDER BY bp.NoPendaftaran ASC;"""))
    return result


def query_pendapatan_produk(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT DISTINCT evprsna.TanggalPelayanan, evprsna.Tarif, jp.Deskripsi
            FROM dbo.EIS_ViewPendapatanRumahSakitNewAll evprsna
            INNER JOIN dbo.ListPelayananRS lpr 
            ON evprsna.KdPelayananRS = lpr.KdPelayananRS 
            INNER JOIN dbo.JenisPelayanan jp 
            ON lpr.KdJnsPelayanan = jp.KdJnsPelayanan 
            WHERE evprsna.TanggalPelayanan >= '{start_date}'
            AND evprsna.TanggalPelayanan < '{end_date}'
            ORDER BY evprsna.TanggalPelayanan ASC;"""))
    return result


def query_pendapatan_instalasi(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT spp.TglStruk, spp.NoPendaftaran, i.NamaInstalasi , spp.TotalBiaya
           FROM dbo.StrukPelayananPasien spp
           INNER JOIN dbo.Ruangan r
           ON spp.KdRuanganTerakhir = r.KdRuangan
           INNER JOIN dbo.Instalasi i
           ON r.KdInstalasi = i.KdInstalasi
           WHERE spp.TglStruk >= '{start_date}'
           AND spp.TglStruk < '{end_date}'
           ORDER BY spp.TglStruk ASC;"""))
    return result


def query_pendapatan_cara_bayar(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT sbkm.TglBKM, cb.CaraBayar, sbkm.JmlBayar
            FROM dbo.StrukBuktiKasMasuk sbkm
            INNER JOIN dbo.CaraBayar cb
            ON sbkm.KdCaraBayar = cb.KdCaraBayar
            WHERE sbkm.TglBKM >= '{start_date}'
            AND sbkm.TglBKM < '{end_date}'
            ORDER BY sbkm.TglBKM ASC;"""))
    return result


def query_pendapatan_kelas(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT spp.TglStruk, spp.NoPendaftaran, kp.DeskKelas, spp.TotalBiaya
           FROM dbo.StrukPelayananPasien spp
           INNER JOIN dbo.PasienDaftar pd
           ON spp.NoPendaftaran = pd.NoPendaftaran
           INNER JOIN dbo.KelasPelayanan kp
           ON pd.KdKelasAkhir = kp.KdKelas
           WHERE spp.TglStruk >= '{start_date}'
           AND spp.TglStruk < '{end_date}'
           ORDER BY spp.TglStruk ASC;"""))
    return result
