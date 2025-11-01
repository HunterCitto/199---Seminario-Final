import pandas as pd
import numpy as np

class FWITools() :

    def __init__(self) : 
        pass

    def kelvin_a_celsius(self, value) :
        """
        Convierte una temperatura de Kelvin a Celsius. 
        
        Parámetros:
        value: Temperatura en Kelvin.

        Retorna:
        Temperatura en Celsius.
        """
        return value - 273.15

    def conversion_kelvin(self, df, cols = ['t2m', 'd2m']):
        """
        Convierte las columnas de temperatura de Kelvin a Celsius en un DataFrame.
        
        Parámetros:
        df: DataFrame con columnas 't2m' y 'd2m' en Kelvin.

        Retorna:
        DataFrame con las columnas 't2m' y 'd2m' convertidas a Celsius.
        """
        df = df.copy()
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype('float64') - 273.15
        return df

    def humedad_relativa(self, temperature, dew_point):
        """
        Calcula la humedad relativa a partir de la temperatura y el punto de rocío.
        
        Parámetros:
        temperature: Temperatura en grados Celsius.
        dew_point: Punto de rocío en grados Celsius.
        
        Retorna:
        Humedad relativa en porcentaje.
        """

        a = 17.625
        b = 243.04
        num = np.exp((a * dew_point) / (b + dew_point))
        den = np.exp((a * temperature) / (b + temperature))
        rh = 100.0 * (num / den)

        rh = np.clip(rh, 0.0, 100.0)
        return rh

    def viento_vel_dir(self, u, v):
        """
        Calcula la velocidad y dirección del viento a partir de componentes u y v.
        
        Parámetros:
        u (ERA5 notation: u-component of wind): componente zonal del viento (m/s).
        v (ERA5 notation: v-component of wind): componente meridional del viento (m/s).

        Retorna:
        vel: velocidad del viento (m/s).
        dir: dirección del viento (grados desde el norte).
        """
        vel = np.sqrt(np.asarray(u) ** 2 + np.asarray(v) ** 2) # Función pitágoras.
        dir_deg = (np.degrees(np.arctan2(np.asarray(v), np.asarray(u))) + 360) % 360 # Función arcotangente.
        return vel, dir_deg

    def calcular_precipitacion(self, group, tp_col = 'tp_mm', time_col = 'acq_datetime'):
        """
        Calcula la precipitación por intervalo a partir de la precipitación acumulada.
        
        Parámetros:
        group : Grupo de datos con columna 'tp_mm' (precipitación acumulada en mm)
        
        Retorna:
        Serie con precipitación por intervalo en mm, alineada al índice del grupo.
        """
        tp = group[tp_col].astype('float64')
        
        if time_col in group.columns:
            tp = tp.sort_index() if False else tp
        diff = tp.diff()
        
        diff.iloc[0] = tp.iloc[0]
        
        mask_neg = diff < 0
        if mask_neg.any():
            diff.loc[mask_neg] = tp.loc[mask_neg]
        
        diff = diff.clip(lower=0.0)
        return diff

    def aplicar_precipitacion(self, df, columna_acumulada = 'tp', tp_mm_col = 'tp_mm', out_col = 'precipitacion_mm',
                            group_cols = ['latitude','longitude'], time_col = 'acq_datetime'):
        """
        Función que calcula la precipitación por intervalo a partir de la acumulada, agrupando por punto espacial.

        Paramétros:
        df: DataFrame con columna de precipitación acumulada en metros (columna_acumulada).
        columna_acumulada: nombre de la columna con precipitación acumulada en metros (default 'tp').
        tp_mm_col: nombre de la columna temporal para precipitación acumulada en mm (default 'tp_mm').
        out_col: nombre de la columna de salida con precipitación por intervalo en mm (default 'precipitacion_mm').
        group_cols: columnas para agrupar por punto espacial (default ['latitude','longitude']).
        time_col: columna de tiempo para ordenar dentro de cada grupo (default 'acq_datetime').

        Retorna:
        DataFrame con columna adicional de precipitación por intervalo en mm (out_col).
        """
        df = df.copy()
        # Convertir m a mm
        if columna_acumulada not in df.columns:
            raise KeyError(f"columna acumulada '{columna_acumulada}' no encontrada en df")
        df[tp_mm_col] = df[columna_acumulada].astype('float64') * 1000.0

        # Asegurar columna de tiempo tipo datetime
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col])

        # Array vacío para resultados
        resultado = pd.Series(index = df.index, dtype = 'float64')

        # Agrupar por lat/lon
        groupby_obj = df.groupby(group_cols, sort = False)

        for _, group_idx in groupby_obj.groups.items():
            group = df.loc[group_idx].sort_values(time_col)
            precip_interval = calcular_precipitacion(group, tp_col = tp_mm_col, time_col = time_col)
            resultado.loc[precip_interval.index] = precip_interval

        df[out_col] = resultado
        df[out_col] = df[out_col].fillna(0.0)
        return df

    def calcular_ffmc(self, temperature, rh, wind_speed, precipitation, previous_ffmc = 85.0):
        """
        Calcula el Fine Fuel Moisture Code (FFMC) del Canadian Forest Fire Weather Index System.
        
        Parámetros:
        temperature : Temperatura en °C
        rh : Humedad relativa en %
        wind_speed : Velocidad del viento en m/s (se convierte a km/h)
        precipitation : Precipitación en mm
        previous_ffmc : FFMC del día anterior (default=85.0)
        
        Retorna:
        ffmc : Fine Fuel Moisture Code

        Ver: https://wikifire.wsl.ch/tiki-index91f7.html?page=Fine+fuel+moisture+code
        """
        if any(pd.isna([temperature, rh, wind_speed, precipitation])):
            return previous_ffmc

        wind_speed_kmh = wind_speed * 3.6
        
        m0 = 147.2 * (101.0 - previous_ffmc) / (59.5 + previous_ffmc)

        # Efecto de la lluvia.
        if precipitation > 0.5:
            rf = precipitation - 0.5        
            if m0 <= 150.0:
                m0 += 42.5 * rf * np.exp(-100.0 / (251.0 - m0)) * (1.0 - np.exp(-6.93 / rf))
            else : 
                m0 += 42.5 * rf * np.exp(-100.0 / (251.0 - m0)) * (1.0 - np.exp(-6.93 / rf)) + 0.0015 * (m0 - 150.0) ** 2 * np.sqrt(rf)
            m0 = min(m0, 150.0)

        # Efecto del secado (temperatura, humedad, viento).
        # Humedad de equilibrio por secado (Ed) y por humectación (Ew).
        Ed = 0.942 * (rh**0.679) + 11.0 * np.exp((rh - 100.0) / 10.0) + \
            0.18 * (21.1 - temperature) * (1.0 - np.exp(-0.115 * rh))
        
        Ew = 0.618 * (rh**0.753) + 10.0 * np.exp((rh - 100.0) / 10.0) + \
            0.18 * (21.1 - temperature) * (1.0 - np.exp(-0.115 * rh))

        # Lógica de secado/humectación
        if m0 > Ed:
            # Secado
            k0 = 0.424 * (1.0 - (rh / 100.0) ** 1.7) + \
                0.0694 * np.sqrt(wind_speed_kmh) * (1.0 - (rh / 100.0) ** 8)
            kd = k0 * 0.581 * np.exp(0.0365 * temperature)
            m = Ed + (m0 - Ed) / (10.0 ** kd)
        elif m0 < Ew:
            # Humectación
            k1 = 0.424 * (1.0 - ((100.0 - rh) / 100.0) ** 1.7) + \
                0.0694 * np.sqrt(wind_speed_kmh) * (1.0 - ((100.0 - rh) / 100.0) ** 8)
            kw = k1 * 0.581 * np.exp(0.0365 * temperature)
            m = Ew - (Ew - m0) / (10.0 ** kw)
        else:
            m = m0

        # FFMC
        ffmc = (59.5 * (250.0 - m)) / (147.2 + m)
        
        return np.clip(ffmc, 0.0, 101.0)

    def calcular_ffmc_series(self, df, ffmc0 = 85.0, group_cols = ['latitude','longitude'],
                            time_col = 'acq_datetime', temp_col = 't2m', rh_col = 'humedad_relativa',
                            wind_col = 'wind_speed', precip_col = 'precipitacion_mm'):
        """
        Calcula FFMC por punto espacial (group_cols), preservando la secuencia temporal.
        
        Parámetros:
        df: DataFrame con columnas de temperatura, humedad relativa, velocidad del viento y precipitación
        ffmc0: valor inicial de FFMC (default=85.0)
        group_cols: columnas para agrupar por punto espacial (default ['latitude','longitude'])
        time_col: columna de tiempo para ordenar dentro de cada grupo (default 'acq_datetime')
        temp_col: columna de temperatura en °C (default 't2m')
        rh_col: columna de humedad relativa en % (default 'humedad_relativa')
        wind_col: columna de velocidad del viento en m/s (default 'wind_speed')
        precip_col: columna de precipitación en mm (default 'precipitacion_mm')

        Retorna:
        Serie con valores de FFMC, alineada al índice del DataFrame original.
        """
        
        ffmc_result = pd.Series(index = df.index, dtype = 'float64')

        df = df.copy()
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col])

        grouped = df.groupby(group_cols, sort=False)
        for key, group_idx in grouped.groups.items():
            group = df.loc[group_idx].sort_values(time_col)
            prev = ffmc0
            vals = []
            for i, row in group.iterrows():
                prev = self.calcular_ffmc(
                    temperature = row[temp_col],
                    rh = row[rh_col],
                    wind_speed = row[wind_col],
                    precipitation = row[precip_col],
                    previous_ffmc = prev
                )
                vals.append(prev)
            ffmc_result.loc[group.index] = vals
        ffmc_result = ffmc_result.fillna(ffmc0)
        return ffmc_result

    def calcular_dmc(self, temperature, rh, precipitation, previous_dmc = 6.0, month = 12):
        """
        Calcula el Duff Moisture Code (DMC) del Canadian Forest Fire Weather Index System.
        
        Parámetros:
        temperature : Temperatura en °C
        rh : Humedad relativa en %
        precipitation : Precipitación en mm
        previous_dmc : DMC del día anterior (default=6.0)
        month : Mes del año (1-12) para ajuste estacional (default=7)
        
        Retorna:
        dmc : Duff Moisture Code

        Ver: https://wikifire.wsl.ch/tiki-index9436.html?page=Duff+moisture+code
        """
        # Factores de longitud del día para hemisferio sur (horas de luz aproximadas).
        day_length_factors = np.array([
            14.5, 13.92, 12.92, 11.92, 10.50, 9.30,
            9.50, 10.58, 11.92, 13.02, 14.03, 15.08
        ])
        day_length_adjustment = day_length_factors[int(month) - 1]

        dmc = previous_dmc

        # Efecto lluvia.
        if precipitation > 1.5 :
            re = 0.92 * precipitation - 1.27
            mo = 20.0 + np.exp(5.6348 - dmc / 43.43)
            if dmc <= 33.0 :
                b = 100.0 / (0.5 + 0.3 * dmc)
            elif dmc <= 65.0:
                b = 14.0 - 1.3 * np.log(dmc)
            else :
                b = 6.2 * np.log(dmc) - 17.2
            mr = mo + 1000.0 * re / (48.77 + b * re)
            dmc = 244.72 - 43.43 * np.log(mr - 20.0)
            dmc = max(dmc, 0.0)

        # Efecto temperatura y humedad.
        temp_adj = max(temperature, -1.1)
        k = 1.894 * (temp_adj + 1.1) * (100.0 - rh) * day_length_adjustment * 1e-4
        dmc = dmc + k
        dmc = max(dmc, 0.0)
        return dmc

    def calcular_dmc_series(self, df, initial_dmc = 6.0, group_cols = ['latitude','longitude'],
                            time_col = 'acq_datetime', temp_col = 't2m', rh_col = 'humedad_relativa',
                            precip_col = 'precipitacion_mm', month_col = None):
        """
        Versión vectorizada para cálculo de DMC por punto espacial.
        
        Parámetros:
        df: DataFrame con columnas de temperatura, humedad relativa y precipitación
        initial_dmc: valor inicial de DMC (default=6.0)
        group_cols: columnas para agrupar por punto espacial (default ['latitude','longitude'])
        time_col: columna de tiempo para ordenar dentro de cada grupo (default 'acq_datetime
        temp_col: columna de temperatura en °C (default 't2m')
        rh_col: columna de humedad relativa en % (default 'humedad_relativa')
        precip_col: columna de precipitación en mm (default 'precipitacion_mm')
        month_col: nombre de la columna con el mes (1..12). Si None, se infiere desde time_col.

        Retorna:
        Serie con valores de DMC, alineada al índice del DataFrame original.
        """
        df = df.copy()
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col])
        if month_col is None and time_col in df.columns:
            df['__month_inferred__'] = df[time_col].dt.month
            month_col = '__month_inferred__'

        dmc_result = pd.Series(index=df.index, dtype='float64')

        grouped = df.groupby(group_cols, sort=False)
        for key, group_idx in grouped.groups.items():
            group = df.loc[group_idx].sort_values(time_col)
            prev = initial_dmc
            vals = []
            for i, row in group.iterrows():
                month = int(row[month_col]) if month_col in row.index else 12
                prev = self.calcular_dmc(
                    temperature = row[temp_col],
                    rh = row[rh_col],
                    precipitation = row[precip_col],
                    previous_dmc = prev,
                    month = month
                )
                vals.append(prev)
            dmc_result.loc[group.index] = vals

        dmc_result = dmc_result.fillna(initial_dmc)
        # limpiar columna temporal si la creamos
        if '__month_inferred__' in df.columns:
            df.drop(columns='__month_inferred__', inplace=True, errors=True)
        return dmc_result

    def calcular_dc(self, temperature, precipitation, previous_dc = 15.0, month = 12):
        """
        Calcula el Drought Code (DC) del Canadian Forest Fire Weather Index System.
        
        Parámetros:
        temperature : Temperatura en °C
        precipitation : Precipitación en mm
        previous_dc : DC del día anterior (default = 15.0)
        month : Mes del año (1-12) para ajuste estacional (default = 12)
        
        Retorna:
        dc : Drought Code

        Ver: https://wikifire.wsl.ch/tiki-index91f7.html?page=Drought+code
        """
        day_length_factors = np.array([
            -1.6, -1.6, -1.6, 0.9, 3.8, 5.8, 6.4, 5.0, 2.4, 0.4, -1.6, -1.6
        ])
        day_length_adjustment = day_length_factors[int(month) - 1]

        dc = previous_dc

        # Efecto lluvia.
        if precipitation > 2.8:
            rd = 0.83 * precipitation - 1.27
            qo = 800.0 * np.exp(-dc / 400.0)
            qr = qo + 3.937 * rd
            if qr > 0:
                dc = 400.0 * np.log(800.0 / qr)
                dc = max(dc, 0.0)
            else:
                dc = 0.0

        # Efecto temperatura.
        temp_adj = max(temperature, -2.8)
        V = (0.36 * (temp_adj + 2.8) + day_length_adjustment) # Esta es la fórmula corregida
        
        # El secado solo ocurre si V > 0
        if V > 0 :
            dc = dc + V

        return dc

    def calcular_dc_series(self, df, initial_dc = 15.0, group_cols = ['latitude','longitude'],
                        time_col = 'acq_datetime', temp_col = 't2m', precip_col = 'precipitacion_mm',
                        month_col = None):
        """
        Versión vectorizada para cálculo de DC por punto espacial.
        
        Parámetros:
        df: DataFrame con columnas de temperatura y precipitación
        initial_dc: valor inicial de DC (default=15.0)
        group_cols: columnas para agrupar por punto espacial (default ['latitude','longitude'])
        time_col: columna de tiempo para ordenar dentro de cada grupo (default 'acq_datetime
        temp_col: columna de temperatura en °C (default 't2m')
        precip_col: columna de precipitación en mm (default 'precipitacion_mm')
        month_col: nombre de la columna con el mes (1..12). Si None, se infiere desde time_col.

        Retorna:
        Serie con valores de DC, alineada al índice del DataFrame original.
        """
        df = df.copy()
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col])
        if month_col is None and time_col in df.columns:
            df['__month_inferred__'] = df[time_col].dt.month
            month_col = '__month_inferred__'

        dc_result = pd.Series(index=df.index, dtype='float64')

        grouped = df.groupby(group_cols, sort=False)
        for key, group_idx in grouped.groups.items():
            group = df.loc[group_idx].sort_values(time_col)
            prev = initial_dc
            vals = []
            for i, row in group.iterrows():
                month = int(row[month_col]) if month_col in row.index else 12
                prev = self.calcular_dc(
                    temperature = row[temp_col],
                    precipitation = row[precip_col],
                    previous_dc = prev,
                    month = month
                )
                vals.append(prev)
            dc_result.loc[group.index] = vals

        dc_result = dc_result.fillna(initial_dc)
        if '__month_inferred__' in df.columns:
            df.drop(columns='__month_inferred__', inplace=True, errors=True)
        return dc_result

    def calcular_isi(self, ffmc, wind_speed):
        """
        Calcula el Initial Spread Index (ISI) del Canadian Forest Fire Weather Index System.
        
        Parámetros:
        ffmc : Fine Fuel Moisture Code
        wind_speed : Velocidad del viento en km/h
        
        Retorna:
        isi : Initial Spread Index
        
        Ver: https://wikifire.wsl.ch/tiki-index91f7.html?page=Initial+spread+index
        """
        
        wind_speed_kmh = np.asarray(wind_speed) * 3.6

        m = 147.2 * (101.0 - ffmc) / (59.5 + ffmc)
        fF = 91.9 * np.exp(-0.1386 * m) * (1.0 + (m ** 5.31) / 49300000.0)
        fW = np.exp(0.05039 * wind_speed_kmh)
        isi = 0.208 * fW * fF

        return isi

    def calcular_bui(self, dmc, dc):
        """
        Calcula el Build Up Index (BUI) del Canadian Forest Fire Weather Index System.
        
        Parámetros:
        dmc: Duff Moisture Code
        dc: Drought Code

        Retorna:
        bui: Build Up Index
        
        Ver: https://wikifire.wsl.ch/tiki-index91f7.html?page=Build-up+index
        """
        dmc = np.asarray(dmc)
        dc = np.asarray(dc)
        bui = np.empty_like(dmc, dtype = 'float64')

        for i in range(len(dmc)) :
            di = float(dmc[i])
            dci = float(dc[i])
            if di <= 0.4 * dci:
                val = (0.8 * dci * di) / (di + 0.4 * dci) if (di + 0.4 * dci) != 0 else 0.0
            else:
                val = di - (1.0 - 0.8 * dci / (di + 0.4 * dci)) * (0.92 + (0.0114 * di) ** 1.7)
            bui[i] = max(val, 0.0)
        return bui

    def calcular_fwi(self, isi, bui):
        """
        Calcula el Fire Weather Index (FWI) del Canadian Forest Fire Weather Index System.
        
        Parámetros:
        isi : Initial Spread Index
        bui : Build Up Index
        
        Retorna:
        fwi : Fire Weather Index
        
        Ver: https://wikifire.wsl.ch/tiki-index91f7.html?page=Fire+weather+index
        """

        isi = np.asarray(isi)
        bui = np.asarray(bui)
        fwi = np.empty_like(isi, dtype = 'float64')

        for i in range(len(isi)) :
            bi = float(bui[i])
            if bi <= 80.0:
                fD = 0.626 * (bi ** 0.809) + 2.0
            else:
                fD = 1000.0 / (25.0 + 108.64 * np.exp(-0.023 * bi))
            B = 0.1 * float(isi[i]) * fD
            if B <= 1.0:
                fwi[i] = B
            else:
                fwi[i] = np.exp(2.72 * (0.434 * np.log(B)) ** 0.647)
        return fwi

    def clasificar_riesgo_fwi(self, fwi):
        """
        Clasificación por umbrales simple.
        """
        if np.isscalar(fwi):
            if fwi < 5:
                return "Muy Bajo"
            elif fwi < 10 :
                return "Bajo"
            elif fwi < 20 :
                return "Moderado"
            elif fwi < 37 :
                return "Alto"
            elif fwi < 50 :
                return "Muy Alto"
            else :
                return "Extremo"
        else :        
            fwi_arr = np.asarray(fwi)
            labels = np.empty(fwi_arr.shape, dtype=object)
            labels[(fwi_arr < 5)] = "Muy Bajo"
            labels[(fwi_arr >= 5) & (fwi_arr < 10)] = "Bajo"
            labels[(fwi_arr >= 10) & (fwi_arr < 20)] = "Moderado"
            labels[(fwi_arr >= 20) & (fwi_arr < 30)] = "Alto"
            labels[(fwi_arr >= 30) & (fwi_arr < 50)] = "Muy Alto"
            labels[(fwi_arr >= 50)] = "Extremo"
            return pd.Series(labels, index = getattr(fwi, 'index', None))


    def calcular_fwi_completo(self, df, ffmc0 = 85.0, dmc0 = 6.0, dc0 = 15.0,
                            group_cols = ['latitude', 'longitude'], time_col = 'acq_datetime'):
        """
        Pipeline principal que aplica las transformaciones y calcula FWI por punto espacial.
        
        Parámetros:
        df: DataFrame con columnas necesarias (t2m, d2m, u10, v10, tp)
        ffmc0: valor inicial de FFMC (default=85.0)
        dmc0: valor inicial de DMC (default=6.0)
        dc0: valor inicial de DC (default=15.0)
        group_cols: columnas para agrupar por punto espacial (default ['latitude','longitude'])
        time_col: columna de tiempo para ordenar dentro de cada grupo (default 'acq_datetime')

        Retorna:
        DataFrame con columnas adicionales: humedad_relativa, wind_speed, wind_direction,
        precipitacion_mm, ffmc, dmc, dc, isi, bui, fwi, riesgo_incendio.
        """
        df = df.copy()

        # Asegurar columnas de tiempo
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col])
            df['day'] = df[time_col].dt.day
            df['month'] = df[time_col].dt.month
            df['year'] = df[time_col].dt.year
        else:
            df['month'] = 12

        df = self.conversion_kelvin(df, cols = ['t2m', 'd2m'])
        df['humedad_relativa'] = self.humedad_relativa(df['t2m'].values, df['d2m'].values)
        df['wind_speed'], df['wind_direction'] = self.viento_vel_dir(df['u10'].values, df['v10'].values)
        df['precipitacion_mm'] = df['tp'] * 1000.0

        # 5) FFMC, DMC, DC por punto espacial
        df['ffmc'] = self.calcular_ffmc_series(df, ffmc0 = ffmc0, group_cols = group_cols,
                                        time_col = time_col, temp_col = 't2m', rh_col = 'humedad_relativa',
                                        wind_col = 'wind_speed', precip_col = 'precipitacion_mm')

        df['dmc'] = self.calcular_dmc_series(df, initial_dmc = dmc0, group_cols = group_cols,
                                        time_col = time_col, temp_col = 't2m', rh_col = 'humedad_relativa',
                                        precip_col = 'precipitacion_mm', month_col = 'month')

        df['dc'] = self.calcular_dc_series(df, initial_dc = dc0, group_cols = group_cols,
                                    time_col = time_col, temp_col = 't2m', precip_col = 'precipitacion_mm',
                                    month_col = 'month')

        # 6) ISI, BUI, FWI.
        df['isi'] = self.calcular_isi(df['ffmc'].values, df['wind_speed'].values)
        df['bui'] = self.calcular_bui(df['dmc'].values, df['dc'].values)
        df['fwi'] = self.calcular_fwi(df['isi'].values, df['bui'].values)

        # Clasificación
        df['riesgo_incendio'] = self.clasificar_riesgo_fwi(df['fwi'])

        return df