

class FireWeatherIndex :

    def __init__(self, temperature, humidity, wind_speed, rain):
        self.temperature = temperature
        self.humidity = humidity
        self.wind_speed = wind_speed
        self.rain = rain

    def calculate_ffmc(self):
        mo = (147.2 * (101.0 - self.humidity)) / (59.5 + self.humidity)
        if self.rain > 0.5:
            rf = self.rain - 0.5
            if mo > 150.0:
                mo = mo + 42.5 * rf * (1 - (mo / 150.0)) ** 4
            else:
                mo = mo + 42.5 * rf * (1 - (mo / 150.0) ** 4)
            if mo > 250.0:
                mo = 250.0
        ed = 0.942 * (self.humidity ** 0.679) + (11.0 * (self.humidity ** 0.16)) - 10.0
        if mo < ed:
            kl = 0.424 * (1 - ((100.0 - self.humidity) / 100) ** 1.7) + (0.0694 * (self.wind_speed ** 0.5)) * (1 - ((100.0 - self.humidity) / 100) ** 8)
            kw = kl * 0.581 * (self.temperature ** 1.5) / (mo + 1)
            mo = mo + kw * (ed - mo)
            if mo > ed:
                mo = ed
        else:
            kd = 0.424 * (1 - (self.humidity / 100) ** 1.7) + (0.0694 * (self.wind_speed ** 0.5)) * (1 - (self.humidity / 100) ** 8)
            kw = kd * 0.581 * (self.temperature ** 1.5) / (mo + 1)
            mo = mo - kw * (mo - ed)
            if mo < ed:
                mo = ed
        ffmc = (59.5 * (250.0 - mo)) / (147.2 + mo)
        if ffmc > 101.0:
            ffmc = 101.0
        return ffmc

    def calculate_dmc(self):
        if self.rain > 1.5:
            ra = self.rain
            rw = 0.92 * ra - 1.27
            if self.humidity < 33.0:
                b = 100.0 / (0.5 + 0.3 * self.humidity)
            elif self.humidity < 65.0:
                b = 14.0 - 1.3 * (self.humidity / 100) * 100.0
            else:
                b = 6.2 * (self.humidity / 100) * 100.0 - 17.2
            if self.temperature < -1.1:
                k = 0.0
            else:
                k = 1.894 * (self.temperature + 1.1) * (100.0 - self.humidity) * (10 ** -6)
            dmc = (b * k) + rw
            if dmc < 0.0:
                dmc = 0.0
        else:
            dmc = 0.0
        return dmc

    def calculate_dc(self):
        if self.rain > 2.8:
            ra = self.rain
            rw = 0.83 * ra - 1.27
            if self.temperature < -2.8:
                k = 0.0
            else:
                k = 0.36 * (self.temperature + 2.8) / 100.0
            dc = k + rw
            if dc < 0.0:
                dc = 0.0
        else:
            dc = 0.0
        return dc

    def calculate_isi(self, ffmc):
        ffmc = max(0.0, min(ffmc, 101.0))
        mo = 147.2 * (101.0 - ffmc) / (59.5 + ffmc)
        f = 91.9 * (mo ** 0.5) / (5.0 + 0.1 * mo)
        isi = f * (self.wind_speed ** 0.5)
        return isi

    def calculate_bui(self, dmc, dc):
        if dmc <= 0.4 * dc:
            bui = (0.8 * dc * dmc) / (dmc + 0.4 * dc)
        else:
            bui = dmc - (1 - (0.8 * dc) / (dmc + 0.4 * dc)) * (0.92 + (0.0114 * dmc) ** 1.7)
        if bui < 0.0:
            bui = 0.0
        return bui

    def calculate_fwi(self, isi, bui):
        if bui <= 80.0:
            f = 0.626 * (bui ** 0.809) + 2.0
        else:
            f = 1000.0 / (25.0 + 108.64 * (math.exp(-0.023 * bui)))
        ff = 0.1 * isi * f
        if ff <= 1.0:
            fwi = ff
        else:
            fwi = math.exp(2.72 * (0.434 * math.log(ff)) ** 0.647)
        return fwi

    def calculate_all_indices(self):
        ffmc = self.calculate_ffmc()
        dmc = self.calculate_dmc()
        dc = self.calculate_dc()
        isi = self.calculate_isi(ffmc)
        bui = self.calculate_bui(dmc, dc)
        fwi = self.calculate_fwi(isi, bui)
        return {
            "FFMC": ffmc,
            "DMC": dmc,
            "DC": dc,
            "ISI": isi,
            "BUI": bui,
            "FWI": fwi
        }
import math
# Example usage:
fwi_calculator = FireWeatherIndex(temperature=30, humidity=45, wind_speed=10, rain=5)
indices = fwi_calculator.calculate_all_indices()
print(indices)
