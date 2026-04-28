from agroclimate_indices.core.stats import safe_divide

def crop_et(et0, kc=1.0):
    out = kc * et0
    out.name = "crop_et"
    out.attrs["units"] = "mm day-1"
    return out

def water_balance(precip, et):
    out = precip - et
    out.name = "water_balance"
    out.attrs["units"] = "mm day-1"
    return out

def water_stress_index(aet, etc):
    out = 1 - safe_divide(aet, etc)
    out = out.clip(0, 1)
    out.name = "water_stress_index"
    out.attrs["units"] = "0-1"
    return out
