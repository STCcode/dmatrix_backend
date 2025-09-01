from scipy.optimize import newton
from datetime import datetime

cashflows = [-100, -100, -100, -100, 800]
dates = [datetime(2020,1,1), datetime(2021,1,1), datetime(2022,1,1), datetime(2023,1,1),datetime(2025,8,29)]

def xnpv(rate, cashflows, dates):
    d0 = dates[0]
    return sum(cf / (1 + rate)**((d - d0).days / 365) for cf, d in zip(cashflows, dates))

irr = newton(lambda r: xnpv(r, cashflows, dates), 0.1)
print(f"Annualized IRR: {irr*100:.2f}%")
