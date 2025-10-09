from datetime import datetime
from scipy.optimize import brentq

# Cashflows
cashflows = [-500000.00, -500000.00, -500000.00, -500000.00, -500000.00,
             -500000.00, -200000.00, -200000.00, -500000.00, -500000.00, 536426.78]

# Dates
dates = [
    datetime(2024, 10, 8),
    datetime(2024, 9, 11),
    datetime(2024, 8, 12),
    datetime(2024, 7, 11),
    datetime(2024, 6, 11),
    datetime(2024, 5, 13),
    datetime(2024, 5, 13),
    datetime(2024, 5, 13),
    datetime(2024, 4, 12),
    datetime(2024, 3, 11),
    datetime(2025, 10, 8)
]

# ---- Define XNPV ----
def xnpv(rate, cashflows, dates):
    d0 = min(dates)
    return sum(cf / (1 + rate) ** ((d - d0).days / 365.0) for cf, d in zip(cashflows, dates))

# ---- Find IRR safely ----
try:
    # Search for root in range -0.9999 to +10 (i.e. -99.99% to +1000% annualized)
    irr = brentq(lambda r: xnpv(r, cashflows, dates), -0.9999, 10)
    print(f"Annualized IRR: {irr * 100:.2f}%")
except ValueError:
    print("❌ No IRR found in the given range. The cashflows might never break even.")
