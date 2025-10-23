# ANSI color codes
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def colorize_number(value: float, positive_is_good: bool = True) -> str:
    """Colorize a number based on whether it's positive or negative."""
    if value > 0:
        color = Colors.GREEN if positive_is_good else Colors.RED
    elif value < 0:
        color = Colors.RED if positive_is_good else Colors.GREEN
    else:
        color = Colors.YELLOW
    return f"{color}${value:,.2f}{Colors.RESET}"


def colorize_ratio(value: float, positive_is_good: bool = True) -> str:
    """Colorize a ratio/percentage based on whether it's positive or negative."""
    if value > 0:
        color = Colors.GREEN if positive_is_good else Colors.RED
    elif value < 0:
        color = Colors.RED if positive_is_good else Colors.GREEN
    else:
        color = Colors.YELLOW
    return f"{color}{value:,.6f}{Colors.RESET}"


def analyze_rental_cashflow(
    house_price: float,
    interest_rate: float,
    lowest_rent_per_month: float,
    highest_rent_per_month: float,
    expected_other_monthly_expenses: float,
    expected_home_price_growth: float,
    lowest_rehab_cost: float,
    highest_rehab_cost: float,
    rehab_value_multiplier: float
) -> dict:
    """
    Analyze the monthly cash flow of a rental property with rehab cost and rent scenarios.

    Args:
        house_price: Purchase price of the house
        interest_rate: Annual interest rate for the home loan (as decimal, e.g., 0.05 for 5%)
        lowest_rent_per_month: Lowest estimate for monthly rental income
        highest_rent_per_month: Highest estimate for monthly rental income
        expected_other_monthly_expenses: Other monthly expenses (maintenance, property tax, insurance, etc.)
        expected_home_price_growth: Annual home price growth rate (as decimal, e.g., 0.03 for 3%)
        lowest_rehab_cost: Lowest estimate for rehab costs
        highest_rehab_cost: Highest estimate for rehab costs
        rehab_value_multiplier: Multiplier for rehab ROI (e.g., 1.5 means $1 rehab adds $1.50 value)

    Returns:
        dict: Dictionary containing cash flow analysis details for all scenarios
    """
    # Calculate mid values
    mid_rehab_cost = (lowest_rehab_cost + highest_rehab_cost) / 2
    mid_rent = (lowest_rent_per_month + highest_rent_per_month) / 2

    rehab_scenarios = {
        'low': lowest_rehab_cost,
        'mid': mid_rehab_cost,
        'high': highest_rehab_cost
    }

    rent_scenarios = {
        'low': lowest_rent_per_month,
        'mid': mid_rent,
        'high': highest_rent_per_month
    }

    results = {}

    # print(f"\n{'='*80}")
    # print(f"RENTAL PROPERTY CASH FLOW ANALYSIS - 9 SCENARIOS")
    # print(f"{'='*80}")
    # print(f"\nBase Purchase Price: ${house_price:,.2f}")
    # print(f"Annual Interest Rate: {interest_rate*100:.2f}%")
    # print(f"Rehab Cost Range: ${lowest_rehab_cost:,.2f} - ${highest_rehab_cost:,.2f}")
    # print(f"Rent Range: ${lowest_rent_per_month:,.2f} - ${highest_rent_per_month:,.2f}")

    # Generate all 9 scenarios (3 rehab x 3 rent combinations)
    for rehab_name, rehab_cost in rehab_scenarios.items():
        for rent_name, monthly_rent in rent_scenarios.items():
            scenario_key = f"{rehab_name}_rehab_{rent_name}_rent"

            # Initial loan before pre-rental interest
            initial_loan = house_price + rehab_cost

            # Calculate 4 months of interest before rental starts (added to loan)
            monthly_interest_rate = interest_rate / 12
            pre_rental_interest = initial_loan * monthly_interest_rate * 4

            # Total loan amount includes house price + rehab costs + pre-rental interest
            total_loan = initial_loan + pre_rental_interest

            # Calculate monthly interest cost on the total loan
            monthly_interest = total_loan * monthly_interest_rate

            # Calculate net monthly profit
            monthly_profit = monthly_rent - monthly_interest - expected_other_monthly_expenses

            # Calculate actual property value (purchase price + rehab value added)
            # Rehab adds value at the multiplier rate (e.g., 1.5x means $1 spent adds $1.50 value)
            property_value = house_price + (rehab_cost * rehab_value_multiplier)

            # Calculate monthly home appreciation (on actual property value)
            monthly_appreciation = (property_value * expected_home_price_growth) / 12

            # Calculate investment return per dollar of loan
            total_monthly_gain = monthly_profit + monthly_appreciation
            invest_per_loan = total_loan / total_monthly_gain if total_monthly_gain != 0 else 0

            # Store results
            results[scenario_key] = {
                'rehab_level': rehab_name,
                'rent_level': rent_name,
                'rehab_cost': rehab_cost,
                'initial_loan': initial_loan,
                'pre_rental_interest': pre_rental_interest,
                'total_loan': total_loan,
                'property_value': property_value,
                'monthly_rent': monthly_rent,
                'monthly_interest': monthly_interest,
                'monthly_expenses': expected_other_monthly_expenses,
                'monthly_profit': monthly_profit,
                'monthly_appreciation': monthly_appreciation,
                'total_monthly_gain': total_monthly_gain,
                'invest_per_loan': invest_per_loan
            }

    # Print all 9 scenarios in a grid format
    print(f"\n{'='*80}")
    for rehab_name in ['low', 'mid', 'high']:
        print(f"\n{'='*80}")
        print(f"{f'REHAB COST: {rehab_name.upper()}':^80}")
        print(f"{'='*80}")

        for rent_name in ['low', 'mid', 'high']:
            scenario_key = f"{rehab_name}_rehab_{rent_name}_rent"
            data = results[scenario_key]

            scenario_header = f'Rent: {rent_name.upper()} (${data["monthly_rent"]:,.2f}) | Rehab: ${data["rehab_cost"]:,.2f}'
            print(f"\n{scenario_header:^80}")
            print(f"{'-'*80}")
            print(f"  Initial Loan (House+Rehab):  ${data['initial_loan']:>14,.2f}")
            print(f"  Pre-Rental Interest (4mo):   ${data['pre_rental_interest']:>14,.2f}")
            print(f"  Total Loan Amount:           ${data['total_loan']:>14,.2f}")
            print(f"  Property Value (w/ Rehab):   ${data['property_value']:>14,.2f}")
            print(f"\n  {'MONTHLY CASH FLOW:':^80}")
            print(f"    Rental Income:            +${data['monthly_rent']:>14,.2f}")
            print(f"    Interest Payment:         -${data['monthly_interest']:>14,.2f}")
            print(f"    Other Expenses:           -${data['monthly_expenses']:>14,.2f}")
            print(f"    {'-'*80}")

            profit_str = colorize_number(data['monthly_profit'])
            print(f"    NET MONTHLY PROFIT:        {profit_str:>25}")

            print(f"\n  {'APPRECIATION:':^80}")
            appreciation_str = colorize_number(data['monthly_appreciation'])
            print(f"    Monthly Appreciation:      {appreciation_str:>25}")
            print(f"    (at {expected_home_price_growth*100:.2f}% annual growth)")
            print(f"\n  {'TOTAL MONTHLY GAIN:':^80}")

            gain_str = colorize_number(data['total_monthly_gain'])
            print(f"    Cash Flow + Appreciation:  {gain_str:>25}")

            invest_per_loan_str = colorize_ratio(data['invest_per_loan'])
            print(f"    Invest Per Loan:           {invest_per_loan_str:>25}")

        print(f"\n{'='*80}")

    # Print summary of ranges across all scenarios
    all_monthly_profits = [data['monthly_profit'] for data in results.values()]
    all_monthly_appreciations = [data['monthly_appreciation'] for data in results.values()]
    all_total_gains = [data['total_monthly_gain'] for data in results.values()]
    all_invest_per_loan = [data['invest_per_loan'] for data in results.values()]

    print(f"\n{'='*80}")
    print(f"{'SUMMARY: RANGE ACROSS ALL 9 SCENARIOS':^80}")
    print(f"{'='*80}")

    print(f"\n  Net Monthly Profit (Cash Flow):")
    min_profit_str = colorize_number(min(all_monthly_profits))
    max_profit_str = colorize_number(max(all_monthly_profits))
    print(f"    Minimum: {min_profit_str:>23}")
    print(f"    Maximum: {max_profit_str:>23}")
    print(f"    Range:   ${max(all_monthly_profits) - min(all_monthly_profits):>12,.2f}")

    print(f"\n  Monthly Appreciation:")
    min_appr_str = colorize_number(min(all_monthly_appreciations))
    max_appr_str = colorize_number(max(all_monthly_appreciations))
    print(f"    Minimum: {min_appr_str:>23}")
    print(f"    Maximum: {max_appr_str:>23}")
    print(f"    Range:   ${max(all_monthly_appreciations) - min(all_monthly_appreciations):>12,.2f}")

    print(f"\n  Total Monthly Gain (Cash Flow + Appreciation):")
    min_gain_str = colorize_number(min(all_total_gains))
    max_gain_str = colorize_number(max(all_total_gains))
    print(f"    Minimum: {min_gain_str:>23}")
    print(f"    Maximum: {max_gain_str:>23}")
    print(f"    Range:   ${max(all_total_gains) - min(all_total_gains):>12,.2f}")

    print(f"\n  Invest Per Loan:")
    min_ipl_str = colorize_ratio(min(all_invest_per_loan))
    max_ipl_str = colorize_ratio(max(all_invest_per_loan))
    print(f"    Minimum: {min_ipl_str:>23}")
    print(f"    Maximum: {max_ipl_str:>23}")
    print(f"    Range:   {max(all_invest_per_loan) - min(all_invest_per_loan):>12,.6f}")

    print(f"\n{'='*80}\n")

    return results


# Example usage
if __name__ == "__main__":
    # Example: $254,000 house with 5.5% interest rate
    # Assume rehab adds 1.2x value (e.g., $50k rehab adds $60k in value)
    result = analyze_rental_cashflow(
        house_price=254000,
        interest_rate=0.055,
        lowest_rent_per_month=(1300+880)*0.9,
        highest_rent_per_month=(1845+1400)*0.9,
        expected_other_monthly_expenses=400,
        expected_home_price_growth=0.026,
        lowest_rehab_cost=60000,
        highest_rehab_cost=100000,
        rehab_value_multiplier=1.04
    )

    # result = analyze_rental_cashflow(
    #     house_price=302000,
    #     interest_rate=0.055,
    #     lowest_rent_per_month=(1600)*0.9,
    #     highest_rent_per_month=(2600)*0.9,
    #     expected_other_monthly_expenses=400,
    #     expected_home_price_growth=0.026,
    #     lowest_rehab_cost=0,
    #     highest_rehab_cost=0,
    #     rehab_value_multiplier=1.08
    # )

    # result = analyze_rental_cashflow(
    #     house_price=379900,
    #     interest_rate=0.055,
    #     lowest_rent_per_month=(2000)*0.95,
    #     highest_rent_per_month=(3000)*0.95,
    #     expected_other_monthly_expenses=400,
    #     expected_home_price_growth=0.026,
    #     lowest_rehab_cost=0,
    #     highest_rehab_cost=0,
    #     rehab_value_multiplier=1.0
    # )

