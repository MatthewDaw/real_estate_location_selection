"""Analysis for dividing SpaceX stock between Matt and Mark."""

def calculate_money_division(matt_invest, mark_invest, interest_rate, cost_for_matt_to_buy, profit_share):
    """
    Function to calculate money generated after an arbitrary amount of time.

    :param matt_invest: how much money Matt sends Mark
    :param mark_invest: how much money Mark is investing
    :param interest_rate: how much SpaceX grows for arbitrary period
    :param cost_for_matt_to_buy: cost for matt to buy one dollar of a share
    :param profit_share: percent of matt earning that goes to mark
    :return:
    """

    matt_space_x_dollars = matt_invest * (1/cost_for_matt_to_buy)
    mark_space_x_dollars = mark_invest + matt_invest * (1 - 1/cost_for_matt_to_buy)

    matt_space_x_dollars_after_interest = matt_space_x_dollars * interest_rate
    mark_space_x_dollars_after_interest = mark_space_x_dollars * interest_rate

    matt_raw_profit = matt_space_x_dollars_after_interest - matt_invest
    mark_raw_profit = mark_space_x_dollars_after_interest - mark_invest

    matt_take_home_profit = matt_raw_profit * (1-profit_share)
    mark_raw_profit +=  matt_raw_profit * profit_share

    return matt_take_home_profit, mark_raw_profit


"""
For cleaner mathematical notation, we will use the following variable substitutions:

for matt_invest and mark_invest we will simply set these values to 1 so that they can drop out of the math.
interest_rate: r
cost_for_matt_to_buy: c
profit_share: p

With this, the general formula for calculating how much profit Matthew makes is 
(1/c * r)*(1-p)

If we wish to constrain profit_share to 0 (ie, none of matt's profit goes to Mark once stock sells) 
then the formula for money earned becomes 1/c*r, finding a value c that is equivalent to an arbitrary mix of c and p
can be found by solving x in the equation:

(1/c*r)*(1-p) = 1/x*r

The solution is in the function convert_to_cost_to_buy
"""

def convert_to_cost_to_buy(interest_rate, cost_for_matt_to_buy, profit_share):
    return (interest_rate * cost_for_matt_to_buy) / (interest_rate * (1 - profit_share) + cost_for_matt_to_buy * profit_share)

"""
if we set c = 1, then finding a profit share equivalent to any arbitrary mix of c and p becomes a matter of solving for x in
(1/c*r)*(1-p) = r*(1-x)
The solution of which is in convert_to_profit_share
"""

def convert_to_profit_share(interest_rate, cost_for_matt_to_buy, profit_share):
    return 1 - (1/cost_for_matt_to_buy * interest_rate - 1)*(1-profit_share) / (interest_rate - 1)

def run_convertion_analysis(interest_rate):
    """
    Small helper function to convert a mix of cost_for_matt_to_buy stock and profit_share percentage to convert to
     the equivalent profit sharing if we only charged investment fees through one or the other.
     """
    cost_for_matt_to_buy = 1.05
    profit_share = 0.0

    print("Assuming cost_for_matt_to_buy is 1.1 and profit_share is 0.0 then:")
    cost_for_matt_to_profit_share = convert_to_profit_share(interest_rate, cost_for_matt_to_buy, profit_share)
    print("equivalent profit_share: ", cost_for_matt_to_profit_share, '\n\n')

    cost_for_matt_to_buy = 1.05
    profit_share = 0.075

    print("Assuming cost_for_matt_to_buy is 1.0 and profit_share is 0.15 then:")
    cost_for_matt_to_buy_prime = convert_to_cost_to_buy(interest_rate, cost_for_matt_to_buy, profit_share)
    print("equivalent cost_for_matt_to_buy_prime: ", cost_for_matt_to_buy_prime)

if __name__ == '__main__':
    run_convertion_analysis(1.32)

"""
Insights from a couple test runs:

And if SpaceX does (relative to them) very poor with 20% then

cost_for_matt_to_buy: 1.1 and profit_share 0.0 -> cost_for_matt_to_buy: 1.0 and profit_share 0.545
cost_for_matt_to_buy: 1.0 and profit_share 0.15 -> cost_for_matt_to_buy: 1.0256 and profit_share 0.0


Assuming SpaceX maintains it's average interest rate of 31.7%, then

cost_for_matt_to_buy: 1.1 and profit_share 0.0 -> cost_for_matt_to_buy: 1.0 and profit_share 0.378
cost_for_matt_to_buy: 1.0 and profit_share 0.15 -> cost_for_matt_to_buy: 1.037 and profit_share 0.0

However, if SpaceX does very poor with an interest rate of 40% then

cost_for_matt_to_buy: 1.1 and profit_share 0.0 -> cost_for_matt_to_buy: 1.0 and profit_share 0.318
cost_for_matt_to_buy: 1.0 and profit_share 0.15 -> cost_for_matt_to_buy: 1.04477 and profit_share 0.0

So we would guess that cost_for_matt_to_buy: 1.1 ~ profit_share 0.31-0.545
and 
profit_share 0.15 ~ cost_for_matt_to_buy 1.0256-1.04477 

The main insight from this is that giving a cut in upfront cuts is much more expensive to me than a seemingly larger profit share.
So much so that 1.1 dollar to stock exchange rate can be worth up to 54% of all of my profits. Something that's kind of mind boggling, but true all the same.
 
Another insight is that if I expect SpaceX to do poorly I'm more incentivized to give up profit_share, and if I expect it 
to do well then I'm more incentivized to give a more generous cost_for_matt_to_buy.
"""


"""
Now, to consider a trade, we need to figure out what a profit_share % is. Once that number is decided, we can use the
math above to find an equivalent profit_share and cost_for_matt_to_buy trade off.

To find determine a percentage, we need to consider the parameters of possible trades.

For this, we will assume that a "possible trade" is profit_share % where both of us end up with a non-negative increased
utility relative to all other options.

To your greatest limit, I could push your price down to equivalent to the best offer any other family member/friend is
willing offer. I don't know what that would be, and I suspect we don't want to spend the social capital to figure 
out what this exactly. I would guess though that it's something like 10% of raw stock value (feel free to dispute)

To my greatest limit, I would only want to invest if this investment scheme is better than investing more into real estate.
We will now calculate what that is.
"""

def estimate_matt_return_on_investment():
    # house costs 545000, and requires roughly a 15% downpayment
    initial_investment = 0.15 * 545000
    # house value estimate after 20 months
    new_house_value = 570000
    # rent income plus about 350 per month in extra tax return per month (mostly from interest payment and utilities)
    # minus lost money from interest and other expenses (2800 + 300) = 4100
    monthly_profit = (350+400+550*4+570*2 + 350) - 4100
    # total profit after 20 months
    total_earning_after_20_months = (570000 - 545000) + monthly_profit * 20
    # normalizing profit to 12 month period
    total_earnings_by_year = total_earning_after_20_months * 12/20
    # rough interest from investment
    rough_profit_percentage = ((total_earnings_by_year + initial_investment)) / initial_investment
    # 1.23339

"""
ANSWER: 1.23339 AFTER tax. 
To be fair, the property value went up a lot and the value of the place is exceptionally 
good, so it isn't as clear that I actually can get that good of a return next year.
Realistically, we would also have to factor in all of the work I have to put in to
make this work, which was kind of a lot.
 
To make this value comparable to returns in SpaceX, we need to back compute how much
interest is needed to have that resulting profit AFTER capital gain tax.

For this, we will be using the following table from https://learn.valur.com/texas-capital-gains-tax/#:~:text=In%202025%20Explained-,What%20Is%20The%20Texas%20Capital%20Gains%20Tax%3F,businesses%2C%20or%20other%20legal%20entities.

Taxable income
(Married Filing Jointly)	Tax Rate
$0 to $96,700	0%
$96,700 – $600,050	15%
$600,050 or more	20%

For simplicity, we'll assume you're still married when you sell (thanks Jess 🤑), we will also assume your base taxable
income is above $96,700, we will hold for at least one year before selling, and will never sell enough to make your
total taxable income get above $600,000 in a year. With those assumption, we can assume that we will be taxed 
15% on all of our profit, as capital gain tax does not charge based off of revenue. 
"""


def calculate_real_interest_rate(interest_rate=1.317, capital_gain_tax_rate=0.15):
    profit = (interest_rate - 1)
    tax = profit * capital_gain_tax_rate
    net_interest_after_tax = (1 + profit - tax)
    return net_interest_after_tax

def calculate_needed_interest_rate_to_match(net_interest_after_tax = 1.23, capital_gain_tax_rate=0.15):
    target = (net_interest_after_tax - capital_gain_tax_rate) / (1 - capital_gain_tax_rate)
    # answer: 1.2707
    assert net_interest_after_tax == calculate_real_interest_rate(target)

""""
From this, we see that to break even and I need my effective interest rate (AFTER tax) from spaceX to be at least 1.2707.
Now we need to know how much of a profit percentage I need to give to reduce my effective interest rate down to this.
"""

def calculate_max_interest_rate_to_match():
    difference = 0.317 - 0.2707
    profit_share_percentage = difference / 0.317
    # answer: 0.14605
    interest_rate = 1.317
    matt_take_home_profit, _ =calculate_money_division(1.0, 1.0, interest_rate, 1.0, profit_share_percentage)
    assert 0.2707 == round(matt_take_home_profit, 4)

"""
From this, we see that the highest profit sharing that would make sense for me is 14.6%.

So, without getting thing more complicated, the range of workable profitable sharing percentages should be 0% to 14.6%
However, we are ignoring some factors that should move the percentage up. 

Those being that I have to put in a ton of hours of work to buy another property 
you can probably sell your stock for a better deal that to another family member
your investment is probably is less high risk (probably? I'm honestly not sure, real estate is pretty safe)
odds of finding another 23% interest isn't super high (using my company's data will help a lot, but my first house was still an exceptionally good deal)
Assuming an average return of 31.7% may not be that fair anyway.

CONCLUSION:

Getting an investment scheme that has a similar return what my house is earning without the extra work would be a great deal,
even if there is room to push I don't want to. In fact, to be straight, I could be pushed to give more if pushed.

As such, 15% of all profits, and a optional burden of the 15% capital gain tax feels like a pretty good offer to me.
As far as exchanging that for upfront cost, I think we should just use my equation to find the midpoint of equivalence.
That should reduce the stock for both of us without either of us losing out too much on the possibility if SpaceX blows up.

OFFER: 

Running the formula a couple times, it seems a pretty good midpoint for values equivalent to 15% profit tax is
7.5% tax, and 1.02 and cost stock increase.

As for fees for buying stock later, I think the fairest thing is to get stock based off whatever the value is at the 
time I send you money. I understand you'll still get the same return, and this way I'm incentivized to send sooner
so that I can buy at the lower price.

"""
