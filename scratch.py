#Ex 1: List of Square of even numbers
even_square = [x**2 for x in range(10) if x%2 == 0]


#Ex 2: Filter even numbers
even = [n for n in range(1,11) if (lambda x: x%2 == 0)(n)]


#Ex 3: Upper ticker symbols
upper = [t.upper() for t in ['nvda', 'mu', 'msft']]


#Ex 4: Filter prices above threshold
prices = [110, 120, 87, 60, 100]
filtered_price = [p for p in prices if p > 100]


#Ex 5: Nested: pairs of (ticker, price)
tickers = ['NVDA', 'MU']
prices = [430, 85]
# Method 1
nested = []
for i in range(len(tickers)):
    pair = (tickers[i], prices[i])
    nested.append(pair)

# Method 2
[(t, p) for t, p in zip(tickers, prices)] 
# the zip function takes 2 or more lists and pairs them up element by element


#Ex 6: Lambda: sort by price descending
stocks = [{'name': 'NVDA', 'price': 430}, {'name': 'MU', 'price': 85}]
# Method 1
sorted(stocks, key=lambda s: s['price'], reverse=True)

# Method 2
result = []
for dict in stocks:
    result.append(dict['price'])
sorted_result = sorted(result, reverse=True)


#Ex 7: Sort list of tuples by second element
pairs = [(1, 3), (4, 1), (2, 5), (3, 2)]
sorted_pairs = sorted(pairs, key=lambda t: t[1])


#Ex 8: Calculate portfolio value from shares * price
holdings = [("AAPL", 10, 182.5), ("TSLA", 5, 245.0), ("GOOGL", 3, 140.2)]
# Method 1
get_value = lambda ticker, shares, price: (ticker, shares * price)
portfolio = [get_value(*h) for h in holdings] 
# the * is the unpacking operator that takes the tuple h and 
# unpacks it into separate arguments before passing them to the function.

# Method 2
port = []
for i in range(len(holdings)):
    port.append((holdings[i][0], holdings[i][1]*holdings[i][2]))


# Ex 9: Apply a flat 15% tax to capital gains
gains = [1200, 450, 8900, 300, 5000]

# Method 1: simple for loop
after = []
for g in gains:
    after.append(round(g*0.85, 2))

# Method 2: fancy lambda function
after_tax = lambda g: round(g * (1 - 0.15), 2)
net_gains = [after_tax(g) for g in gains]


# Ex 10: Compound interest for multiple principals
principals = [1000, 5000, 10000, 25000]

# Method 1: simple for loop
r = 0.07
t = 10
compound_values = []
for p in principals:
    compound_values.append(round(p*(1+r)**t, 2))

# Method 2: fancy lambda function
compound = lambda p, r=0.07, t=10: round(p * (1 + r) ** t, 2)
future_values = [compound(p) for p in principals]

