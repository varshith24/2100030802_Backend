import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="sftek",
    user="postgres",
    password="admin",
    host="localhost",
)
cursor = conn.cursor()

# 1. List all customers
cursor.execute('''
SELECT * FROM Customers;
''')
customers = cursor.fetchall()
print("1. Customers:")
print(*customers,sep="\n")
print()


# 2. Find all orders placed in January 2023
cursor.execute('''
SELECT * FROM Orders
WHERE OrderDate BETWEEN '2023-01-01' AND '2023-01-31';
''')
january_orders = cursor.fetchall()
print("2. January 2023 Orders:")
print(*january_orders,sep="\n")
print()

# 3. Get the details of each order, including the customer name and email
cursor.execute('''
SELECT 
    Orders.OrderID, 
    Customers.FirstName, 
    Customers.LastName, 
    Customers.Email, 
    Orders.OrderDate
FROM Orders
JOIN Customers ON Orders.CustomerID = Customers.CustomerID;
''')
order_details = cursor.fetchall()
print("3. Order Details:", )
print(*order_details,sep="\n")
print()

# 4. List the products purchased in a specific order (e.g., OrderID = 1)
cursor.execute('''
SELECT 
    Products.ProductName, 
    OrderItems.Quantity
FROM OrderItems
JOIN Products ON OrderItems.ProductID = Products.ProductID
WHERE OrderItems.OrderID = 1;
''')
order1_products = cursor.fetchall()
print("4. Products in Order 1:", order1_products)
print(*order1_products,sep="\n")
print()

# 5. Calculate the total amount spent by each customer
cursor.execute('''
SELECT 
    Customers.CustomerID, 
    Customers.FirstName, 
    Customers.LastName, 
    SUM(Products.Price * OrderItems.Quantity) AS TotalSpent
FROM Customers
JOIN Orders ON Customers.CustomerID = Orders.CustomerID
JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
JOIN Products ON OrderItems.ProductID = Products.ProductID
GROUP BY Customers.CustomerID, Customers.FirstName, Customers.LastName;
''')
total_spent_by_customers = cursor.fetchall()
print("5. Total Amount Spent by Each Customer:", total_spent_by_customers)
print(*total_spent_by_customers,sep="\n")
print()

# 6. Find the most popular product (the one that has been ordered the most)
cursor.execute('''
SELECT 
    Products.ProductID, 
    Products.ProductName, 
    SUM(OrderItems.Quantity) AS TotalQuantity
FROM OrderItems
JOIN Products ON OrderItems.ProductID = Products.ProductID
GROUP BY Products.ProductID, Products.ProductName
ORDER BY TotalQuantity DESC
LIMIT 1;
''')
most_popular_product = cursor.fetchone()
print("6. Most Popular Product:")
print(*most_popular_product,sep="\n")
print()

# 7. Get the total number of orders and the total sales amount for each month in 2023
cursor.execute('''
SELECT 
    TO_CHAR(OrderDate, 'YYYY-MM') AS Month, 
    COUNT(Orders.OrderID) AS TotalOrders, 
    SUM(Products.Price * OrderItems.Quantity) AS TotalSales
FROM Orders
JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
JOIN Products ON OrderItems.ProductID = Products.ProductID
WHERE EXTRACT(YEAR FROM OrderDate) = 2023
GROUP BY TO_CHAR(OrderDate, 'YYYY-MM');
''')
monthly_sales_2023 = cursor.fetchall()
print("7. Monthly Sales in 2023:")
print(*monthly_sales_2023,sep="\n")
print()

# 8. Find customers who have spent more than $1000
cursor.execute('''
SELECT 
    Customers.CustomerID, 
    Customers.FirstName, 
    Customers.LastName, 
    SUM(Products.Price * OrderItems.Quantity) AS TotalSpent
FROM Customers
JOIN Orders ON Customers.CustomerID = Orders.CustomerID
JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
JOIN Products ON OrderItems.ProductID = Products.ProductID
GROUP BY Customers.CustomerID, Customers.FirstName, Customers.LastName
HAVING SUM(Products.Price * OrderItems.Quantity) > 1000;
''')
high_spending_customers = cursor.fetchall()
print("8. Customers Who Spent More Than $1000:")
print(*high_spending_customers,sep="\n")
print()
# Commit the transactions
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
