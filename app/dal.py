from typing import List, Dict, Any

# 1
def get_customers_by_credit_limit_range(cnx):
    """Return customers with credit limits outside the normal range."""
    query = """
            SELECT customerName, creditLimit
            FROM customers
            WHERE creditLimit < 10000 OR creditLimit > 100000;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p1: {str(e)}")

# 2
def get_orders_with_null_comments(cnx):
    """Return orders that have null comments."""
    query = """
            SELECT orderNumber, comments
            FROM orders
            WHERE comments IS NULL
            ORDER BY orderDate;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p2: {str(e)}")

# 3
def get_first_5_customers(cnx):
    """Return the first 5 customers."""
    query = """
            SELECT customerName, contactLastName, contactFirstName
            FROM customers
            ORDER BY contactLastName
            LIMIT 5;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p3: {str(e)}")

# 4
def get_payments_total_and_average(cnx):
    """Return total and average payment amounts."""
    query = """
            SELECT 
                SUM(amount) AS totalAmount,
                AVG(amount) AS averageAmount,
                MIN(amount) AS minAmount,
                MAX(amount) AS maxAmount
            FROM payments;
        """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p4: {str(e)}")

# 5
def get_employees_with_office_phone(cnx):
    """Return employees with their office phone numbers."""
    query = """
            SELECT e.firstName, e.lastName, o.phone
            FROM employees e
            JOIN offices o 
            ON e.officeCode = o.officeCode;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p5: {str(e)}")

# 6
def get_customers_with_shipping_dates(cnx):
    """Return customers with their order shipping dates."""
    query = """
            SELECT c.customerName, o.orderDate
            FROM customers c 
            LEFT JOIN orders o
            ON c.customerNumber = o.customerNumber
            LEFT JOIN orderdetails od
            ON o.orderNumber = od.orderNumber
            GROUP BY c.customerName, o.orderDate;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p6: {str(e)}")

# 7
def get_customer_quantity_per_order(cnx):
    """Return customer name and quantity for each order."""
    query = """
            SELECT c.customerName, SUM(od.quantityOrdered) AS totalQuantity
            FROM customers c 
            LEFT JOIN orders o
            ON c.customerNumber = o.customerNumber
            JOIN orderdetails od
            ON o.orderNumber = od.orderNumber
            GROUP BY c.customerName
            ORDER BY c.customerName;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p7: {str(e)}")

# 8
def get_customers_payments_by_lastname_pattern(cnx, pattern: str = "son"):
    """Return customers and payments for last names matching pattern."""
    query = """
            SELECT c.customerName, CONCAT(e.firstName, ' ' ,e.lastName) AS employeeFullName, SUM(p.amount) AS totalAmount
            FROM customers c
            JOIN employees e
            ON c.salesRepEmployeeNumber = e.employeeNumber
            JOIN payments p
            ON c.customerNumber = p.customerNumber
            WHERE c.contactFirstName LIKE '%Mu%' OR c.contactFirstName LIKE '%ly%'
            GROUP BY c.customerName, CONCAT(e.firstName, ' ' ,e.lastName)
            ORDER BY totalAmount DESC;
            """
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        raise Exception(f"could not execute p8: {str(e)}")
