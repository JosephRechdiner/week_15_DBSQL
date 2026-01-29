from fastapi import FastAPI, Depends, HTTPException
from db_init import init_database
from db import get_db_connection
import dal


app = FastAPI()

init_database()

@app.get("/health")
def health_check():
    return {"status": "ok"}

# 1
@app.get("/q1/customers-credit-limit-outliers")
def customers_credit_limit_outliers(cnx = Depends(get_db_connection)):
    try:
        return dal.get_customers_by_credit_limit_range(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 2
@app.get("/q2/orders-null-comments")
def orders_null_comments(cnx = Depends(get_db_connection)):
    try:
        return dal.get_orders_with_null_comments(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 3
@app.get("/q3/customers-first-5")
def customers_first_5(cnx = Depends(get_db_connection)):
    try:
        return dal.get_first_5_customers(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 4
@app.get("/q4/payments-total-average")
def payments_total_average(cnx = Depends(get_db_connection)):
    try:
        return dal.get_payments_total_and_average(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 5
@app.get("/q5/employees-office-phone")
def employees_office_phone(cnx = Depends(get_db_connection)):
    try:
        return dal.get_employees_with_office_phone(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 6
@app.get("/q6/customers-shipping-dates")
def customers_shipping_dates(cnx = Depends(get_db_connection)):
    try:
        return dal.get_customers_with_shipping_dates(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 7
@app.get("/q7/customer-quantity-per-order")
def customer_quantity_per_order(cnx = Depends(get_db_connection)):
    try:
        return dal.get_customer_quantity_per_order(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")

# 8
@app.get("/q8/customers-payments-by-lastname-pattern")
def customers_payments_by_lastname_pattern(cnx = Depends(get_db_connection), pattern: str = "son"):
    try:
        return dal.get_customers_payments_by_lastname_pattern(cnx)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {str(e)}")
