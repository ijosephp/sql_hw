--------------------------------------------------------------------------------------------------------------------------------
-- CREATE VIEWS
--------------------------------------------------------------------------------------------------------------------------------
-- create view that formats store_table_emp
CREATE OR REPLACE VIEW store_table_emp_view AS
    SELECT
        STORE_ID AS STORE_ID,
        STORE_ADDRESS1 AS STORE_ADDRESS1,
        STORE_ADDRESS2 AS STORE_ADDRESS2,
        STORE_CITY AS STORE_CITY,
        STORE_STATE AS STORE_STATE,
        STORE_ZIP AS STORE_ZIP,
        DATE_LAST_UPDATED AS DATE_LAST_UPDATED
    FROM StoreTable_emp;  
    
    
-- create view that formats store_table_sup
CREATE OR REPLACE VIEW store_table_sup_view AS
    SELECT
        STORE_ID AS STORE_ID,
        STORE_ADDRESS1 AS STORE_ADDRESS1,
        STORE_ADDRESS2 AS STORE_ADDRESS2,
        STORE_CITY AS STORE_CITY,
        STORE_STATE AS STORE_STATE,
        STORE_ZIP AS STORE_ZIP,
        DATE_LAST_UPDATED AS DATE_LAST_UPDATED
    FROM StoreTable_sup;  

-- create view that formats Department_emp
CREATE OR REPLACE VIEW department_emp_view AS
    SELECT
        DEPARTMENT_ID AS DEP_ID,
        DEPARTMENT_NAME AS DEP_NAME,
        DEP_START_DATE AS DEP_START_DATE,
        DEP_END_DATE AS DEP_END_DATE,
        DATE_LAST_UPDATED AS DATE_LAST_UPDATED
    FROM Department_emp;  


-- create view that formats Department_sup
CREATE OR REPLACE VIEW department_sup_view AS
    SELECT
        DEP_ID AS DEP_ID,
        DEP_NAME AS DEP_NAME,
        DEP_START_DATE AS DEP_START_DATE,
        DEP_END_DATE AS DEP_END_DATE,
        DATE_LAST_UPDATED AS DATE_LAST_UPDATED
    FROM Department_sup;  

-- create view that formats Product
CREATE OR REPLACE VIEW Product_view AS
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        PRODUCT_NAME AS PRODUCT_NAME,
        dep_name AS dep_name,
        product_descr AS product_descr,
        standard_price AS standard_price,
        quantity AS quantity,
        last_updated AS date_last_updated
    FROM Product;  

-- create view that formats Product_sale
CREATE OR REPLACE VIEW Product_sale_view AS
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        PRODUCT_NAME AS PRODUCT_NAME,
        product_descr AS product_descr,
        standard_price AS standard_price,
        quantity AS quantity,
        last_updated AS date_last_updated
    FROM Product_sale;  



--------------------------------------------------------------------------------------------------------------------------------
-- ETL PROCEDURES
--------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE store_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
    INSERT INTO StoreTable_dw
        (STORE_ID, STORE_ADDRESS1, STORE_ADDRESS2, STORE_CITY , STORE_STATE , STORE_ZIP ,DATE_LAST_UPDATED)
    SELECT  *
    FROM   store_table_emp_view
    WHERE  NOT EXISTS (SELECT *
                       FROM   StoreTable_dw
                       WHERE  
                              store_table_emp_view.STORE_ID = StoreTable_dw.STORE_ID);
    -- insert values for unique data_source/client_id from prospective_user_view into customers_dw table
    INSERT INTO StoreTable_dw
        (STORE_ID, STORE_ADDRESS1, STORE_ADDRESS2, STORE_CITY , STORE_STATE , STORE_ZIP ,DATE_LAST_UPDATED)
    SELECT  *
    FROM   store_table_sup_view
    WHERE  NOT EXISTS (SELECT *
                       FROM   StoreTable_dw
                       WHERE  
                              store_table_sup_view.STORE_ID = StoreTable_dw.STORE_ID);
                              
                              
    -- Merge statement that updates matching data_source/client_id combinations from curr_user_view
    -- in client_dw and if no combination found, inserts new client into client_dw
    MERGE INTO StoreTable_dw
        USING (SELECT * FROM store_table_emp_view) stew  ON (stew.STORE_ID = StoreTable_dw.STORE_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          StoreTable_dw.STORE_ADDRESS1 = stew.STORE_ADDRESS1,
          StoreTable_dw.STORE_ADDRESS2 = stew.STORE_ADDRESS2,
          StoreTable_dw.STORE_CITY = stew.STORE_CITY,
          StoreTable_dw.STORE_STATE = stew.STORE_STATE,
          StoreTable_dw.STORE_ZIP = stew.STORE_ZIP,
          StoreTable_dw.DATE_LAST_UPDATED = stew.DATE_LAST_UPDATED
          
      WHEN NOT MATCHED THEN
        INSERT (STORE_ID, STORE_ADDRESS1, STORE_ADDRESS2, STORE_CITY , STORE_STATE , STORE_ZIP ,DATE_LAST_UPDATED)
        VALUES (stew.STORE_ID, stew.STORE_ADDRESS1, 
        stew.STORE_ADDRESS2, stew.STORE_CITY, stew.STORE_STATE,
        stew.STORE_ZIP,stew.DATE_LAST_UPDATED);
        
        
    -- Merge statement that updates matching data_source/client_id combinations from prospective_view
    -- in client_dw and if no combination found, inserts new client into client_dw
    MERGE INTO StoreTable_dw
        USING (SELECT * FROM store_table_sup_view) stsv  ON (stsv.STORE_ID = StoreTable_dw.STORE_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          StoreTable_dw.STORE_ADDRESS1 = stsv.STORE_ADDRESS1,
          StoreTable_dw.STORE_ADDRESS2 = stsv.STORE_ADDRESS2,
          StoreTable_dw.STORE_CITY = stsv.STORE_CITY,
          StoreTable_dw.STORE_STATE = stsv.STORE_STATE,
          StoreTable_dw.STORE_ZIP = stsv.STORE_ZIP,
          StoreTable_dw.DATE_LAST_UPDATED = stsv.DATE_LAST_UPDATED
          
      WHEN NOT MATCHED THEN
        INSERT (STORE_ID, STORE_ADDRESS1, STORE_ADDRESS2, STORE_CITY , STORE_STATE , STORE_ZIP ,DATE_LAST_UPDATED)
        VALUES (stsv.STORE_ID, stsv.STORE_ADDRESS1, 
        stsv.STORE_ADDRESS2, stsv.STORE_CITY, stsv.STORE_STATE,
        stsv.STORE_ZIP,stsv.DATE_LAST_UPDATED);
        
 COMMIT;
END;

/


-- DEPARTMENT PROCEDURE
CREATE OR REPLACE PROCEDURE dep_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into Department_dw table
    INSERT INTO Department_dw
        (dep_id, dep_name, dep_start_date, dep_end_date , date_last_updated)
    SELECT  *
    FROM   department_emp_view
    WHERE  NOT EXISTS (SELECT *
                       FROM   Department_dw
                       WHERE  department_emp_view.dep_id = Department_dw.dep_id);
    -- insert values for unique data_source/client_id from prospective_user_view into Department_dw table
    INSERT INTO Department_dw
        (dep_id, dep_name, dep_start_date, dep_end_date , date_last_updated)
    SELECT  *
    FROM   department_sup_view
    WHERE  NOT EXISTS (SELECT *
                       FROM   Department_dw
                       WHERE  department_sup_view.dep_id = Department_dw.dep_id);
                              
                              
    -- Merge statement that updates matching data_source/client_id combinations from curr_user_view
    -- in client_dw and if no combination found, inserts new department into Department_dw
    MERGE INTO Department_dw
        USING (SELECT * FROM department_emp_view) dev  ON (dev.dep_id = Department_dw.dep_id) 
      WHEN MATCHED THEN
        UPDATE SET
          Department_dw.dep_name = dev.dep_name,
          Department_dw.dep_start_date = dev.dep_start_date,
          Department_dw.dep_end_date = dev.dep_end_date,
          Department_dw.date_last_updated = dev.date_last_updated
          
      WHEN NOT MATCHED THEN
        INSERT (dep_id, dep_name, dep_start_date, dep_end_date , date_last_updated)
        VALUES (dev.dep_id, dev.dep_name, 
        dev.dep_start_date, dev.dep_end_date, dev.date_last_updated);
        
        
    -- Merge statement that updates matching data_source/client_id combinations from prospective_view
    -- in client_dw and if no combination found, inserts new client into client_dw
    MERGE INTO Department_dw
        USING (SELECT * FROM department_sup_view) dsv  ON (dsv.dep_id = Department_dw.dep_id) 
      WHEN MATCHED THEN
        UPDATE SET
          Department_dw.dep_name = dsv.dep_name,
          Department_dw.dep_start_date = dsv.dep_start_date,
          Department_dw.dep_end_date = dsv.dep_end_date,
          Department_dw.date_last_updated = dsv.date_last_updated
          
      WHEN NOT MATCHED THEN
        INSERT (dep_id, dep_name, dep_start_date, dep_end_date , date_last_updated)
        VALUES (dsv.dep_id, dsv.dep_name, 
        dsv.dep_start_date, dsv.dep_end_date, dsv.date_last_updated);
        
 COMMIT;
END;

/


-- EMPLOYEE PROCEDURE
CREATE OR REPLACE PROCEDURE employee_etl_proc  
AS
BEGIN
  -- insert values for unique employee_id from Employee into Employee_dw table
    INSERT INTO Employee_dw
        (EMPLOYEE_ID, EMP_FIRST_NAME, EMP_LAST_NAME, EMP_BIRTH_DATE, EMP_HIRE_DATE, EMP_ADDRESS1, EMP_ADDRESS2,
        EMP_CITY, EMP_STATE, EMP_ZIP, EMP_EMAIL, EMP_SALARY, STORE_ID, CURRENT_POSITION, DEPARTMENT_NAME, DEP_ID, FROM_DATE,
        TERMINATION_DATE, DATE_LAST_UPDATED)
    SELECT  *
    FROM   Employee
    WHERE  NOT EXISTS (SELECT *
                       FROM   Employee_dw
                       WHERE  
                              employee.employee_id = Employee_dw.employee_id);
            
    -- Merge statement that updates matching data_source/client_id combinations from prospective_view
    -- in client_dw and if no combination found, inserts new client into client_dw
    MERGE INTO Employee_dw
        USING (SELECT * FROM employee) emp  ON (emp.employee_id = Employee_dw.employee_id) 
      WHEN MATCHED THEN
        UPDATE SET
          Employee_dw.EMP_FIRST_NAME = INITCAP(emp.EMP_FIRST_NAME),
          Employee_dw.EMP_LAST_NAME = INITCAP(emp.EMP_LAST_NAME),
          Employee_dw.EMP_BIRTH_DATE = emp.EMP_BIRTH_DATE,
          Employee_dw.EMP_HIRE_DATE = emp.EMP_HIRE_DATE,
          Employee_dw.EMP_ADDRESS1 = UPPER(emp.EMP_ADDRESS1), 
          Employee_dw.EMP_ADDRESS2 = UPPER(emp.EMP_ADDRESS2),
          Employee_dw.EMP_CITY = UPPER(emp.EMP_CITY),
          Employee_dw.EMP_STATE = UPPER(emp.EMP_STATE),
          Employee_dw.EMP_ZIP = emp.EMP_ZIP,
          Employee_dw.EMP_EMAIL = UPPER(emp.EMP_EMAIL),
          Employee_dw.EMP_SALARY = emp.EMP_SALARY,
          Employee_dw.STORE_ID = emp.STORE_ID,
          Employee_dw.CURRENT_POSITION = UPPER(emp.CURRENT_POSITION),
          Employee_dw.DEPARTMENT_NAME = UPPER(emp.DEPARTMENT_NAME),
          Employee_dw.DEP_ID = emp.DEPARTMENT_ID,
          Employee_dw.FROM_DATE = emp.FROM_DATE,
          Employee_dw.TERMINATION_DATE = emp.TERMINATION_DATE,
          Employee_dw.DATE_LAST_UPDATED = emp.DATE_LAST_UPDATED
          
      WHEN NOT MATCHED THEN
        INSERT (EMPLOYEE_ID, EMP_FIRST_NAME, EMP_LAST_NAME, EMP_BIRTH_DATE, EMP_HIRE_DATE, EMP_ADDRESS1, EMP_ADDRESS2,
            EMP_CITY, EMP_STATE, EMP_ZIP, EMP_EMAIL, EMP_SALARY, STORE_ID, CURRENT_POSITION, DEPARTMENT_NAME, DEP_ID, FROM_DATE,
            TERMINATION_DATE, DATE_LAST_UPDATED)
        VALUES (emp.EMPLOYEE_ID, emp.EMP_FIRST_NAME, emp.EMP_LAST_NAME, emp.EMP_BIRTH_DATE, emp.EMP_HIRE_DATE, emp.EMP_ADDRESS1,
            emp.EMP_ADDRESS2, emp.EMP_CITY, emp.EMP_STATE, emp.EMP_ZIP, emp.EMP_EMAIL, emp.EMP_SALARY, emp.STORE_ID,
            emp.CURRENT_POSITION, emp.DEPARTMENT_NAME, emp.DEPARTMENT_ID, emp.FROM_DATE, emp.TERMINATION_DATE, emp.DATE_LAST_UPDATED);
    
        
 COMMIT;

END;

/


-- SS PROCEDURE
CREATE OR REPLACE PROCEDURE social_sec_etl_proc  
AS
BEGIN
  -- insert values for unique employee_id from Employee into Employee_dw table
    INSERT INTO socialsecurity_dw
        (EMPLOYEE_ID, SSN)
    SELECT  *
    FROM   socialsecurity ss
    WHERE  NOT EXISTS (SELECT *
                       FROM   socialsecurity_dw
                       WHERE  
                              ss.employee_id = socialsecurity_dw.employee_id);
            
 COMMIT;

END;

/


-- ACCOUNT PROCEDURE
CREATE OR REPLACE PROCEDURE acct_dtl_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
    INSERT INTO AccountDetail_dw
        (ACCOUNT_ID, ACC_FIRST_NAME, ACC_LAST_NAME, CARD_TYPE , DATE_CREATED, ACC_MAIL_ADDR1,ACC_MAIL_ADDR2, ACC_CITY, ACC_STATE,
        ACC_COUNTRY,ACC_ZIPCODE,DATE_LAST_UPDATED)
    SELECT  *
    FROM   ACCOUNTDETAIL
    WHERE  NOT EXISTS (SELECT *
                       FROM   AccountDetail_dw
                       WHERE  
                              ACCOUNTDETAIL.ACCOUNT_ID = AccountDetail_dw.ACCOUNT_ID);
    MERGE INTO AccountDetail_dw
        USING (SELECT * FROM ACCOUNTDETAIL) accd  ON (accd.ACCOUNT_ID = AccountDetail_dw.ACCOUNT_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          AccountDetail_dw.ACC_FIRST_NAME = INITCAP(accd.ACC_FIRST_NAME),
          AccountDetail_dw.ACC_LAST_NAME = INITCAP(accd.ACC_LAST_NAME),
          AccountDetail_dw.CARD_TYPE = UPPER(accd.CARD_TYPE),
          AccountDetail_dw.DATE_CREATED = accd.DATE_CREATED,
          AccountDetail_dw.ACC_MAIL_ADDR1 = UPPER(accd.ACC_MAIL_ADDR1),
          AccountDetail_dw.ACC_MAIL_ADDR2 = UPPER(accd.ACC_MAIL_ADDR2),
          AccountDetail_dw.ACC_CITY = UPPER(accd.ACC_CITY),
          AccountDetail_dw.ACC_STATE = UPPER(accd.ACC_STATE),
          AccountDetail_dw.ACC_COUNTRY = UPPER(accd.ACC_COUNTRY),
          AccountDetail_dw.ACC_ZIPCODE = accd.ACC_ZIPCODE,
          AccountDetail_dw.DATE_LAST_UPDATED = accd.DATE_LAST_UPDATED
      WHEN NOT MATCHED THEN
        INSERT  (ACCOUNT_ID, ACC_FIRST_NAME, ACC_LAST_NAME, CARD_TYPE , DATE_CREATED, ACC_MAIL_ADDR1,ACC_MAIL_ADDR2, ACC_CITY, ACC_STATE,
        ACC_COUNTRY,ACC_ZIPCODE,DATE_LAST_UPDATED)
        VALUES (accd.ACCOUNT_ID, accd.ACC_FIRST_NAME, accd.ACC_LAST_NAME, accd.CARD_TYPE , accd.DATE_CREATED, accd.ACC_MAIL_ADDR1,
        accd.ACC_MAIL_ADDR2, accd.ACC_CITY, accd.ACC_STATE,accd.ACC_COUNTRY,accd.ACC_ZIPCODE,accd.DATE_LAST_UPDATED);
 COMMIT;
END;
/

-- PRODUCT PROCEDURE
CREATE OR REPLACE PROCEDURE prod_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
INSERT INTO Product_dw
        (PRODUCT_ID, PRODUCT_NAME, DEPT_NAME, PRODUCT_DESCR , STANDARD_PRICE, QUANTITY,date_LAST_UPDATED)
    SELECT  *
    FROM   Product_view
    WHERE  NOT EXISTS (SELECT *
                       FROM   Product_dw
                       WHERE  Product_view.PRODUCT_ID = Product_dw.PRODUCT_ID);
    -- insert values for unique data_source/client_id from prospective_user_view into customers_dw table
    INSERT INTO Product_dw
        (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCR , STANDARD_PRICE, QUANTITY,date_LAST_UPDATED)
    SELECT  *
    FROM   Product_sale_view
    WHERE  NOT EXISTS (SELECT *
                       FROM   Product_dw
                       WHERE  
                              Product_sale_view.PRODUCT_ID = Product_dw.PRODUCT_ID);
                              
                              
    -- Merge statement that updates matching data_source/client_id combinations from curr_user_view
    -- in client_dw and if no combination found, inserts new client into client_dw
    MERGE INTO Product_dw
        USING (SELECT * FROM Product_view) pv  ON (pv.PRODUCT_ID = Product_dw.PRODUCT_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          Product_dw.PRODUCT_NAME = pv.PRODUCT_NAME,
          Product_dw.DEPT_NAME = pv.DEP_NAME,
          Product_dw.PRODUCT_DESCR = pv.PRODUCT_DESCR,
          Product_dw.STANDARD_PRICE = pv.STANDARD_PRICE,
          Product_dw.QUANTITY = pv.QUANTITY,
          Product_dw.DATE_LAST_UPDATED = pv.DATE_LAST_UPDATED
          
      WHEN NOT MATCHED THEN
        INSERT (PRODUCT_ID, PRODUCT_NAME, DEPT_NAME, PRODUCT_DESCR , STANDARD_PRICE, QUANTITY,date_LAST_UPDATED)
        VALUES (pv.PRODUCT_ID, pv.PRODUCT_NAME, pv.DEP_NAME, pv.PRODUCT_DESCR , pv.STANDARD_PRICE, pv.QUANTITY,pv.date_LAST_UPDATED);
        
        
    -- Merge statement that updates matching data_source/client_id combinations from prospective_view
    -- in client_dw and if no combination found, inserts new client into client_dw
    MERGE INTO Product_dw
        USING (SELECT * FROM Product_sale_view) psv  ON (psv.PRODUCT_ID = Product_dw.PRODUCT_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          Product_dw.PRODUCT_NAME = psv.PRODUCT_NAME,
          Product_dw.PRODUCT_DESCR = psv.PRODUCT_DESCR,
          Product_dw.STANDARD_PRICE = psv.STANDARD_PRICE,
          Product_dw.QUANTITY = psv.QUANTITY,
          Product_dw.DATE_LAST_UPDATED = psv.DATE_LAST_UPDATED
          
      WHEN NOT MATCHED THEN
        INSERT (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCR , STANDARD_PRICE, QUANTITY,date_LAST_UPDATED)
        VALUES (psv.PRODUCT_ID, psv.PRODUCT_NAME, psv.PRODUCT_DESCR , psv.STANDARD_PRICE, psv.QUANTITY, psv.date_LAST_UPDATED);
        
 COMMIT;
END;

/

-- SALE PROCEDURE
CREATE OR REPLACE PROCEDURE sale_etl_proc  
AS
BEGIN
  -- insert values for unique employee_id from Employee into Employee_dw table
    INSERT INTO Sale_dw
        (SALE_ORDER_ID, ACC_FLAG, ACCOUNT_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_MAIL_ADDR1, CUST_MAIL_ADDR2,
        CUST_CITY, CUST_STATE, CUST_ZIP, PAYMENT_METHOD, PURCHASE_DATE, EMPLOYEE_ID, STORE_ID, STORE_CITY,
        STORE_STATE, SALE_TOTAL)
    SELECT  *
    FROM   sale
    WHERE  NOT EXISTS (SELECT *
                       FROM   Sale_dw
                       WHERE  Sale_dw.SALE_ORDER_ID = sale.SALE_ORDER_ID);
 COMMIT;

END;

/
-- SaleDetail PROCEDURE
CREATE OR REPLACE PROCEDURE SaleDetail_etl_proc  
AS
BEGIN
  -- insert values for unique SALE_DETAIL_ID from SaleDetail into SaleDetail_dw table
    INSERT INTO SaleDetail_dw
        (SALE_ORDER_ID, PRODUCT_ID, QUANTITY, PRICE, BRAND, SALE_DESCRIPTION, DISCOUNT, PRODUCT_TOTAL)

    SELECT  *
    FROM   SaleDetail
    WHERE  NOT EXISTS (SELECT *
                       FROM   SaleDetail_dw
                       WHERE  SaleDetail.sale_detail_id = SaleDetail_dw.SALE_ORDER_ID AND
                              SaleDetail.PRODUCT_ID = SaleDetail_dw.PRODUCT_ID);
            
 COMMIT;

END;

/

-- STORE INVENTORY PROCEDURE
CREATE OR REPLACE PROCEDURE store_inventory_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
    INSERT INTO StoreInventory_dw
        (PRODUCT_ID, STORE_ID, dep_name, PRODUCT_NAME , REGIONAL_PRICE, QUANTITY,LAST_UPDATED)
    SELECT  *
    FROM   STOREINVENTORY
    WHERE  NOT EXISTS (SELECT *
                       FROM   StoreInventory_dw
                       WHERE  
                              STOREINVENTORY.PRODUCT_ID = StoreInventory_dw.PRODUCT_ID
                              and STOREINVENTORY.STORE_ID = StoreInventory_dw.STORE_ID);
    MERGE INTO StoreInventory_dw
        USING (SELECT * FROM STOREINVENTORY) si  ON (si.product_id = StoreInventory_dw.product_id and si.store_id = StoreInventory_dw.STORE_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          StoreInventory_dw.dep_name = INITCAP(si.DEPT_NAME),
          StoreInventory_dw.PRODUCT_NAME = INITCAP(si.PRODUCT_NAME),
          StoreInventory_dw.REGIONAL_PRICE = si.REGIONAL_PRICE,
          StoreInventory_dw.QUANTITY = si.QUANTITY,
          StoreInventory_dw.LAST_UPDATED = si.LAST_UPDATED
      WHEN NOT MATCHED THEN
        INSERT  (PRODUCT_ID, STORE_ID, DEP_NAME, PRODUCT_NAME , REGIONAL_PRICE, QUANTITY,LAST_UPDATED)
        VALUES (si.PRODUCT_ID, si.STORE_ID, si.DEPT_NAME, si.PRODUCT_NAME , si.REGIONAL_PRICE, si.QUANTITY,si.LAST_UPDATED);
 COMMIT;
END;
/

-- ORDER PROCEDURE
CREATE OR REPLACE PROCEDURE order_table_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
    INSERT INTO OrderTable_dw
        (PURCHASE_ORDER_ID, SUPPLIER_ID, EMPLOYEE_ID, PURCHASE_QUANTITY , SUPP_NAME,SUPP_STREET_ADDR1,SUPP_STREET_ADDR2,
        SUPP_CIY,SUPP_STATE,SUPP_ZIP,SUPP_PURCHASE_DATE)
    SELECT  *
    FROM   ORDERTABLE
    WHERE  NOT EXISTS (SELECT *
                       FROM   OrderTable_dw
                       WHERE  ORDERTABLE.PURCHASE_ORDER_ID = OrderTable_dw.PURCHASE_ORDER_ID);
 COMMIT;
END;
/

-- ORDER PRODUCT LINK PROCEDURE
CREATE OR REPLACE PROCEDURE order_product_link_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
    INSERT INTO orderproductlink_dw
        (PURCHASE_ORDER_ID, PRODUCT_ID)
    SELECT  *
    FROM   orderproductlink
    WHERE  NOT EXISTS (SELECT *
                       FROM   orderproductlink_dw
                       WHERE  
                              orderproductlink.PURCHASE_ORDER_ID = orderproductlink_dw.PURCHASE_ORDER_ID);
 COMMIT;
END;
/


-- EmployeeStore PROCEDURE
CREATE OR REPLACE PROCEDURE EmployeeStore_etl_proc  
AS
BEGIN
  -- insert values for unique data_source/customer_id from curr_user_view into client_dw table
    INSERT INTO EmployeeStore_dw
        (employee_id, STORE_ID, DATE_LAST_UPDATED)
    SELECT  *
    FROM   EmployeeStore
    WHERE  NOT EXISTS (SELECT *
                       FROM   EmployeeStore_dw
                       WHERE  
                              EmployeeStore.employee_id = EmployeeStore_dw.employee_id AND
                              EmployeeStore.STORE_ID = EmployeeStore_dw.STORE_ID);
    MERGE INTO EmployeeStore_dw
        USING (SELECT * FROM EmployeeStore) es  ON (es.employee_id = EmployeeStore_dw.employee_id and es.store_id = EmployeeStore_dw.STORE_ID) 
      WHEN MATCHED THEN
        UPDATE SET
          EmployeeStore_dw.DATE_LAST_UPDATED = es.DATE_LAST_UPDATED
      WHEN NOT MATCHED THEN
        INSERT  (employee_id, STORE_ID, DATE_LAST_UPDATED)
        VALUES (es.employee_id, es.STORE_ID, es.DATE_LAST_UPDATED);
 COMMIT;
END;
/





EXECUTE STORE_ETL_PROC;
EXECUTE DEP_ETL_PROC;
EXECUTE EMPLOYEE_ETL_PROC;
EXECUTE SOCIAL_SEC_ETL_PROC;
EXECUTE ACCT_DTL_ETL_PROC;
EXECUTE PROD_ETL_PROC;
EXECUTE SALE_ETL_PROC;
EXECUTE SALEDETAIL_ETL_PROC;
EXECUTE STORE_INVENTORY_ETL_PROC;
EXECUTE ORDER_TABLE_ETL_PROC;
EXECUTE ORDER_PRODUCT_LINK_ETL_PROC;
EXECUTE EMPLOYEESTORE_ETL_PROC;
