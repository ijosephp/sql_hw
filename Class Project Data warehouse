-- This script creates the datawarehouse

--------------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE SECTION
--------------------------------------------------------------------------------------------------------------------------------

DROP TABLE Employee_dw          CASCADE CONSTRAINTS;
DROP TABLE Department_dw        CASCADE CONSTRAINTS;
DROP TABLE EmployeeStore_dw     CASCADE CONSTRAINTS;
DROP TABLE SocialSecurity_dw    CASCADE CONSTRAINTS;
DROP TABLE StoreTable_dw        CASCADE CONSTRAINTS;
DROP TABLE SaleDetail_dw        CASCADE CONSTRAINTS;
DROP TABLE Sale_dw              CASCADE CONSTRAINTS;
DROP TABLE AccountDetail_dw     CASCADE CONSTRAINTS;
DROP TABLE Product_dw           CASCADE CONSTRAINTS;
DROP TABLE StoreInventory_dw    CASCADE CONSTRAINTS;
DROP TABLE OrderTable_dw        CASCADE CONSTRAINTS;
DROP TABLE OrderProductLink_dw  CASCADE CONSTRAINTS;

--------------------------------------------------------------------------------------------------------------------------------
-- CREATE TABLE SECTION
--------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE StoreTable_dw
(
    store_id            NUMBER          NOT NULL,
    store_address1      VARCHAR(40)     NOT NULL,
    store_address2      VARCHAR(40)             ,
    store_city          VARCHAR(40)     NOT NULL,
    store_state         CHAR(2)         NOT NULL,
    store_zip           CHAR(5)         NOT NULL,
    date_last_updated   DATE            NOT NULL,
        CONSTRAINT StoreTable_dw PRIMARY KEY (store_id)
);

CREATE TABLE Department_dw
(
    dep_id              NUMBER          NOT NULL,
    dep_name            VARCHAR(35)     NOT NULL,
    dep_start_date      DATE            NOT NULL,
    dep_end_date        DATE                    ,
    date_last_updated   DATE            NOT NULL,
        CONSTRAINT department_dw_pk  PRIMARY KEY (dep_id)
);

CREATE TABLE Employee_dw
(
    employee_id         NUMBER          NOT NULL,
    emp_first_name      VARCHAR(35)     NOT NULL,
    emp_last_name       VARCHAR(35)     NOT NULL,
    emp_birth_date      DATE            NOT NULL,    
    emp_hire_date       DATE            NOT NULL,
    emp_address1        VARCHAR(40)     NOT NULL,
    emp_address2        VARCHAR(15)             ,
    emp_city            VARCHAR(40)     NOT NULL,
    emp_state           CHAR(2)         NOT NULL,
    emp_zip             CHAR(5)         NOT NULL,
    emp_email           VARCHAR(40)     NOT NULL,
    emp_salary          NUMBER(7)       NOT NULL,
    store_id            NUMBER                  ,
    current_position    VARCHAR(35)     NOT NULL,
    department_name     VARCHAR(35)     NOT NULL,
    dep_id              NUMBER(3)       NOT NULL,
    from_date           DATE            NOT NULL,
    termination_date    DATE                    ,
    date_last_updated   DATE            NOT NULL,
        CONSTRAINT employee_dw_pk PRIMARY KEY (employee_id),
        CONSTRAINT emp_dw_store_id_fk FOREIGN KEY (store_id) REFERENCES StoreTable_dw(store_id),
        CONSTRAINT emp_dw_dep_id_fk FOREIGN KEY (dep_id) REFERENCES Department_dw(dep_id)
);


CREATE TABLE SocialSecurity_dw
(
    employee_id         NUMBER          NOT NULL,
    ssn                 VARCHAR2(11)    NOT NULL,
        CONSTRAINT ssn_dw_pk PRIMARY KEY (employee_id),
        CONSTRAINT ssn_dw_fk FOREIGN KEY (employee_id) REFERENCES Employee_dw(employee_id)
);

CREATE TABLE EmployeeStore_dw
(
    employee_id         NUMBER          NOT NULL,
    store_id     NUMBER          NOT NULL,
    date_last_updated   DATE            NOT NULL,
        CONSTRAINT employeeStore_dw_pk PRIMARY KEY (employee_id, store_id),
        CONSTRAINT es_dw_emp_id_fk FOREIGN KEY (employee_id) REFERENCES Employee_dw(employee_id),
        CONSTRAINT es_dw_store_id_fk FOREIGN KEY (store_id) REFERENCES StoreTable_dw(store_id) 
);


CREATE TABLE Product_dw
(
    product_id          NUMBER          NOT NULL,
    product_name        VARCHAR(50)     NOT NULL,
    dept_name           VARCHAR(50)     NOT NULL,
    product_descr       VARCHAR(500)            ,  
    standard_price      NUMBER          NOT NULL,
    quantity            NUMBER          NOT NULL,
    date_last_updated        DATE            NOT NULL,
        CONSTRAINT Product_dw_pk PRIMARY KEY (product_id)
);


CREATE TABLE StoreInventory_dw
(
    product_id          NUMBER          NOT NULL,
    store_id            NUMBER          NOT NULL,
    dep_name            VARCHAR(50)     NOT NULL,
    product_name        VARCHAR(50)     NOT NULL,
    regional_price      NUMBER          NOT NULL,
    quantity            NUMBER          NOT NULL,
    last_updated        DATE            NOT NULL,
        CONSTRAINT StoreInventory_dw_pk PRIMARY KEY (product_id, store_id),
        CONSTRAINT StoreInventory_dw_product_id_fk FOREIGN KEY (product_id) REFERENCES Product_dw (product_id),
        CONSTRAINT StoreInventory_dw_store_id_fk FOREIGN KEY (store_id) REFERENCES StoreTable_dw (store_id)
);

CREATE TABLE OrderTable_dw
(
    purchase_order_id   NUMBER          NOT NULL,
    supplier_id         NUMBER          NOT NULL,
    employee_id         NUMBER          NOT NULL,
    purchase_quantity   NUMBER          NOT NULL,
    supp_name           VARCHAR(50)     NOT NULL,
    supp_street_addr1   VARCHAR(500)    NOT NULL,
    supp_street_addr2   VARCHAR(500)            ,
    supp_ciy            VARCHAR(50)     NOT NULL,
    supp_state          VARCHAR(50)     NOT NULL,
    supp_zip            NUMBER          NOT NULL,
    supp_purchase_date  DATE            NOT NULL,
        CONSTRAINT OrderTable_dw_pk PRIMARY KEY (purchase_order_id)
);


CREATE TABLE OrderProductLink_dw
(
    purchase_order_id   NUMBER      NOT NULL,
    product_id          NUMBER      NOT NULL,
        CONSTRAINT OrderProductLink_dw_pk PRIMARY KEY (purchase_order_id, product_id),
        CONSTRAINT OrderProductLink_dw_purchase_order_id_fk FOREIGN KEY (purchase_order_id) references OrderTable_dw (purchase_order_id),
        CONSTRAINT OrderProductLink_dw_product_id_fk FOREIGN KEY (product_id) references Product_dw (product_id)
);

CREATE TABLE AccountDetail_dw
(
    account_id          NUMBER          NOT NULL,
    acc_first_name      VARCHAR(35)     NOT NULL,
    acc_last_name       VARCHAR(35)     NOT NULL,
    card_type           VARCHAR(1)      NOT NULL,
    date_created        DATE            NOT NULL,
    acc_mail_addr1      VARCHAR(30)     NOT NULL,
    acc_mail_addr2      VARCHAR(2)              ,
    acc_city            VARCHAR(20)     NOT NULL,
    acc_state           CHAR(2)         NOT NULL,
    acc_country         VARCHAR(35)     NOT NULL,
    acc_zipcode         NUMBER(5)       NOT NULL,
    date_last_updated   DATE            NOT NULL,
        CONSTRAINT acc_detail_dw_pk PRIMARY KEY (account_id)
);

CREATE TABLE Sale_dw
(
    sale_order_id       NUMBER          NOT NULL,
    acc_flag            VARCHAR(1)      NOT NULL,
    account_id          NUMBER                  ,
    cust_first_name     VARCHAR(35)             ,
    cust_last_name      VARCHAR(35)             ,
    cust_mail_addr1     VARCHAR(50)             ,
    CUST_MAIL_ADDR2      VARCHAR(15)             ,
    cust_city           VARCHAR(30)             ,
    cust_state          CHAR(2)                 ,
    cust_zip            VARCHAR(5)              ,
    payment_method      VARCHAR(30)     NOT NULL,
    purchase_date       DATE            NOT NULL,
    employee_id         NUMBER          NOT NULL,
    store_id            NUMBER          NOT NULL,
    store_city          VARCHAR(30)     NOT NULL,
    store_state         CHAR(2)         NOT NULL,
    sale_total          NUMBER(10)      NOT NULL,
        CONSTRAINT sale_dw_pk PRIMARY KEY (sale_order_id),
        CONSTRAINT sale_dw_account_id_fk FOREIGN KEY (account_id) REFERENCES AccountDetail_dw(account_id),
        CONSTRAINT sale_dw_emp_id_fk FOREIGN KEY (employee_id) REFERENCES Employee_dw(employee_id)
);


CREATE TABLE SaleDetail_dw
(
    sale_order_id       NUMBER                  ,
    product_id          NUMBER          NOT NULL,
    quantity            NUMBER(5)       NOT NULL,
    price               NUMBER(10)      NOT NULL,
    brand               VARCHAR(40)     NOT NULL,
    sale_description    VARCHAR(50)     NOT NULL,
    discount            NUMBER(4)               ,
    product_total       NUMBER(10)      NOT NULL,
        CONSTRAINT sale_detail_dw_pk PRIMARY KEY (sale_order_id, product_id),
        CONSTRAINT sale_detail_dw_sale_order_id_fk FOREIGN KEY (sale_order_id) REFERENCES Sale_dw(sale_order_id),
        CONSTRAINT sale_detail_dw_product_id_fk FOREIGN KEY (product_id) REFERENCES Product_dw(product_id)        
);
