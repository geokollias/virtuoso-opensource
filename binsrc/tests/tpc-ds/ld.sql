select curdatetime ();

status ('');

log_enable (2); insert into dbgen_version select * from dbgen_version_f &
log_enable (2); insert into customer_address select * from customer_address_f &
log_enable (2); insert into customer_demographics select * from customer_demographics_f &
log_enable (2); insert into date_dim select * from date_dim_f &
log_enable (2); insert into warehouse select * from warehouse_f &
log_enable (2); insert into ship_mode select * from ship_mode_f &
log_enable (2); insert into time_dim select * from time_dim_f &
log_enable (2); insert into reason select * from reason_f &
log_enable (2); insert into income_band select * from income_band_f &
log_enable (2); insert into item select * from item_f &
log_enable (2); insert into store select * from store_f &
log_enable (2); insert into call_center select * from call_center_f &
log_enable (2); insert into customer select * from customer_f &
log_enable (2); insert into web_site select * from web_site_f &
log_enable (2); insert into store_returns select * from store_returns_f &
log_enable (2); insert into household_demographics select * from household_demographics_f &
log_enable (2); insert into web_page select * from web_page_f &
log_enable (2); insert into promotion select * from promotion_f &
log_enable (2); insert into catalog_page select * from catalog_page_f &
log_enable (2); insert into inventory select * from inventory_f &
log_enable (2); insert into catalog_returns select * from catalog_returns_f &
log_enable (2); insert into web_returns select * from web_returns_f &
log_enable (2); insert into web_sales select * from web_sales_f &
log_enable (2); insert into catalog_sales select * from catalog_sales_f &
log_enable (2); insert into store_sales select * from store_sales_f &

wait_for_children;

status ('');

select curdatetime ();

checkpoint;

select curdatetime ();

